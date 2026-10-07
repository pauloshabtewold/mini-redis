"""The connection cap: `--max-connections`, refused at accept and never queued.

Most tests here drive a real listener a step at a time through `listening()` and
`pump()`, with the cap set on the server it yields, so a refusal is observed from the
client's own socket and not inferred from a mock. The caps are tiny on purpose, bar the
two tests that read `0` and the default: the boundary is the thing under test, and a
cap in the hundreds would measure descriptor limits rather than the comparison. Three
tests use no listener. The two that refuse a negative cap, at the CLI parser and at
the constructor, never serve a connection, and the one that starts a real `server.py`
under its own `Popen` is there to show that `main()` passes the flag on. A fourth test
starts a real `server.py` too, for the other reason a server refuses a connection, a
descriptor table with no room, which only a child can have: lowering the limit in this
process would take the rest of the suite's descriptors with it.
"""

import contextlib
import errno
import gc
import logging
import pathlib
import select
import signal
import socket
import subprocess
import sys
import threading
import time
import types
import warnings

import pytest

import server as server_mod
from server import DEFAULT_MAX_CONNECTIONS, REFUSAL_EPISODE_GAP_SECONDS, Server, build_arg_parser
from tests.test_server_lifecycle import listening, pump

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
PING = b"*1\r\n$4\r\nPING\r\n"
INFO = b"*1\r\n$4\r\nINFO\r\n"


@contextlib.contextmanager
def _injected_clock(start=1_000.0):
    # server.py's own `time` binding is replaced and not time.monotonic itself, because patching the real function reaches every module in this process. the namespace holds exactly the three names server.py reads from it
    clock = types.SimpleNamespace(t=start)
    real = server_mod.time
    server_mod.time = types.SimpleNamespace(monotonic=lambda: clock.t, time=real.time, sleep=real.sleep)
    try:
        yield clock
    finally:
        server_mod.time = real


@contextlib.contextmanager
def _capped(cap):
    with listening() as (server, connect, _listener):
        server.max_connections = cap
        yield server, connect


def _until_closed(client):
    # a clean close and a reset are the two spellings of one refusal, and neither carries a byte, so both end in whatever had been received by then. a timeout is the failure: the server kept the connection
    client.settimeout(1)
    received = b""
    try:
        while True:
            chunk = client.recv(4096)
            if not chunk:
                return received
            received += chunk
    except ConnectionResetError:
        return received


def _ping(server, client):
    client.sendall(PING)
    pump(server)
    client.settimeout(2)
    return client.recv(64)


def _connected_clients_line(server, client):
    client.sendall(INFO)
    pump(server)
    client.settimeout(2)
    reply = client.recv(65536).decode()
    return [line for line in reply.splitlines() if line.startswith("connected_clients")]


class _Records(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)


@contextlib.contextmanager
def _server_log():
    # DEBUG is below the logger's inherited level, so it is lowered for the duration and put back: a DEBUG record that was never emitted would make "quieter" pass for the wrong reason
    log = logging.getLogger("server")
    handler = _Records()
    before = log.level
    log.setLevel(logging.DEBUG)
    log.addHandler(handler)
    try:
        yield handler.records
    finally:
        log.removeHandler(handler)
        log.setLevel(before)


def test_the_connection_at_the_cap_is_admitted():
    with _capped(2) as (server, connect):
        first, second = connect(), connect()
        pump(server, times=6)
        assert server.connected_clients == 2, "a cap of two refused one of the first two clients"
        assert _ping(server, first) == b"+PONG\r\n"
        assert _ping(server, second) == b"+PONG\r\n"
        # the other side of the same boundary: with a count equal to the cap there is no room for one more, so a comparison that reads equal as not-yet-full admits a third here
        third = connect()
        pump(server, times=6)
        assert server.connected_clients == 2, "a third client was admitted at a cap of two"
        assert _until_closed(third) == b""


def test_the_connection_past_the_cap_is_refused():
    with _capped(3) as (server, connect):
        kept = [connect() for _ in range(3)]
        pump(server, times=8)
        assert server.connected_clients == 3
        refused = connect()
        # a socket dropped without a close is closed by the garbage collector on CPython, which looks the same from the client and warns here: the refusal has to close it itself
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            pump(server, times=4)
            gc.collect()
        assert not [w for w in caught if issubclass(w.category, ResourceWarning)], [str(w.message) for w in caught]
        assert _until_closed(refused) == b"", "the refused client was answered"
        # a cap lowered beneath a count that already exceeds it still refuses, so the test is on "is there room" and not on "is the count exactly the cap"
        server.max_connections = 2
        lowered = connect()
        pump(server, times=4)
        assert _until_closed(lowered) == b"", "a count above the cap was not full"
        assert len(kept) == 3 and server.connected_clients == 3


def test_a_refused_connection_never_enters_the_connection_set():
    with _capped(2) as (server, connect):
        first, second = connect(), connect()
        pump(server, times=6)
        refused = connect()
        pump(server, times=4)
        peers = {conn.addr for conn in server._connections}
        assert first.getsockname() in peers, "the control: an admitted client is found by its address"
        assert refused.getsockname() not in peers
        assert len(server._connections) == 2
        assert _until_closed(refused) == b""
        assert second.getsockname() in peers


def test_a_refused_connection_does_not_change_connected_clients():
    with _capped(2) as (server, connect):
        first, _second = connect(), connect()
        pump(server, times=6)
        before = (server.connected_clients, _connected_clients_line(server, first))
        refused = connect()
        pump(server, times=4)
        after = (server.connected_clients, _connected_clients_line(server, first))
        assert before == (2, ["connected_clients:2"]), before
        assert after == before, "a refused connection moved the count INFO reports"
        assert _until_closed(refused) == b""


def test_a_refused_connection_receives_no_bytes_before_the_close():
    with _capped(1) as (server, connect):
        kept = connect()
        pump(server, times=4)
        refused = connect()
        # sent before the server has looked at the connection, so a reply of any kind -- a PONG, an error line explaining the refusal -- has a request to answer
        refused.sendall(PING)
        pump(server, times=4)
        assert _until_closed(refused) == b""
        assert _ping(server, kept) == b"+PONG\r\n"


def test_a_slot_freed_by_a_disconnect_admits_the_next_client():
    with _capped(2) as (server, connect):
        first, second = connect(), connect()
        pump(server, times=6)
        refused = connect()
        pump(server, times=4)
        assert _until_closed(refused) == b""
        first.close()
        pump(server, times=6)
        assert server.connected_clients == 1, "the server did not see the disconnect"
        admitted = connect()
        pump(server, times=6)
        assert server.connected_clients == 2
        assert _ping(server, admitted) == b"+PONG\r\n"
        assert _ping(server, second) == b"+PONG\r\n"


def test_a_zero_cap_admits_everything():
    with _capped(0) as (server, connect):
        clients = [connect() for _ in range(12)]
        pump(server, times=24)
        assert server.connected_clients == 12, "a cap of zero refused connections"
        for client in clients:
            assert _ping(server, client) == b"+PONG\r\n"


def test_a_negative_cap_is_refused_at_the_cli(capsys):
    for value in ("-1", "-4096"):
        with pytest.raises(SystemExit) as raised:
            build_arg_parser().parse_args(["--max-connections", value])
        assert raised.value.code == 2
    message = capsys.readouterr().err
    assert "max connections" in message and "_max_connections" not in message
    # the controls: the flag is read at all, and zero is the one value below one that is allowed
    assert build_arg_parser().parse_args(["--max-connections", "3"]).max_connections == 3
    assert build_arg_parser().parse_args(["--max-connections", "0"]).max_connections == 0


def test_a_negative_cap_is_refused_by_the_constructor():
    with pytest.raises(ValueError, match="max_connections"):
        Server(0, max_connections=-1)
    # appended after the existing parameters, so a caller that passes the first four positionally and the rest by name is unaffected
    server = Server(0, 0, 0, 0, max_connections=7)
    try:
        assert server.max_connections == 7
    finally:
        server._loop.close()


def test_the_first_refusal_warns_and_the_rest_are_quieter():
    with _server_log() as records, _capped(1) as (server, connect):
        kept = connect()
        pump(server, times=4)
        for _ in range(3):
            refused = connect()
            pump(server, times=4)
            assert _until_closed(refused) == b""
        episode = [record.levelno for record in records if "refusing" in record.getMessage()]
        assert episode == [logging.WARNING, logging.DEBUG, logging.DEBUG], episode

        # the count falls back below the cap and the slot is taken again, and a refusal after that is still the same episode: a close is what a saturated cap's retrier is waiting for, so it cannot be what ends one
        records.clear()
        kept.close()
        pump(server, times=6)
        assert server.connected_clients == 0
        again = connect()
        pump(server, times=4)
        refused = connect()
        pump(server, times=4)
        assert _until_closed(refused) == b""
        second = [record.levelno for record in records if "refusing" in record.getMessage()]
        assert second == [logging.DEBUG], second
        assert again.getsockname() in {conn.addr for conn in server._connections}


def _refusals_logged(records):
    return [record for record in records if "--max-connections" in record.getMessage()]


def test_a_quiet_gap_starts_a_new_episode():
    with _injected_clock() as clock, _server_log() as records, _capped(1) as (server, connect):
        kept = connect()
        pump(server, times=4)
        outcomes = []
        # the first refusal, one exactly a gap later, and one a little more than a gap after that: only a gap longer than the limit ends the episode, so the second is the same episode and the third is a new one
        for step in (0, REFUSAL_EPISODE_GAP_SECONDS, REFUSAL_EPISODE_GAP_SECONDS + 1):
            clock.t += step
            refused = connect()
            pump(server, times=4)
            assert _until_closed(refused) == b""
            outcomes.append(_refusals_logged(records)[-1].levelno)
            assert len(_refusals_logged(records)) == len(outcomes), "a refusal wrote more than one line"
        assert outcomes == [logging.WARNING, logging.DEBUG, logging.WARNING], outcomes
        assert kept.getsockname() in {conn.addr for conn in server._connections}


def test_a_churning_cap_does_not_write_a_warning_per_refusal(monkeypatch):
    every = 4
    monkeypatch.setattr(server_mod, "REFUSALS_PER_LINE", every)
    refusals = 12
    # a cap that is neither one, which is what the first refusal's own count is, nor the interval between count lines, so a figure printed from the wrong source reads differently from the right one
    cap = 3
    with _server_log() as records, _capped(cap) as (server, connect):
        for _ in range(refusals):
            # the slots are held by short-lived clients while another knocks, and freed again before the next group: a close, and an admission, between every two refusals
            holders = [connect() for _ in range(cap)]
            pump(server, times=cap + 4)
            refused = connect()
            pump(server, times=4)
            assert _until_closed(refused) == b""
            for holder in holders:
                holder.close()
            pump(server, times=cap + 4)
            assert server.connected_clients == 0, "the churn did not free the slots"
        lines = _refusals_logged(records)
    assert len(lines) == refusals, "a refusal was not logged at all, at any level"
    levels = [line.levelno for line in lines]
    # the first in full and then one line per `every`, the same shape as a periodic task's failures: one warning per refusal is what a close re-arming it wrote, and 300 refusals wrote 300 lines
    assert levels == [logging.WARNING if n == 1 or n % every == 0 else logging.DEBUG for n in range(1, refusals + 1)], levels
    assert levels.count(logging.WARNING) == 1 + refusals // every
    assert "%d connections refused" % every in lines[every - 1].getMessage(), lines[every - 1].getMessage()
    assert "refusing" in lines[0].getMessage() and "refused so far" not in lines[0].getMessage()
    # the figures an operator reads, each from its own source: the first line gives the limit, how often a count follows and how long a quiet gap ends the episode, and a count line gives the count so far and the limit
    first = lines[0].getMessage()
    assert "%d connections is the --max-connections limit" % cap in first, first
    assert "with a count every %d, until none has come for %d seconds" % (every, REFUSAL_EPISODE_GAP_SECONDS) in first, first
    counts = [line.getMessage() for line in lines if line.levelno == logging.WARNING][1:]
    assert counts == [
        "%d connections refused so far at the --max-connections limit of %d" % (n, cap)
        for n in range(every, refusals + 1, every)
    ], counts


def test_the_cap_defaults_reach_both_doors_and_leave_room_for_the_standard_benchmark():
    # redis-benchmark -c 50 is the standard invocation, and a cap under it turns the benchmark into a test of the cap -- not a cap of exactly 50, which admits exactly 50, because the comparison asks whether there is room for one more. the property is what is asserted, so a later change of the number for a good reason does not fail here for a bad one
    benchmark_clients = 50
    args = build_arg_parser().parse_args([])
    server = Server(0)
    try:
        # each read from the one constant: a literal in either place would pass the checks below for the wrong reason
        assert args.max_connections == DEFAULT_MAX_CONNECTIONS
        assert server.max_connections == DEFAULT_MAX_CONNECTIONS
    finally:
        server._loop.close()
    # nonzero as well as large: 0 is no cap at all, which is a different default and not a larger one
    assert DEFAULT_MAX_CONNECTIONS > benchmark_clients
    with listening() as (default_server, connect, _listener):
        clients = [connect() for _ in range(benchmark_clients)]
        pump(default_server, times=benchmark_clients + 10)
        assert default_server.connected_clients == benchmark_clients, "the default cap refused part of a standard benchmark"
        assert len(clients) == benchmark_clients


def _launch_through_main(tmp_path, *flags):
    proc = subprocess.Popen(
        [sys.executable, str(REPO_ROOT / "server.py"), "--port", "0",
         "--snapshot-path", str(tmp_path / "dump.mrdb"), *flags],
        stdout=subprocess.PIPE, cwd=tmp_path)
    ready, _, _ = select.select([proc.stdout], [], [], 10)
    line = proc.stdout.readline().decode() if ready else ""
    assert line.startswith("listening on "), ("the server never said where it listened", line)
    return proc, int(line.strip().rsplit(":", 1)[1])


def test_the_flag_reaches_a_server_started_through_main(tmp_path):
    # every other test here sets max_connections on a Server it built itself, which goes around main(): a main() that stopped passing the flag on would leave all of them green
    proc, port = _launch_through_main(tmp_path, "--max-connections", "2")
    clients = []
    try:
        for _ in range(2):
            client = socket.create_connection(("127.0.0.1", port))
            clients.append(client)
            client.settimeout(5)
            client.sendall(PING)
            assert client.recv(64) == b"+PONG\r\n", "an admitted client was not answered"
        third = socket.create_connection(("127.0.0.1", port))
        clients.append(third)
        assert _until_closed(third) == b"", "the third client was kept at --max-connections 2"
    finally:
        for client in clients:
            client.close()
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        proc.stdout.close()


# this module's own copies of the three helpers test_graceful_shutdown.py waits on a child with, and not an import of them: the names are private there and a rename would break this module from a distance. the bound on every wait is a thread join or a deadline, never a wait() that a process which does not exit would hold for ever
_MARGIN_SECONDS = 3.0


def _wait_until(condition, seconds, what):
    end = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < end, "timed out waiting for " + what
        time.sleep(0.005)


def _returns_within(proc, bound):
    outcome = {}
    waiter = threading.Thread(target=lambda: outcome.update(rc=proc.wait()), daemon=True)
    waiter.start()
    waiter.join(bound)
    return (not waiter.is_alive()), outcome.get("rc")


def _reap(proc):
    if proc.poll() is None:
        proc.kill()
    proc.wait(timeout=10)
    if proc.stdout is not None:
        proc.stdout.close()


# a real descriptor table that is really full: accept() fails with EMFILE only when there is no descriptor to give, and nothing scripted stands in for that faithfully. the limit is lowered by a thread inside the child, once it is told to, and not by the test, where it would take the rest of the suite's descriptors with it; the thread says when the table is full so that the clients are sent to a process that has none to give
_FULL_TABLE_THEN_MAIN = """
import errno, os, resource, sys, threading
sys.path.insert(0, %r)
import server


def fill_when_told():
    sys.stdin.readline()
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (64, hard))
    held = []
    while True:
        try:
            held.append(os.open(os.devnull, os.O_RDONLY))
        except OSError as exc:
            assert exc.errno == errno.EMFILE, exc
            break
    print("full", flush=True)


threading.Thread(target=fill_when_told, daemon=True).start()
server.main(sys.argv[1:])
"""


def test_a_connection_refused_for_want_of_descriptors_is_reported_and_bounded(tmp_path):
    # _on_accept used to read every OSError out of accept() as a peer that aborted between readiness and accept, which is normal and says nothing -- and EMFILE arrives there too. the failed accept() takes the pending connection with it on this platform, so the client saw a reset and the server logged nothing, where the connection cap, which refuses for the same reason, writes a line. a refusal by descriptors is now reported through the cap's own bounded path: a WARNING for the first that names the error, DEBUG for the rest, a count line every REFUSALS_PER_LINE, because what fills a descriptor table fills it as fast as a client can connect and logging writes to stderr with a blocking write on the only thread. the stream is read by a thread and not after the exit, so that a platform whose failed accept leaves the connection in the backlog, and so refuses again on the next pass, cannot fill the pipe and park the child inside a log write. the bound is asserted as arithmetic over every refusal that was logged, whatever it came to, and not as a count of three, for the same reason
    snapshot = tmp_path / "dump.mrdb"
    proc = subprocess.Popen(
        [sys.executable, "-c", _FULL_TABLE_THEN_MAIN % str(REPO_ROOT),
         "--port", "0", "--snapshot-path", str(snapshot), "--snapshot-interval", "0",
         "--log-level", "DEBUG"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=tmp_path)
    stderr_lines = []
    reader = threading.Thread(
        target=lambda: stderr_lines.extend(iter(proc.stderr.readline, b"")), daemon=True)
    reader.start()
    clients = []

    def refusals():
        return [line.decode() for line in list(stderr_lines) if b"the descriptor table is full" in line]

    try:
        ready, _, _ = select.select([proc.stdout], [], [], 10)
        line = proc.stdout.readline().decode() if ready else ""
        assert line.startswith("listening on "), ("the server never said where it listened", line)
        proc.stdin.write(b"fill\n")
        proc.stdin.flush()
        ready, _, _ = select.select([proc.stdout], [], [], 10)
        filled = proc.stdout.readline() if ready else b""
        assert filled == b"full\n", ("the child never filled its descriptor table", filled)
        port = int(line.strip().rsplit(":", 1)[1])
        for _ in range(3):
            clients.append(socket.create_connection(("127.0.0.1", port)))
        _wait_until(lambda: len(refusals()) >= 3, 10, "three refusals by descriptor to be logged")
        proc.send_signal(signal.SIGTERM)
        returned, rc = _returns_within(proc, server_mod.SELECT_TIMEOUT_SECONDS + _MARGIN_SECONDS)
        reader.join(10)
    finally:
        for client in clients:
            client.close()
        _reap(proc)
        reader.join(10)
        proc.stderr.close()
        proc.stdin.close()
    stderr = b"".join(stderr_lines).decode()
    assert returned and rc == 0, (returned, rc, stderr[-2000:])
    assert "Logging error" not in stderr, stderr[-2000:]
    table = refusals()
    levels = [entry.split(":", 1)[0] for entry in table]
    assert len(table) >= 3, ("a client the full table refused was not reported", table)
    assert levels[0] == "WARNING", levels[:3]
    assert "OSError(%d, " % errno.EMFILE in table[0] and "Too many open files" in table[0], table[0]
    # one WARNING for the first refusal and one per REFUSALS_PER_LINE after it, however many there were: a WARNING for the second and the third as well is the line per attempt this path exists to prevent
    assert levels.count("WARNING") == 1 + len(table) // server_mod.REFUSALS_PER_LINE, (
        len(table), levels.count("WARNING"))
