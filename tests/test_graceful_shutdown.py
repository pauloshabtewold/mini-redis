"""The shutdown sequence: stop accepting, save, drain for a bounded time, tear down.

Three kinds of test live here and the first two cannot share a harness. The ones that
measure how long the process takes to exit, or what status it exits with, run a real
`server.py` under their own `Popen`: conftest's `launch_server` tears down with
terminate, wait, kill and ignores the status, so a drain that overran its bound would be
hidden by the very fixture that stops it. Every wait on such a process is a thread join
with a bound, never a sleep and never a clock read after a `wait()` that might not come
back.

The ones that have to see inside the sequence -- the order of its steps, what the selector
holds when the save runs -- run `Server.run()` on the test's own thread, because it
installs signal handlers and only the main thread may, while a second thread plays the
clients and asks it to stop. The ones about a stop that arrives before `run()` is reached
run `main()` there instead, with the signal sent from inside the snapshot load. The rest
never run a server: they build a parser or a `Server` and check how
`--shutdown-drain-timeout` is validated and what its default is.
Everything the first two kinds start keeps its snapshot inside the test's own
`tmp_path`, because every SIGTERM now saves one.
"""

import array
import errno
import fcntl
import logging
import os
import pathlib
import re
import select
import shutil
import signal
import socket
import struct
import subprocess
import sys
import termios
import threading
import time

import pytest

import commands
import persistence
import server as server_module
from connection import Connection
from server import (
    DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SECONDS, MAX_SCHEDULABLE_INTERVAL, SELECT_TIMEOUT_SECONDS,
    Server, build_arg_parser,
)
from store import Store

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
# on top of the drain timeout and one select timeout: the shutdown save of a few MiB, which ends in an fsync that is slow on some filesystems, and the process's own teardown
_MARGIN_SECONDS = 3.0
_VALUE_BYTES = 4 << 20
# how many replies _stalled_client asks for when a test does not say, which is the three tests that run at the shipped defaults and so under the 32 MiB --write-buffer-limit. one reply is 4,194,316 bytes: the 10-byte header "$4194304\r\n", the 4 MiB value and a 2-byte terminator. eight of them are 33,554,528 bytes, 96 over the limit's 33,554,432, and a connection is judged after the kernel has taken what it will, so eight read as one batch were closed at the eighth unless the kernel had taken at least 96 bytes of them -- and a connection closed there leaves the drain nothing to wait on. a loopback pair takes hundreds of kilobytes, so that never failed, which is the trouble: it passed on what the kernel absorbs and not on the arithmetic. four are 16,777,264 bytes, half the limit, and no amount the kernel absorbs or declines moves that across it. none of the three needs more than something owed at the moment of the stop. keep the product of this and the reply size well under the limit; the callers that switch the limit off pass a count of their own
_DEFAULT_STALL_REPLIES = 4
# the stall these tests build -- tens of MiB of replies owed to a client that reads none of them -- is the state the high-water mark exists to prevent. at the defaults a connection is paused after the first large reply, the rest of its requests are never dispatched, and the server owes one reply where the test counts on many. the tests that follow are about the drain and not about the water marks, so the marks and the limit are switched off for them; a test that only needs the server to owe something at the moment of the stop runs at the defaults, and keeps its stall well under the limit: see _DEFAULT_STALL_REPLIES
_NO_BACKPRESSURE_FLAGS = ("--write-buffer-limit", "0", "--write-buffer-high-water", "0")
_NO_BACKPRESSURE = {"write_buffer_limit": 0, "write_buffer_high_water": 0}


def _resp(*parts):
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts)


def _bulk(value):
    return b"$%d\r\n%s\r\n" % (len(value), value)


def _read_total(client, wanted, seconds=20):
    # a total deadline and not an idle timeout: a reader that gives up after one quiet recv() reports a drain that merely paused as one that stopped
    client.settimeout(seconds)
    end = time.monotonic() + seconds
    received = bytearray()
    while len(received) < wanted and time.monotonic() < end:
        chunk = client.recv(1 << 20)
        if not chunk:
            break
        received += chunk
    return bytes(received)


def _wait_until(condition, seconds, what):
    end = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < end, "timed out waiting for " + what
        time.sleep(0.005)


def _returns_within(proc, bound):
    # the bound is the join: a wait() on this thread would simply never return for a server that never exits, and then there would be no measurement, only a hung suite
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


@pytest.fixture
def start(tmp_path):
    started = []

    def launch(*flags):
        proc = subprocess.Popen(
            [sys.executable, str(REPO_ROOT / "server.py"), "--port", "0",
             "--snapshot-path", str(tmp_path / "dump.mrdb"), *flags],
            stdout=subprocess.PIPE, cwd=tmp_path)
        started.append(proc)
        ready, _, _ = select.select([proc.stdout], [], [], 10)
        line = proc.stdout.readline().decode() if ready else ""
        assert line.startswith("listening on "), ("the server never said where it listened", line)
        return proc, int(line.strip().rsplit(":", 1)[1])

    yield launch
    for proc in started:
        _reap(proc)


def _stalled_client(port, replies=_DEFAULT_STALL_REPLIES):
    # asks for a 4 MiB value back several times and reads none of it, so the server owes megabytes that nothing will take
    client = socket.create_connection(("127.0.0.1", port))
    client.settimeout(10)
    client.sendall(_resp(b"SET", b"k", b"hello"))
    assert client.recv(64) == b"+OK\r\n"
    client.sendall(_resp(b"SET", b"big", b"x" * _VALUE_BYTES))
    assert client.recv(64) == b"+OK\r\n"
    for _ in range(replies):
        client.sendall(_resp(b"GET", b"big"))
    return client


def test_a_stalled_client_does_not_hold_the_server_past_the_drain_timeout(start):
    drain = 1
    proc, port = start("--snapshot-interval", "3600", "--shutdown-drain-timeout", str(drain))
    client = _stalled_client(port)
    try:
        time.sleep(0.3)
        killed = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        returned, rc = _returns_within(proc, drain + SELECT_TIMEOUT_SECONDS + _MARGIN_SECONDS)
        elapsed = time.monotonic() - killed
    finally:
        client.close()
    assert returned, "the server was still running after the drain timeout plus a margin"
    assert rc == 0, rc
    # not before the timeout either: a server that owes a client unread replies and is gone in a tenth of a second drained nothing
    assert elapsed >= drain - 0.1, elapsed


def test_a_stalled_client_still_leaves_a_current_snapshot_on_disk(start, tmp_path):
    snapshot = tmp_path / "dump.mrdb"
    older = Store()
    older.write(b"before", b"1", keep_ttl=False)
    persistence.save(older, str(snapshot))
    an_hour_ago = time.time() - 3600
    os.utime(snapshot, (an_hour_ago, an_hour_ago))

    drain = 1
    proc, port = start("--snapshot-interval", "3600", "--shutdown-drain-timeout", str(drain))
    client = _stalled_client(port)
    try:
        # written and acknowledged on a second connection, so it is known to have been dispatched before the signal and the snapshot is what has to hold it
        late = socket.create_connection(("127.0.0.1", port))
        try:
            late.settimeout(5)
            late.sendall(_resp(b"SET", b"late", b"v"))
            assert late.recv(64) == b"+OK\r\n"
        finally:
            late.close()
        signalled = time.time()
        proc.send_signal(signal.SIGTERM)
        returned, rc = _returns_within(proc, drain + SELECT_TIMEOUT_SECONDS + _MARGIN_SECONDS)
    finally:
        client.close()
    assert (returned, rc) == (True, 0)
    # a filesystem with coarse timestamps can round a fresh file down, but never to the seed's hour-old mtime, so a second's slack still tells a replaced file from a stale one
    assert os.path.getmtime(snapshot) >= signalled - 1.0, "the snapshot on disk is the one from before the signal"
    loaded = persistence.load(str(snapshot))
    assert set(loaded.live_keys()) == {b"before", b"k", b"big", b"late"}
    assert loaded.lookup(b"big") == b"x" * _VALUE_BYTES


def test_the_drain_bound_is_asserted_on_a_thread_join_not_a_sleep(start):
    # the instrument first: a process that ignores SIGTERM must read as "did not return" through the same join, or every pass below could be the clock and not the server
    stubborn = subprocess.Popen(
        [sys.executable, "-c",
         "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
         "print('up', flush=True); time.sleep(60)"],
        stdout=subprocess.PIPE)
    try:
        assert stubborn.stdout.readline() == b"up\n"
        stubborn.send_signal(signal.SIGTERM)
        returned, rc = _returns_within(stubborn, 0.5)
        assert (returned, rc) == (False, None), "the join reported a process that is still running as returned"
    finally:
        _reap(stubborn)

    drain = 1
    proc, port = start("--snapshot-interval", "3600", "--shutdown-drain-timeout", str(drain))
    client = _stalled_client(port)
    try:
        time.sleep(0.3)
        proc.send_signal(signal.SIGTERM)
        returned, rc = _returns_within(proc, drain + SELECT_TIMEOUT_SECONDS + _MARGIN_SECONDS)
    finally:
        client.close()
    assert (returned, rc) == (True, 0)


def test_a_cooperative_client_receives_its_whole_reply_before_exit(start, tmp_path):
    proc, port = start("--snapshot-interval", "3600", "--shutdown-drain-timeout", "10", *_NO_BACKPRESSURE_FLAGS)
    replies = 8
    client = _stalled_client(port, replies)
    try:
        time.sleep(0.3)
        proc.send_signal(signal.SIGTERM)
        # the snapshot appearing is the sign the loop has stopped and the drain is next, so every byte the client reads below comes from the drain and none from the main loop. there is deliberately no sleep followed by a poll() for a live process after it: the server stays up 0.6 to 1.7 s after the snapshot appears even at --shutdown-drain-timeout 0, so a poll() 0.2 s later cannot fire for the defect its message would name, a drain that was skipped, and under a skip-drain mutant this test fails at the byte comparison below with or without one. what observes the drain is that comparison
        _wait_until((tmp_path / "dump.mrdb").exists, 10, "the shutdown save")
        received = _read_total(client, replies * len(_bulk(b"x" * _VALUE_BYTES)))
        returned, rc = _returns_within(proc, 5)
    finally:
        client.close()
    assert received == _bulk(b"x" * _VALUE_BYTES) * replies, len(received)
    assert (returned, rc) == (True, 0)


def test_a_cooperative_client_exits_well_inside_the_timeout(start):
    drain = 10
    proc, port = start("--snapshot-interval", "3600", "--shutdown-drain-timeout", str(drain), *_NO_BACKPRESSURE_FLAGS)
    idle = socket.create_connection(("127.0.0.1", port))
    replies = 2
    client = _stalled_client(port, replies)
    try:
        idle.settimeout(5)
        idle.sendall(_resp(b"SET", b"k2", b"v"))
        assert idle.recv(64) == b"+OK\r\n"
        time.sleep(0.3)
        proc.send_signal(signal.SIGTERM)
        received = _read_total(client, replies * len(_bulk(b"x" * _VALUE_BYTES)))
        # half the timeout, because a drain that sits out its whole timeout after everything was delivered is the one defect a control that passes with no drain at all cannot show
        returned, rc = _returns_within(proc, drain / 2)
    finally:
        idle.close()
        client.close()
    assert len(received) == replies * len(_bulk(b"x" * _VALUE_BYTES))
    assert (returned, rc) == (True, 0)


def test_a_request_arriving_during_the_drain_does_not_cost_the_client_its_replies(start, tmp_path):
    replies = 4
    proc, port = start("--snapshot-interval", "3600", "--shutdown-drain-timeout", "20", *_NO_BACKPRESSURE_FLAGS)
    client = _stalled_client(port, replies)
    expected = _bulk(b"x" * _VALUE_BYTES) * replies
    received = bytearray()
    reset = None
    try:
        time.sleep(0.3)
        proc.send_signal(signal.SIGTERM)
        # the snapshot is written after the loop stops and before the drain starts, so a request sent once it exists arrives during the save or the drain, never to the main loop. there is deliberately no margin sleep and no poll() for a live process after the wait: whether the PING reaches the drain's setup pass or one of its later passes the outcome is the same, since both read and discard it, and the process being alive 0.2 s on says nothing the replies read below do not
        _wait_until((tmp_path / "dump.mrdb").exists, 10, "the shutdown save")
        client.sendall(_resp(b"PING"))
        # slowly, so the server's last byte is handed to the kernel while most of what it owes is still in flight: a reader that kept up would have an empty queue at the close and never see a reset
        client.settimeout(20)
        try:
            while len(received) < len(expected):
                chunk = client.recv(1 << 16)
                if not chunk:
                    break
                received += chunk
                time.sleep(0.002)
        except ConnectionResetError as exc:
            reset = exc
        returned, rc = _returns_within(proc, 10)
    finally:
        client.close()
    # a request left unread in the server's receive queue makes the close that follows send a reset instead of a FIN, and a reset discards the closing socket's own send queue: what the server had handed its kernel and the kernel had not yet put on the wire. what the client had already received is not taken back, so the loss is the tail
    assert reset is None, "reset after %d of %d bytes: %r" % (len(received), len(expected), reset)
    # exact, not a minimum: the request sent after the signal is read and discarded, so a reply to it arriving behind the owed ones is a command dispatched during the drain
    assert len(received) == len(expected), "%d bytes arrived where %d were owed" % (len(received), len(expected))
    assert bytes(received) == expected, "the replies arrived altered"
    assert (returned, rc) == (True, 0)


# the load is slowed by the driver and not by a snapshot big enough to be slow, because the window under test is the one inside Server's construction and a stop sent at a marker the process itself prints lands in it on every run, where one sent on a timer lands in it on a good day. the driver is main() unchanged: only persistence.load is wrapped, to say it has begun and to take longer
_LOAD_SECONDS = 1.0
_SLOW_LOAD_THEN_MAIN = """
import sys, time
sys.path.insert(0, %r)
import persistence, server

real_load = persistence.load


def slow_load(path):
    print("loading", flush=True)
    time.sleep(%r)
    return real_load(path)


persistence.load = slow_load
server.main(sys.argv[1:])
"""


def test_a_sigterm_during_a_slow_snapshot_load_is_honoured_and_not_discarded(tmp_path):
    # the load runs inside Server's construction, ahead of the handlers run() installs, so a SIGTERM
    # that arrived during it had its default disposition: the process ended with no save, and as PID 1
    # in a container the kernel discarded it outright and the container served on until SIGKILL. the
    # window scaled with the snapshot. what is asserted is the process's own account of it: a clean
    # exit, the snapshot written by the stop, and the drain's one line
    snapshot = tmp_path / "dump.mrdb"
    older = Store()
    older.write(b"before", b"1", keep_ttl=False)
    persistence.save(older, str(snapshot))
    an_hour_ago = time.time() - 3600
    os.utime(snapshot, (an_hour_ago, an_hour_ago))

    proc = subprocess.Popen(
        [sys.executable, "-c", _SLOW_LOAD_THEN_MAIN % (str(REPO_ROOT), _LOAD_SECONDS),
         "--port", "0", "--snapshot-path", str(snapshot)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=tmp_path)
    try:
        ready, _, _ = select.select([proc.stdout], [], [], 10)
        line = proc.stdout.readline() if ready else b""
        assert line == b"loading\n", ("the load never began", line)
        signalled = time.time()
        proc.send_signal(signal.SIGTERM)
        returned, rc = _returns_within(proc, _LOAD_SECONDS + SELECT_TIMEOUT_SECONDS + _MARGIN_SECONDS)
        # read only once it has exited, since a process that is still running would hold this read open for ever
        stderr = proc.stderr.read().decode() if returned else ""
    finally:
        _reap(proc)
        proc.stderr.close()
    assert returned, "the server was still running after the load and a margin: the stop was lost"
    assert rc == 0, (rc, stderr)
    lines = [entry for entry in stderr.splitlines() if "shutdown drain" in entry]
    assert len(lines) == 1 and "shutdown drain complete" in lines[0], stderr
    assert os.path.getmtime(snapshot) >= signalled - 1.0, "the snapshot on disk is the one from before the signal"
    assert set(persistence.load(str(snapshot)).live_keys()) == {b"before"}


# SIGINT is bound explicitly in the child, so that what main() is asked to leave alone is a handler that raises. a process started from a non-interactive background shell inherits SIGINT ignored, Python then leaves it ignored, and the test would be asking about a signal nothing ever delivers
_CTRL_C_RAISES = "import signal\nsignal.signal(signal.SIGINT, signal.default_int_handler)\n"


def test_a_sigint_during_a_slow_snapshot_load_still_aborts_it(tmp_path):
    # the other half of the test above, and the reason SIGINT is not in the startup record: Ctrl-C
    # during a load that is taking too long has to end the process, as it did before the record
    # existed, and not wait for the load to finish and then exit 0. measured on a 79.5 MB snapshot,
    # a process that recorded SIGINT exited 0 at 5.52 s and one that did not died of the signal at
    # 0.22 s. the load here sleeps for _LOAD_SECONDS, so a process that outlived the signal by that
    # long finished it
    snapshot = tmp_path / "dump.mrdb"
    older = Store()
    older.write(b"before", b"1", keep_ttl=False)
    persistence.save(older, str(snapshot))

    proc = subprocess.Popen(
        [sys.executable, "-c", _CTRL_C_RAISES + _SLOW_LOAD_THEN_MAIN % (str(REPO_ROOT), _LOAD_SECONDS),
         "--port", "0", "--snapshot-path", str(snapshot)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=tmp_path)
    try:
        ready, _, _ = select.select([proc.stdout], [], [], 10)
        line = proc.stdout.readline() if ready else b""
        assert line == b"loading\n", ("the load never began", line)
        proc.send_signal(signal.SIGINT)
        returned, rc = _returns_within(proc, _LOAD_SECONDS + SELECT_TIMEOUT_SECONDS + _MARGIN_SECONDS)
        stderr = proc.stderr.read().decode() if returned else ""
    finally:
        _reap(proc)
        proc.stderr.close()
    assert returned, "the server was still running after the load and a margin: Ctrl-C did not abort it"
    assert rc != 0, ("the process exited cleanly, which is what a recorded SIGINT does", stderr)
    assert "KeyboardInterrupt" in stderr, stderr
    assert "shutdown drain" not in stderr, ("the load was allowed to finish and the stop ran", stderr)


# every byte in this is sent and none is dispatched: the command is cut inside its last element, so
# the server holds a multibulk it can never complete. a number that is neither a round figure nor the
# size of any other thing in this module, so that a figure taken from the wrong place cannot equal it
_UNFINISHED_COMMAND = _resp(b"SET", b"never-completed", b"v" * 41)[:-17]


@pytest.mark.parametrize("drain_timeout", ["0", "5"], ids=["no drain", "the shipped drain"])
@pytest.mark.parametrize("read_first", [False, True], ids=["unread at the stop", "read and parsed at the stop"])
def test_a_real_servers_drain_line_reports_the_request_bytes_the_stop_discarded(
        tmp_path, drain_timeout, read_first):
    # the figure is asserted where it is read, on the line a real process writes to stderr, and by
    # value. everything else that checks the figure runs Server.run() on the test's own thread or
    # drives the drain directly, and a figure that is correct there and absent from the process --
    # the line formatted from the wrong attribute, the counting switched off by something main()
    # does -- would pass all of them. a half-sent command is the shape this can build exactly: it
    # is never dispatched, and the sites that hold its bytes are disjoint, so what the line reports is
    # their sum and the sum is what the client sent. which sites hold them is the parameter. unread,
    # the stop is sent as soon as the server's kernel has the bytes, which finds them still in a
    # receive queue or in the accept backlog and has them counted there. read and parsed, a round trip
    # on a second connection comes first: its request is sent after the first client's bytes are in
    # the server's kernel, so the pass that answers it has those bytes among its ready events and has
    # read them before it ends, and the stop then finds a multibulk's finished elements in the
    # connection's own count and the rest in its read buffer. the waits are on conditions and not on a
    # clock: the client's own count of bytes the server's kernel has not acknowledged has to reach
    # zero, since a byte still in the client's send queue is the one thing no count on the server's
    # side can see
    proc = subprocess.Popen(
        [sys.executable, str(REPO_ROOT / "server.py"), "--port", "0",
         "--snapshot-path", str(tmp_path / "dump.mrdb"), "--snapshot-interval", "3600",
         "--shutdown-drain-timeout", drain_timeout],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=tmp_path)
    clients = []
    try:
        ready, _, _ = select.select([proc.stdout], [], [], 10)
        line = proc.stdout.readline().decode() if ready else ""
        assert line.startswith("listening on "), ("the server never said where it listened", line)
        port = int(line.strip().rsplit(":", 1)[1])
        client = socket.create_connection(("127.0.0.1", port))
        clients.append(client)
        client.sendall(_UNFINISHED_COMMAND)
        _wait_until(lambda: not _unacknowledged_bytes(client), 10, "the server's kernel to take every byte sent")
        if read_first:
            other = socket.create_connection(("127.0.0.1", port))
            clients.append(other)
            other.settimeout(10)
            other.sendall(_resp(b"PING"))
            assert other.recv(64) == b"+PONG\r\n"
        proc.send_signal(signal.SIGTERM)
        returned, rc = _returns_within(proc, int(drain_timeout) + SELECT_TIMEOUT_SECONDS + _MARGIN_SECONDS)
        stderr = proc.stderr.read().decode() if returned else ""
    finally:
        for opened in clients:
            opened.close()
        _reap(proc)
        proc.stderr.close()
    assert (returned, rc) == (True, 0), (returned, rc)
    lines = [entry for entry in stderr.splitlines() if "shutdown drain" in entry]
    assert len(lines) == 1, stderr
    found = re.search(r"request bytes discarded undispatched: (\d+)$", lines[0])
    assert found, lines[0]
    assert int(found.group(1)) == len(_UNFINISHED_COMMAND), (found.group(1), len(_UNFINISHED_COMMAND), lines[0])
    # the level is the documented one and is not the figure's to move: nothing was owed a reply, so the
    # line says complete over a figure that is not zero, and it is the figure that says what was thrown away
    assert "shutdown drain complete" in lines[0], lines[0]


# --- the tests below run Server.run() on this thread, with a second thread as the clients


class _Records(logging.Handler):
    def __init__(self, collecting=True):
        super().__init__(level=logging.DEBUG)
        self.collecting = collecting
        self.records = []

    def emit(self, record):
        if self.collecting:
            self.records.append(record)

    def messages(self):
        return [record.getMessage() for record in list(self.records)]


class _server_log:
    # DEBUG and INFO sit below the logger's inherited level, so it is lowered for the duration and put back: a record that was never emitted would make "exactly one line" pass for the wrong reason
    def __init__(self, collecting=True):
        self.handler = _Records(collecting)
        self.log = logging.getLogger("server")

    def __enter__(self):
        self.level = self.log.level
        self.log.setLevel(logging.DEBUG)
        self.log.addHandler(self.handler)
        return self.handler

    def __exit__(self, *exc_info):
        self.log.removeHandler(self.handler)
        self.log.setLevel(self.level)


class _Scene:
    def __init__(self, server):
        self.server = server
        self.listener = None
        self.port = None
        self.clients = []
        # commands the server has dispatched, counted at the hand-off from the read path so the count rises only once a batch has been answered. written by the server's thread and read by the scenario's, which a plain int allows because one side only adds
        self.dispatched = 0
        real_batch = server._dispatch_batch

        def counting_batch(conn, parsed_commands):
            real_batch(conn, parsed_commands)
            self.dispatched += len(parsed_commands)

        server._dispatch_batch = counting_batch

    def connect(self):
        client = socket.create_connection(("127.0.0.1", self.port))
        self.clients.append(client)
        return client

    def wait_for(self, condition, what, seconds=10):
        _wait_until(condition, seconds, what)

    def server_owes_bytes(self):
        try:
            return any(conn.write_buffer for conn in list(self.server._connections))
        except RuntimeError:
            # the set changed under this thread's copy of it, which only means the server was busy
            return False

    def stall(self, replies=16, size=2 << 20):
        client = self.connect()
        client.settimeout(10)
        before = self.dispatched
        client.sendall(_resp(b"SET", b"k", b"hello"))
        assert client.recv(64) == b"+OK\r\n"
        client.sendall(_resp(b"SET", b"big", b"x" * size))
        assert client.recv(64) == b"+OK\r\n"
        for _ in range(replies):
            client.sendall(_resp(b"GET", b"big"))
        # every request is waited for by name, and not inferred from an empty read buffer and a quiet select(): neither can see a request the client has sent that has not yet reached the server's receive queue, the stop that follows makes the drain read and discard it by design, and the client is then owed fewer replies than the test counts on -- every failure of the check this replaces was a whole number of replies short
        self.wait_for(lambda: self.dispatched - before == 2 + replies, "the server to dispatch every request it was sent")
        self.wait_for(self.server_owes_bytes, "the server to hold replies the client has not read")
        return client, replies * len(_bulk(b"x" * size))

    def run(self, scenario, patience=30):
        server = self.server
        real_open = server._open_listener

        def open_listener():
            self.listener = real_open()
            self.port = self.listener.getsockname()[1]
            return self.listener

        server._open_listener = open_listener
        # a short select timeout, so a stop request is noticed in tens of milliseconds and not a tenth of a second per step
        server._loop._timeout = 0.02
        failure = []

        def play():
            try:
                self.wait_for(lambda: self.port is not None, "the listener")
                scenario()
            except BaseException as exc:
                failure.append(exc)
            finally:
                # always, so a scenario that raised cannot leave run() looping on this thread for ever
                server._request_stop(None, None)

        player = threading.Thread(target=play, daemon=True)
        player.start()
        try:
            server.run()
        finally:
            player.join(patience)
        if failure:
            raise failure[0]

    def close(self):
        for client in self.clients:
            client.close()
        if self.listener is not None:
            self.listener.close()
        self.server._loop.close()


@pytest.fixture
def scene_for():
    scenes = []

    def make(server):
        scene = _Scene(server)
        scenes.append(scene)
        return scene

    yield make
    for scene in scenes:
        scene.close()


def _server(home, **overrides):
    settings = {
        "snapshot_path": str(home / "dump.mrdb"),
        "snapshot_interval": 3600,
        "shutdown_drain_timeout": 1,
    }
    settings.update(overrides)
    return Server(0, **settings)


def _listener_registered(server):
    # the listener is the one registration whose key data is None
    return any(key.data is None for key in server._loop._selector.get_map().values())


def test_the_listener_is_unregistered_before_the_save(tmp_path, scene_for):
    server = _server(tmp_path)
    scene = scene_for(server)
    serving = []
    saves = []
    real_save = server._save_snapshot
    real_run_once = server._loop.run_once

    def spying_save():
        saves.append(_listener_registered(server))
        real_save()

    def spying_run_once():
        if not serving:
            serving.append(_listener_registered(server))
        real_run_once()

    server._save_snapshot = spying_save
    server._loop.run_once = spying_run_once
    scene.run(lambda: scene.wait_for(lambda: serving, "the loop's first pass"))
    # the control first: the loop did hold the listener, so a False below is a change and not an instrument that cannot see it
    assert serving == [True]
    assert saves == [False], "the listener was still in the select set when the save ran"


def test_run_holds_a_spare_descriptor_for_the_backlog_sweeps_and_the_first_sweep_gives_it_back(tmp_path, scene_for):
    # the reserve is what lets the sweeps accept when the process has no descriptor left, and it is only
    # there if run() opens it once it has its listener and only free again if the first sweep gives it
    # back before accepting. the drain is spied at both ends: its entry, where the reserve has to still
    # be held, since an earlier release would leave the sweep nothing to give back, and its exit, where
    # the entry sweep has released it
    server = _server(tmp_path)
    scene = scene_for(server)
    seen = {}
    real_drain = server._drain_for

    def spying_drain(seconds, listener=None):
        seen["at the drain's entry"] = server._spare_fd
        real_drain(seconds, listener)
        seen["after the drain"] = server._spare_fd

    server._drain_for = spying_drain
    held_while_serving = []

    def scenario():
        scene.wait_for(lambda: server._spare_fd is not None, "run() to hold its spare descriptor")
        held_while_serving.append(server._spare_fd)

    scene.run(scenario)
    assert held_while_serving, "run() never held a spare descriptor while it served"
    assert seen["at the drain's entry"] == held_while_serving[0], seen
    assert seen["after the drain"] is None, "the entry sweep did not give the spare descriptor back"
    assert server._spare_fd is None


def test_a_run_that_raises_before_the_backlog_sweep_still_gives_its_spare_descriptor_back(tmp_path, monkeypatch):
    # the reserve is released by the first sweep on the way out of a stop, and a run() that ends some other
    # way -- here, a raise out of the loop -- never reaches a sweep, so what returns it is run()'s own exit
    server = _server(tmp_path)
    held = []
    real_hold = server._hold_spare_descriptor

    def recording_hold():
        real_hold()
        held.append(server._spare_fd)

    def exploding_tick():
        raise RuntimeError("injected: the loop failed")

    monkeypatch.setattr(server, "_hold_spare_descriptor", recording_hold)
    monkeypatch.setattr(server, "_tick", exploding_tick)
    try:
        with pytest.raises(RuntimeError, match="injected"):
            server.run()
    finally:
        server._loop.close()
    assert held and held[0] is not None, "run() never held a spare descriptor"
    assert server._spare_fd is None
    with pytest.raises(OSError) as raised:
        os.fstat(held[0])
    assert raised.value.errno == errno.EBADF, "the spare descriptor was left open when run() raised"


def test_a_connection_attempted_during_the_drain_is_not_accepted(tmp_path, scene_for):
    server = _server(tmp_path, **_NO_BACKPRESSURE)
    scene = scene_for(server)
    passes = []
    late_connected = threading.Event()
    real_run_once = server._loop.run_once

    def spying_run_once():
        # every pass after a stop was asked for: the drain's, and at most one the main loop was already inside
        if not server._running:
            passes.append((server.connected_clients, _listener_registered(server), late_connected.is_set()))
        real_run_once()

    server._loop.run_once = spying_run_once

    def scenario():
        scene.stall()
        server._request_stop(None, None)
        # two, because the first may be the main loop's last pass and the connect must come after a whole pass that had no listener
        scene.wait_for(lambda: len(passes) >= 2, "the drain to be running")
        scene.connect()
        late_connected.set()
        scene.wait_for(lambda: sum(1 for p in passes if p[2]) >= 5, "five drain passes after the late connect")

    scene.run(scenario)
    after = [p for p in passes if p[2]]
    assert len(after) >= 5
    # the handshake completes in the kernel's backlog whatever this does, so the connection set is what shows whether anything accepted it
    assert {count for count, _, _ in after} == {1}, passes
    assert not any(registered for _, registered, _ in after)


def test_the_save_runs_before_the_drain_not_after(tmp_path, scene_for):
    server = _server(tmp_path, **_NO_BACKPRESSURE)
    scene = scene_for(server)
    snapshot = tmp_path / "dump.mrdb"
    at_entry = {}
    real_drain = server._drain_for

    def spying_drain(seconds, listener=None):
        # asked of the filesystem and not of a flag: what matters is that the bytes were on disk when the drain began
        at_entry["exists"] = snapshot.exists()
        if at_entry["exists"]:
            at_entry["keys"] = set(persistence.load(str(snapshot)).live_keys())
        real_drain(seconds, listener)

    server._drain_for = spying_drain

    def scenario():
        scene.stall()
        server._request_stop(None, None)

    scene.run(scenario)
    assert at_entry.get("exists"), "the drain began with no snapshot on disk"
    assert at_entry["keys"] == {b"k", b"big"}


def test_a_shutdown_save_happens_with_the_snapshot_interval_at_zero(tmp_path, scene_for):
    server = _server(tmp_path, snapshot_interval=0)
    scene = scene_for(server)
    snapshot = tmp_path / "dump.mrdb"
    before_stop = {}

    def scenario():
        client = scene.connect()
        client.settimeout(5)
        client.sendall(_resp(b"SET", b"k", b"v"))
        assert client.recv(64) == b"+OK\r\n"
        # nothing is armed and nothing has been written, so the file that appears can only be the save on the way out
        before_stop["armed"] = server.armed_snapshot_interval
        before_stop["exists"] = snapshot.exists()

    scene.run(scenario)
    assert before_stop == {"armed": 0, "exists": False}
    assert sorted(persistence.load(str(snapshot)).live_keys()) == [b"k"]


def test_an_ignored_snapshot_is_not_overwritten_when_periodic_saving_is_off(start, tmp_path):
    snapshot = tmp_path / "dump.mrdb"
    seed = Store()
    for key, value in ((b"cart:42", b"3"), (b"sessions", b"7"), (b"users", b"11")):
        seed.write(key, value, keep_ttl=False)
    persistence.save(seed, str(snapshot))
    seeded = snapshot.read_bytes()

    drain = 1
    proc, port = start("--ignore-snapshot", "--snapshot-interval", "0", "--shutdown-drain-timeout", str(drain))
    client = socket.create_connection(("127.0.0.1", port))
    try:
        client.settimeout(5)
        # the keyspace started empty and now holds one key the seed does not, so a save on the way out has something different to write and the bytes could not come back the same by accident
        client.sendall(_resp(b"SET", b"fresh", b"v"))
        assert client.recv(64) == b"+OK\r\n"
        client.sendall(_resp(b"DBSIZE"))
        assert client.recv(64) == b":1\r\n", "the seeded snapshot was loaded, so nothing was ignored"
        proc.send_signal(signal.SIGTERM)
        returned, rc = _returns_within(proc, drain + SELECT_TIMEOUT_SECONDS + _MARGIN_SECONDS)
    finally:
        client.close()
    assert (returned, rc) == (True, 0)
    # the bytes and not the mtime or the size: a save of a different keyspace can come out the same length
    assert snapshot.read_bytes() == seeded
    assert sorted(persistence.load(str(snapshot)).live_keys()) == [b"cart:42", b"sessions", b"users"]


def test_an_ignored_snapshot_is_still_replaced_on_a_stop_when_a_save_interval_is_set(start, tmp_path):
    snapshot = tmp_path / "dump.mrdb"
    seed = Store()
    for key, value in ((b"cart:42", b"3"), (b"sessions", b"7"), (b"users", b"11")):
        seed.write(key, value, keep_ttl=False)
    persistence.save(seed, str(snapshot))
    seeded = snapshot.read_bytes()

    drain = 1
    # an interval far too long to come due while the test runs, so the only thing that can write the file is the save on the way out
    proc, port = start("--ignore-snapshot", "--snapshot-interval", "3600", "--shutdown-drain-timeout", str(drain))
    client = socket.create_connection(("127.0.0.1", port))
    try:
        client.settimeout(5)
        client.sendall(_resp(b"SET", b"fresh", b"v"))
        assert client.recv(64) == b"+OK\r\n"
        client.sendall(_resp(b"DBSIZE"))
        assert client.recv(64) == b":1\r\n", "the seeded snapshot was loaded, so nothing was ignored"
        assert snapshot.read_bytes() == seeded, "the file changed before the stop, so the stop is not what wrote it"
        proc.send_signal(signal.SIGTERM)
        returned, rc = _returns_within(proc, drain + SELECT_TIMEOUT_SECONDS + _MARGIN_SECONDS)
    finally:
        client.close()
    assert (returned, rc) == (True, 0)
    # the bytes and the keyspace, not the mtime or the size: a save of a different keyspace can come out the same length
    assert snapshot.read_bytes() != seeded, "the stop left the ignored snapshot in place"
    assert sorted(persistence.load(str(snapshot)).live_keys()) == [b"fresh"]


def test_no_snapshot_is_written_when_no_path_is_configured(tmp_path, scene_for, monkeypatch):
    # a scratch cwd, so a save that fell back to ./dump.mrdb would land where it can be seen and not in the repository
    monkeypatch.chdir(tmp_path)
    saves = []
    real_save = persistence.save
    monkeypatch.setattr(persistence, "save", lambda *args: (saves.append(args), real_save(*args)))
    server = Server(0, shutdown_drain_timeout=1)
    scene = scene_for(server)

    def scenario():
        client = scene.connect()
        client.settimeout(5)
        client.sendall(_resp(b"SET", b"k", b"v"))
        assert client.recv(64) == b"+OK\r\n"

    with _server_log() as log:
        scene.run(scenario)
    assert saves == []
    assert list(tmp_path.iterdir()) == []
    # the guard is what keeps this quiet: without it the save is attempted against no path and its failure is caught and logged, which leaves the directory empty too
    assert [r for r in log.records if r.levelno >= logging.ERROR] == []


def test_a_failing_shutdown_save_still_drains_and_still_exits(tmp_path, scene_for):
    snapdir = tmp_path / "snap"
    snapdir.mkdir()
    server = _server(tmp_path, snapshot_path=str(snapdir / "dump.mrdb"), shutdown_drain_timeout=5,
                     **_NO_BACKPRESSURE)
    scene = scene_for(server)
    outcome = {}

    with _server_log() as log:
        def scenario():
            client, expected = scene.stall()
            # removed from under the server, so the save's temporary file cannot be created: a real failure on the real path, where a stub would only show that the stub was called
            shutil.rmtree(snapdir)
            server._request_stop(None, None)
            scene.wait_for(
                lambda: any("shutdown snapshot save failed" in m for m in log.messages()),
                "the failed save to be reported")
            # read only now, after the main loop is over: only the drain can deliver what arrives
            outcome["received"] = len(_read_total(client, expected))
            outcome["expected"] = expected

        scene.run(scenario)
    assert outcome["received"] == outcome["expected"]
    failures = [r for r in log.records if "shutdown snapshot save failed" in r.getMessage()]
    assert len(failures) == 1 and failures[0].exc_info
    assert not snapdir.exists()


def test_a_drain_that_raises_still_closes_the_listener_and_the_selector(tmp_path, scene_for):
    server = _server(tmp_path)
    scene = scene_for(server)

    def exploding_drain(seconds, listener=None):
        raise RuntimeError("drain exploded")

    server._drain_for = exploding_drain
    clients = []

    def scenario():
        client = scene.connect()
        clients.append(client)
        scene.wait_for(lambda: server.connected_clients == 1, "the connection to be accepted")

    with pytest.raises(RuntimeError, match="drain exploded"):
        scene.run(scenario)
    assert scene.listener.fileno() == -1, "the listening socket was left open"
    assert server._loop._selector.get_map() is None, "the selector was left open"
    assert server.connected_clients == 0
    clients[0].settimeout(2)
    assert clients[0].recv(1) == b""


def test_a_drain_that_raises_still_logs_its_one_line(tmp_path, scene_for):
    server = _server(tmp_path, shutdown_drain_timeout=5)
    scene = scene_for(server)
    real_pass = server._loop.run_once
    drain_passes = []

    def exploding_pass():
        # only a pass the drain makes: the main loop's own passes are what carry the scenario as far as a stop, and the drain under test is the real one, so what is raised is raised from inside it
        if server._draining:
            drain_passes.append(1)
            raise RuntimeError("pass exploded")
        real_pass()

    server._loop.run_once = exploding_pass
    real_drain = server._drain_for

    with _server_log(collecting=False) as log:
        def spying_drain(seconds, listener=None):
            log.collecting = True
            try:
                real_drain(seconds, listener)
            finally:
                log.collecting = False

        server._drain_for = spying_drain

        def scenario():
            scene.stall()
            server._request_stop(None, None)

        with pytest.raises(RuntimeError, match="pass exploded"):
            scene.run(scenario)
    assert drain_passes == [1], "the drain made no pass, or kept going after one raised"
    assert len(log.records) == 1, [r.getMessage() for r in log.records]
    message = log.records[0].getMessage()
    # a stalled client's replies were still owed when the pass raised, which is a loss, so the one line is the WARNING and not a clean ending
    assert log.records[0].levelno == logging.WARNING and message.startswith("shutdown drain incomplete"), message
    assert _drain_counts(log.records[0]) == (0, 1), message
    assert scene.listener.fileno() == -1, "the listening socket was left open"
    assert server._loop._selector.get_map() is None, "the selector was left open"


def test_a_connection_owing_nothing_is_closed_before_the_drain_loop(tmp_path, scene_for):
    server = _server(tmp_path, **_NO_BACKPRESSURE)
    scene = scene_for(server)
    first_pass = []
    real_run_once = server._loop.run_once
    real_unregister = server._loop.unregister
    refused_once = []
    idle = []

    def spying_run_once():
        if not server._running and not first_pass:
            first_pass.append(server.connected_clients)
        real_run_once()

    def failing_unregister(conn):
        # the first close once the stop was asked for fails, as any statement in a close can: the walk is past the point where a guard applies, so what is left must still be closed
        if not server._running and not refused_once:
            refused_once.append(conn)
            raise OSError("injected: the selector refused to let go")
        real_unregister(conn)

    server._loop.run_once = spying_run_once
    server._loop.unregister = failing_unregister

    def scenario():
        for _ in range(3):
            quiet = scene.connect()
            idle.append(quiet)
            quiet.settimeout(5)
            quiet.sendall(_resp(b"PING"))
            assert quiet.recv(64) == b"+PONG\r\n"
        scene.stall()
        assert server.connected_clients == 4
        server._request_stop(None, None)

    with _server_log() as log:
        scene.run(scenario)
    # idle connections alongside a busy one is the ordinary case, and walking the set itself while those are closed out of it raises before the loop starts; the busy one is the only one left when the first pass runs
    assert first_pass == [1], first_pass
    assert len(refused_once) == 1, "the injected failure never fired, so the walk was not exercised"
    assert any("failed; abandoning it" in m for m in log.messages())
    for quiet in idle:
        quiet.settimeout(2)
        assert quiet.recv(1) == b""

def test_a_close_raising_at_teardown_still_closes_every_connection_that_owed_bytes(tmp_path, scene_for):
    server = _server(tmp_path, **_NO_BACKPRESSURE)
    scene = scene_for(server)
    held = []
    owing_at_the_deadline = []
    drained = []
    real_drain = server._drain_for
    real_unregister = server._loop.unregister

    def spying_drain(seconds, listener=None):
        try:
            real_drain(seconds, listener)
        finally:
            owing_at_the_deadline.extend(c for c in held if not c.closed and c.write_buffer)
            drained.append(True)

    def failing_unregister(conn):
        # only once the drain is over: the walk before it has its own test, and what is left for the teardown to close is the connections that owed bytes and were not served, which the walk never closes and nothing else covers
        if drained:
            raise RuntimeError("injected: the selector refused to let go")
        real_unregister(conn)

    server._drain_for = spying_drain
    server._loop.unregister = failing_unregister

    def scenario():
        for _ in range(3):
            scene.stall(replies=4)
        held.extend(server._connections)
        server._request_stop(None, None)

    raised = []
    try:
        scene.run(scenario)
    except RuntimeError as exc:
        raised.append(exc)
    assert len(held) == 3
    # the control: all three reached the teardown still owed bytes, so the loop under test had three connections to close and the first failure stood in front of two
    assert owing_at_the_deadline == held, "a connection was closed or served before the teardown, so it was not the one under test"
    assert [c.closed for c in held] == [True] * 3, "a close that raised stranded the connections behind it"
    assert raised == [], raised
    assert server._connections == set()


def test_the_drain_dispatches_no_command(tmp_path, scene_for, monkeypatch):
    server = _server(tmp_path, **_NO_BACKPRESSURE)
    scene = scene_for(server)
    dispatched = []
    real_dispatch = commands.dispatch

    def spying_dispatch(store, conn, argv):
        dispatched.append(list(argv))
        return real_dispatch(store, conn, argv)

    monkeypatch.setattr(commands, "dispatch", spying_dispatch)
    stopped = []
    ticks_in_drain = []
    inside = []
    real_run_once = server._loop.run_once
    real_drain = server._drain_for
    real_tick = server._tick

    def spying_run_once():
        # the flag as the drain sees it on every pass after a stop was asked for
        if not server._running:
            stopped.append(server._draining)
        real_run_once()

    def spying_drain(seconds, listener=None):
        inside.append(True)
        try:
            real_drain(seconds, listener)
        finally:
            inside.pop()

    def spying_tick():
        if inside:
            ticks_in_drain.append(True)
        real_tick()

    server._loop.run_once = spying_run_once
    server._drain_for = spying_drain
    server._tick = spying_tick

    def scenario():
        client, _ = scene.stall()
        server._request_stop(None, None)
        scene.wait_for(lambda: len(stopped) >= 2, "the drain to be running")
        # a pipelined write that arrives once the snapshot is already written: dispatching it would acknowledge a write that no snapshot holds
        client.sendall(_resp(b"SET", b"late", b"v"))
        scene.wait_for(lambda: len(stopped) >= 12, "the drain to have had the chance to read it")

    scene.run(scenario)
    assert not any(b"late" in argv for argv in dispatched), "the drain dispatched a command"
    # the periodic tick is the other thing that must not run here: its sweep deletes keys after the snapshot was written, which is a write no snapshot holds
    assert ticks_in_drain == [], "the drain ran the periodic tick"
    assert b"late" not in set(server._store.live_keys())
    # the first may be a pass of the main loop, so it is skipped: every one after it is the drain's, and each must see the flag set
    assert len(stopped) >= 12
    assert all(stopped[1:]), "a pass of the drain ran with the draining flag unset"


def test_a_zero_drain_timeout_skips_the_drain_entirely(tmp_path, scene_for):
    drain_passes = {}
    for timeout in (0, 1):
        home = tmp_path / ("timeout-%d" % timeout)
        home.mkdir()
        server = _server(home, shutdown_drain_timeout=timeout, **_NO_BACKPRESSURE)
        scene = scene_for(server)
        inside = []
        count = [0]
        real_drain = server._drain_for
        real_run_once = server._loop.run_once

        def spying_drain(seconds, listener=None, real_drain=real_drain, inside=inside):
            inside.append(True)
            try:
                real_drain(seconds, listener)
            finally:
                inside.pop()

        def spying_run_once(real_run_once=real_run_once, inside=inside, count=count):
            if inside:
                count[0] += 1
            real_run_once()

        server._drain_for = spying_drain
        server._loop.run_once = spying_run_once

        def scenario(scene=scene, server=server):
            scene.stall()
            server._request_stop(None, None)

        scene.run(scenario)
        drain_passes[timeout] = count[0]
    assert drain_passes[0] == 0, "a zero timeout still ran the drain loop"
    # the control: the same stalled client under a one second timeout is waited on for many passes, so zero above is the flag and not a scenario that never had bytes owed
    assert drain_passes[1] >= 10, drain_passes


def _read_it_all(scene, client, expected, home):
    scene.wait_for((home / "dump.mrdb").exists, "the shutdown save")
    assert len(_read_total(client, expected)) == expected


def _half_close_then_read(scene, client, expected, home):
    # the flag is set after the loop has stopped and the save is written, so a half-close sent once it is up reaches the drain; one sent earlier would reach the main loop, which answers an end of input the way it always does
    scene.wait_for(lambda: scene.server._draining, "the drain to begin")
    client.shutdown(socket.SHUT_WR)
    assert len(_read_total(client, expected)) == expected


def _vanish(scene, client, expected, home):
    scene.wait_for(lambda: scene.server._draining, "the drain to begin")
    # a reset and not a polite close: a zero linger makes close() send RST at once, so the client is gone whatever it had or had not read
    client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    client.close()


def _drain_lines(tmp_path, scene_for, name, timeout, act=None):
    home = tmp_path / name
    home.mkdir()
    server = _server(home, shutdown_drain_timeout=timeout, **_NO_BACKPRESSURE)
    scene = scene_for(server)
    real_drain = server._drain_for

    with _server_log(collecting=False) as log:
        def spying_drain(seconds, listener=None):
            log.collecting = True
            try:
                real_drain(seconds, listener)
            finally:
                log.collecting = False

        server._drain_for = spying_drain

        def scenario():
            client, expected = scene.stall()
            server._request_stop(None, None)
            if act is not None:
                act(scene, client, expected, home)

        scene.run(scenario)
    return log.records


def test_the_drain_logs_exactly_one_line_however_it_ends(tmp_path, scene_for):
    drained = _drain_lines(tmp_path, scene_for, "drained", 5, act=_read_it_all)
    timed_out = _drain_lines(tmp_path, scene_for, "timed-out", 1)
    skipped = _drain_lines(tmp_path, scene_for, "skipped", 0)
    for ending, records in (("drained", drained), ("timed out", timed_out), ("skipped", skipped)):
        assert len(records) == 1, (ending, [r.getMessage() for r in records])

    message = drained[0].getMessage()
    assert drained[0].levelno == logging.INFO and message.startswith("shutdown drain complete"), message
    # the counts are read by name and not off the end of the line: asserting the tail pins which figure is last rather than what any of them says, and a figure added to the line then moves the assertion without changing what it checks
    assert _drain_counts(drained[0]) == (0, 0), message
    # the third figure is asserted on its own, because the two reply counts cannot say anything about it: a request thrown away undispatched is not a reply anybody is owed. a stop that handed every reply over and read nothing it had to throw away reports zero request bytes discarded, and a figure taken from the wrong buffer or never filled in leaves both reply counts green
    assert _drain_discarded(drained[0]) == 0, message
    # a loss is a WARNING and a clean ending is not, so a configuration that shows warnings and above shows the line that says something was lost and not the one that says nothing was
    for ending, records in (("timed out", timed_out), ("skipped", skipped)):
        message = records[0].getMessage()
        assert records[0].levelno == logging.WARNING, (ending, message)
        assert message.startswith("shutdown drain incomplete"), (ending, message)
        assert _drain_counts(records[0])[1] == 1, (ending, message)
        # every request the client sent was dispatched before the stop, and the loss here is of replies, so there is no request for the drain to have discarded
        assert _drain_discarded(records[0]) == 0, (ending, message)


def _drain_figure(record, label):
    message = record.getMessage()
    found = re.search(r"%s: (\d+)" % label, message)
    assert found, (label, message)
    return int(found.group(1))


def _drain_counts(record):
    return (_drain_figure(record, "closed while owed bytes"),
            _drain_figure(record, "still owed bytes"))


def _drain_discarded(record):
    return _drain_figure(record, "request bytes discarded undispatched")


def test_a_client_that_half_closes_during_the_drain_still_receives_every_reply(tmp_path, scene_for, monkeypatch):
    ends_of_input = []
    real_receive = Connection.receive

    def watching_receive(conn):
        alive = real_receive(conn)
        if not alive:
            ends_of_input.append(conn.id)
        return alive

    monkeypatch.setattr(Connection, "receive", watching_receive)
    # finished sending and still reading is not a dead peer, and an end of input that closed the connection would discard everything it is owed. the read inside _half_close_then_read is what asserts all of it arrived
    records = _drain_lines(tmp_path, scene_for, "half-closed", 5, act=_half_close_then_read)
    assert len(records) == 1, [r.getMessage() for r in records]
    assert records[0].levelno == logging.INFO, records[0].getMessage()
    assert _drain_counts(records[0]) == (0, 0), records[0].getMessage()
    # once, and not on every pass: end of input stays readable, so a connection left registered for reading is reported readable by every select() for as long as the drain runs, and the loop spins at full speed on a socket that has nothing more to say
    assert len(ends_of_input) == 1, "the end of input was read %d times" % len(ends_of_input)

def test_eof_outside_the_drain_with_owed_bytes_closes_the_connection(tmp_path, scene_for):
    server = _server(tmp_path, **_NO_BACKPRESSURE)
    scene = scene_for(server)
    outcome = {}

    def scenario():
        client, _ = scene.stall()
        conn = next(iter(server._connections))
        # the control: replies are queued and unread at the moment of the half-close, which is the one thing that separates this from the end of input every other client ends with
        assert conn.write_buffer, "the server owed nothing, so this is not the case under test"
        assert server._running and not server._draining
        client.shutdown(socket.SHUT_WR)
        # the stop is never asked for here, so this is the main loop's own end of input: the drain's allowance for a client that is finished asking and still reading is the drain's alone, and a connection that kept it outside the drain would sit at an empty mask once its buffer emptied, with nothing left to close it
        scene.wait_for(lambda: server.connected_clients == 0, "the connection to be closed")
        outcome["closed"] = conn.closed
        outcome["draining"] = server._draining

    scene.run(scenario)
    assert outcome == {"closed": True, "draining": False}, outcome


def test_a_client_that_disappears_during_the_drain_is_reported_as_a_loss(tmp_path, scene_for):
    records = _drain_lines(tmp_path, scene_for, "vanished", 5, act=_vanish)
    # the drain writes exactly one line of its own. the connection that vanished also logs its close, at INFO, which is not the drain's, so that line is set aside and whatever else was written during the drain has to be the drain's one line: a second line of any other text fails the count
    records = [r for r in records if not r.getMessage().startswith("closed connection")]
    assert len(records) == 1, [r.getMessage() for r in records]
    # closed with its replies queued, and nothing is still owed once it is gone: a line that counted only what remained at the end would call this a clean shutdown, which is exactly the case it exists to report
    assert records[0].levelno == logging.WARNING, records[0].getMessage()
    assert records[0].getMessage().startswith("shutdown drain incomplete"), records[0].getMessage()
    assert _drain_counts(records[0]) == (1, 0), records[0].getMessage()

def test_a_connection_fully_served_and_then_closed_is_not_counted_as_a_loss(tmp_path, scene_for):
    server = _server(tmp_path, shutdown_drain_timeout=10, **_NO_BACKPRESSURE)
    scene = scene_for(server)
    real_drain = server._drain_for

    with _server_log(collecting=False) as log:
        def spying_drain(seconds, listener=None):
            log.collecting = True
            try:
                real_drain(seconds, listener)
            finally:
                log.collecting = False

        server._drain_for = spying_drain

        def scenario():
            finished, expected = scene.stall(replies=4)
            owing, _ = scene.stall(replies=4)
            server._request_stop(None, None)
            scene.wait_for(lambda: server._draining, "the drain to begin")
            assert len(_read_total(finished, expected)) == expected
            finished.close()
            # the second is read only after the first is gone, so the drain is still waiting on a connection that owes when the first one closes: every other drain test leaves its finished clients open, and a close of one that had been fully served is a different case from a close of one that was not
            scene.wait_for(lambda: server.connected_clients == 1, "the server to see the close")
            assert len(_read_total(owing, expected)) == expected

        scene.run(scenario)
    # the drain writes exactly one line of its own. the connection that was served and closed while the drain waited on the other also logs its close, at INFO, which is not the drain's, so that line is set aside and whatever else was written during the drain has to be the drain's one line: a second line of any other text fails the count
    log.records = [r for r in log.records if not r.getMessage().startswith("closed connection")]
    assert len(log.records) == 1, [r.getMessage() for r in log.records]
    # closed with an empty buffer is closed after everything was handed to the kernel, and only a close that discarded a reply is a loss: counting this one would report a clean shutdown as incomplete
    assert log.records[0].levelno == logging.INFO, log.records[0].getMessage()
    assert _drain_counts(log.records[0]) == (0, 0), log.records[0].getMessage()


def test_the_drain_discards_what_arrives_instead_of_holding_it(tmp_path, scene_for, monkeypatch):
    flood = 8 << 20
    server = _server(tmp_path, shutdown_drain_timeout=10, **_NO_BACKPRESSURE)
    scene = scene_for(server)
    left_standing = []
    read_in_drain = [0]
    real_run_once = server._loop.run_once
    real_receive = Connection.receive

    def spying_run_once():
        real_run_once()
        if server._draining:
            left_standing.append(max((len(conn.read_buffer) for conn in server._connections), default=0))

    def counting_receive(conn):
        before = len(conn.read_buffer)
        alive = real_receive(conn)
        if server._draining:
            read_in_drain[0] += len(conn.read_buffer) - before
        return alive

    server._loop.run_once = spying_run_once
    monkeypatch.setattr(Connection, "receive", counting_receive)

    def scenario():
        client, expected = scene.stall(replies=4)
        server._request_stop(None, None)
        scene.wait_for(lambda: server._draining, "the drain to begin")
        client.sendall(b"x" * flood)
        scene.wait_for(lambda: read_in_drain[0] >= flood, "the drain to read the whole flood")
        # the replies are read last, so the drain ends when they are delivered and not at its timeout
        assert len(_read_total(client, expected)) == expected

    scene.run(scenario)
    # the control: the instrument saw the flood go through the discard, so an empty result below is the discard and not a spy that watched nothing
    assert left_standing, "no pass of the drain was observed"
    assert read_in_drain[0] >= flood, read_in_drain[0]
    # zero and not a size: what the discard promises is that a pass leaves nothing behind it, and a buffer that keeps what it read ends the flood holding all of it
    assert max(left_standing) == 0, "a pass of the drain left %d unparsed bytes standing" % max(left_standing)


def test_the_drain_waits_for_every_connection_that_owes_bytes(tmp_path, scene_for):
    server = _server(tmp_path, shutdown_drain_timeout=10, **_NO_BACKPRESSURE)
    scene = scene_for(server)
    outcome = {}

    def scenario():
        first, expected = scene.stall(replies=4)
        second, _ = scene.stall(replies=4)
        server._request_stop(None, None)
        scene.wait_for(lambda: server._draining, "the drain to begin")
        # one after the other, so the first is complete while the second has not read a byte: a drain that ends when any connection has been served, instead of when none still owes, strands the second
        outcome["first"] = len(_read_total(first, expected))
        outcome["second"] = len(_read_total(second, expected))
        outcome["expected"] = expected

    scene.run(scenario)
    assert outcome["first"] == outcome["expected"]
    assert outcome["second"] == outcome["expected"], "the drain ended with the second connection still owed bytes"


# Darwin's SO_NWRITE, which this Python's socket module does not expose: sys/socket.h gives it as 0x1024
_SO_NWRITE = 0x1024


def _unacknowledged_bytes(client):
    # what this socket has taken from sendall() that the far end's kernel has not yet acknowledged. a
    # TCP receiver acknowledges bytes it has already put in the receive queue and never ones it has not,
    # so zero here is the sender saying that every byte it wrote is now sitting in the other kernel's
    # queue -- which is the one fact a test needs about a connection that nobody has accepted and so has
    # nothing on the server's side to ask. Darwin spells the question SO_NWRITE and Linux spells it
    # TIOCOUTQ, and both count the unsent bytes with the unacknowledged ones
    if sys.platform == "darwin":
        return client.getsockopt(socket.SOL_SOCKET, _SO_NWRITE)
    held = array.array("i", [0])
    fcntl.ioctl(client.fileno(), termios.TIOCOUTQ, held, True)
    return held[0]


def test_the_unacknowledged_byte_probe_reports_bytes_a_peer_has_not_read():
    # the control for the wait in the test below, which is only as good as its probe: one that answered
    # zero whatever it was asked would pass that wait at once and leave the race where it was. a sender
    # that writes until the kernel takes no more, to a peer that reads nothing, is the one state in
    # which the answer cannot be zero
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    client = socket.create_connection(listener.getsockname())
    accepted, _ = listener.accept()
    try:
        client.setblocking(False)
        try:
            # bounded, so a kernel that never refuses a write ends in the assertion and not in a hang
            for _ in range(1024):
                client.send(b"x" * 65536)
        except BlockingIOError:
            pass
        assert _unacknowledged_bytes(client) > 0, "the probe saw no bytes in a send queue the peer was not draining"
    finally:
        for sock in (client, accepted, listener):
            sock.close()


def test_clients_that_connect_after_the_stop_and_are_never_served_are_counted_in_the_drains_line(tmp_path, scene_for):
    # the listener leaves the select set when the loop stops and stays open until the teardown, so the
    # kernel goes on completing handshakes into its backlog for the whole of the save and the drain.
    # those clients are never served: nothing reads or answers them, and one that had sent bytes is
    # reset when the sweep closes it, as the listener's close would have reset it, while an idle one
    # sees an orderly close. measured, three of them sent 201,000 bytes of complete requests that the
    # line reported as 0.
    # they connect once the drain has made two passes, so the sweep at its entry has come and gone, and
    # this runs through run() and a real listener, which is the one place the listener's hand-over to
    # the drain is seen
    server = _server(tmp_path, shutdown_drain_timeout=10, **_NO_BACKPRESSURE)
    scene = scene_for(server)
    sizes = (30_000, 70_000, 101_000)
    passes = []
    real_run_once = server._loop.run_once
    real_drain = server._drain_for

    def counting_run_once():
        # the drain's passes and none of the main loop's, since the flag is set only on the way into it
        if server._draining:
            passes.append(1)
        real_run_once()

    server._loop.run_once = counting_run_once

    with _server_log(collecting=False) as log:
        def spying_drain(seconds, listener=None):
            log.collecting = True
            try:
                real_drain(seconds, listener)
            finally:
                log.collecting = False

        server._drain_for = spying_drain

        def scenario():
            client, expected = scene.stall()
            server._request_stop(None, None)
            scene.wait_for(lambda: len(passes) >= 2, "the drain to be running")
            late = []
            for size in sizes:
                late.append(scene.connect())
                late[-1].sendall(b"x" * size)
            # sendall() returns once the bytes are in the client's own send queue, and loopback moves
            # them to the other kernel a few hundred microseconds later. the connection is in nobody's
            # hands on the server's side, so there is nothing there to ask, and a sweep that runs in that
            # gap finds less than was sent -- measured over a real listener, 13 of 3,000 sweeps taken
            # straight after the three sends were short and 0 of 3,000 once every client's send queue
            # had emptied. the drain's final sweep is taken the moment the server's own buffer empties,
            # which a fast reader makes soon after the sends, so the test goes on only when no late
            # client has a byte the server's kernel has not acknowledged. the drain is held open by a
            # client that has read nothing until then, so the wait costs it nothing
            scene.wait_for(lambda: not any(_unacknowledged_bytes(c) for c in late),
                           "the late clients' bytes to reach the server's kernel")
            assert len(_read_total(client, expected)) == expected

        scene.run(scenario)
    lines = [r for r in log.records if "shutdown drain" in r.getMessage()]
    assert len(lines) == 1, [r.getMessage() for r in log.records]
    assert _drain_discarded(lines[0]) == sum(sizes), lines[0].getMessage()
    # the clients were owed nothing, so the line is the clean one: the figure beside the counts is what says they were thrown away
    assert lines[0].levelno == logging.INFO and _drain_counts(lines[0]) == (0, 0), lines[0].getMessage()


# --- the tests below run main() on this thread, with the stop signal arriving while it builds the server


class _StopHandlers:
    # what the process held for the two stop signals before main() ran, stood in for by handlers of the test's own and not the defaults, so that a main() that failed to install its recording one costs the test an assertion and not the pytest process, which a default-disposition SIGTERM would end
    def __init__(self):
        self.reached = []
        self.ours = {signum: self.note for signum in (signal.SIGINT, signal.SIGTERM)}

    def note(self, signum, frame):
        self.reached.append(signum)

    def held(self):
        return {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}


@pytest.fixture
def stop_handlers():
    handlers = _StopHandlers()
    previous = {signum: signal.signal(signum, handler) for signum, handler in handlers.ours.items()}
    yield handlers
    for signum, handler in previous.items():
        signal.signal(signum, handler)


@pytest.fixture
def servers_main_builds(monkeypatch):
    # the Servers main() constructs, so that every one has its loop closed whether or not run() got as far as closing it
    built = []

    class Recording(Server):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            built.append(self)

    monkeypatch.setattr(server_module, "Server", Recording)
    yield built
    for server in built:
        server._loop.close()


def _stop_while_main_loads_the_snapshot(home, monkeypatch, signum):
    snapshot = home / "dump.mrdb"
    older = Store()
    older.write(b"before", b"1", keep_ttl=False)
    persistence.save(older, str(snapshot))
    an_hour_ago = time.time() - 3600
    os.utime(snapshot, (an_hour_ago, an_hour_ago))
    real_load = persistence.load

    def load_then_stopped(path):
        # from inside Server's construction, which is where the load runs and where the stop used to find no handler. the signal is a real one and so is the handler that takes it
        os.kill(os.getpid(), signum)
        return real_load(path)

    def the_loop_body_ran(self):
        # the tick follows every pass of the loop and the drain never makes one, so reaching it means the loop was entered and the stop was not honoured. raised and not recorded, because a loop that is entered here never ends
        raise AssertionError("the loop ran: the stop that arrived during startup was not honoured")

    # a context, so that the tick is the real one again for a test that runs a server afterwards
    with monkeypatch.context() as patched:
        patched.setattr(persistence, "load", load_then_stopped)
        patched.setattr(Server, "_tick", the_loop_body_ran)
        patched.setattr(logging, "basicConfig", lambda **kwargs: None)
        signalled = time.time()
        with _server_log() as log:
            server_module.main(["--port", "0", "--snapshot-path", str(snapshot)])
    return snapshot, signalled, log.records


def test_a_stop_that_arrives_while_main_loads_the_snapshot_is_honoured_by_run(
        tmp_path, monkeypatch, servers_main_builds, stop_handlers):
    # SIGTERM alone: main() records that signal and leaves SIGINT as it found it, for the reason the
    # two tests that follow this one give. main() returning at all is the clean exit: a stop that was
    # lost leaves run() in its loop, which the tick above turns into a failure rather than a hang
    snapshot, signalled, records = _stop_while_main_loads_the_snapshot(tmp_path, monkeypatch, signal.SIGTERM)
    assert len(servers_main_builds) == 1
    lines = [r for r in records if "shutdown drain" in r.getMessage()]
    assert len(lines) == 1, [r.getMessage() for r in records]
    assert lines[0].levelno == logging.INFO and lines[0].getMessage().startswith("shutdown drain complete"), lines[0].getMessage()
    assert _drain_counts(lines[0]) == (0, 0), lines[0].getMessage()
    # the ordinary way out taken whole: the stop saves, and the snapshot is the one this process wrote and not the one it started from
    assert os.path.getmtime(snapshot) >= signalled - 1.0, "the snapshot on disk is the one from before the stop"
    assert set(persistence.load(str(snapshot)).live_keys()) == {b"before"}
    assert stop_handlers.reached == [], "the signal reached the handler main() found, so main() had installed none"


def test_a_sigint_while_main_loads_the_snapshot_is_not_recorded_and_still_aborts_the_load(
        tmp_path, monkeypatch, servers_main_builds, stop_handlers):
    # SIGINT is left out of the startup record on purpose, and this is the test that says so. the
    # defect the record closes is `docker stop`, which sends SIGTERM. recording SIGINT as well swallowed Ctrl-C during a slow load:
    # the load ran to its end and the process then exited 0 -- measured on a 79.5 MB snapshot, 5.52 s
    # after the signal where it had died of it in 0.22 s, and three Ctrl-Cs did not abort it -- which
    # took from an operator at a terminal the one way they had to give up on a load that was taking too
    # long. what the process held for SIGINT is Python's own handler, as it does in any interpreter
    # not started with SIGINT ignored, and a SIGINT inside the load raises KeyboardInterrupt out of it
    signal.signal(signal.SIGINT, signal.default_int_handler)
    with pytest.raises(KeyboardInterrupt):
        _stop_while_main_loads_the_snapshot(tmp_path, monkeypatch, signal.SIGINT)
    assert servers_main_builds == [], "a Server was built over a load that Ctrl-C was meant to abandon"
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler, "main() left SIGINT bound to something else"
    # and nothing is left recorded for a Server built directly afterwards
    assert server_module._consume_stop_requested_during_startup() is False


def test_the_stop_recorded_during_startup_is_consumed_so_the_next_run_in_the_process_still_serves(
        tmp_path, monkeypatch, servers_main_builds, stop_handlers, scene_for):
    # one stop is one stop: a request that run() read and left set would end the next Server run in this
    # process before its loop was entered, which is how a long-lived embedding program or a suite finds
    # out. the second server is built and run directly, because a second main() would clear the record
    # itself on the way in and hide a run() that did not
    first = tmp_path / "first"
    first.mkdir()
    _stop_while_main_loads_the_snapshot(first, monkeypatch, signal.SIGTERM)
    second = tmp_path / "second"
    second.mkdir()
    server = _server(second)
    scene = scene_for(server)
    answers = []

    def scenario():
        client = scene.connect()
        client.settimeout(5)
        client.sendall(_resp(b"PING"))
        try:
            answers.append(client.recv(64))
        except OSError as exc:
            # a loop that never ran has nothing to answer with, and a server that has already stopped resets the connection
            answers.append(repr(exc))

    scene.run(scenario)
    assert answers == [b"+PONG\r\n"], answers


@pytest.mark.parametrize(
    "way_out", ["a clean return", "a usage error", "a refused snapshot", "a run that raises"])
def test_main_gives_the_stop_handlers_back_on_every_way_out(
        tmp_path, monkeypatch, servers_main_builds, stop_handlers, way_out):
    # main() is also called in-process, and a recording handler left bound after it returned would
    # swallow every later SIGTERM in that process. each way out is reached from a point where the
    # handler is known to be in place, and the control is that it was not the test's own there, so
    # that a main() that never installed one cannot pass this by leaving the original alone. SIGINT is
    # the other half of the control and was changed deliberately: it is not replaced during main(),
    # because recording it as well swallowed Ctrl-C during startup, and a main() that replaced it
    # again would fail the assertion below that names it
    during = []
    argv = ["--port", "0", "--snapshot-path", str(tmp_path / "dump.mrdb")]
    outcome = None
    if way_out == "a clean return":
        monkeypatch.setattr(Server, "run", lambda self: during.append(stop_handlers.held()))
    elif way_out == "a run that raises":
        def exploding_run(self):
            during.append(stop_handlers.held())
            raise RuntimeError("run exploded")

        monkeypatch.setattr(Server, "run", exploding_run)
        outcome = RuntimeError
    elif way_out == "a refused snapshot":
        def refusing_load(path):
            during.append(stop_handlers.held())
            raise persistence.SnapshotError("injected: the snapshot is refused")

        monkeypatch.setattr(persistence, "load", refusing_load)
        outcome = SystemExit
    else:
        real_parser = server_module.build_arg_parser

        def recording_parser():
            during.append(stop_handlers.held())
            return real_parser()

        monkeypatch.setattr(server_module, "build_arg_parser", recording_parser)
        argv = ["--port", "not-a-port"]
        outcome = SystemExit
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)

    if outcome is None:
        server_module.main(argv)
    else:
        with pytest.raises(outcome):
            server_module.main(argv)

    assert len(during) == 1, "main() never reached the point this way out is taken from"
    assert during[0][signal.SIGTERM] != stop_handlers.ours[signal.SIGTERM], (
        "main() had not replaced the test's handler for SIGTERM there, so a restored handler below proves nothing")
    assert during[0][signal.SIGINT] == stop_handlers.ours[signal.SIGINT], (
        "main() replaced the handler for SIGINT, which would swallow Ctrl-C while the snapshot loads")
    assert stop_handlers.held() == stop_handlers.ours, "main() left its own handlers bound"


def test_main_starts_without_a_stop_request_an_earlier_main_left_behind(
        tmp_path, monkeypatch, servers_main_builds, stop_handlers):
    # a stop recorded by a main() that then failed before run() -- a refused snapshot is the ordinary
    # way -- is never consumed, and the next main() in the process must not begin by honouring it.
    # only the record itself can say so here: a stale one is read by run() exactly as a fresh one is
    monkeypatch.setattr(server_module, "_stop_requested_during_startup", True)
    seen = []
    monkeypatch.setattr(Server, "run", lambda self: seen.append(server_module._consume_stop_requested_during_startup()))
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    server_module.main(["--port", "0", "--snapshot-path", str(tmp_path / "dump.mrdb")])
    assert seen == [False]


def test_a_stop_that_lands_while_run_installs_its_handlers_is_still_honoured(
        tmp_path, monkeypatch, stop_handlers):
    # the order inside run() is the whole of the startup fix. the record is consumed after both of run()'s
    # handlers are armed, so a stop recorded while they are being armed is read by that consume and one
    # arriving later reaches _request_stop. consumed before them, a stop that lands in between sets the
    # record again after it was read, nothing ever reads it, and the server serves on: the whole suite
    # passed with the two moved, which is how this test came to be written. the stop is landed from inside
    # the install of run()'s first handler, the latest point at which the record can be written and the
    # handlers still not all be in place, and the watchdog is what turns a loop that never ends into a
    # failure that does
    server = _server(tmp_path)
    real_signal = signal.signal
    landed = []
    ticks = []
    watchdog_fired = []

    def installing(signum, handler):
        previous = real_signal(signum, handler)
        if handler == server._request_stop and not landed:
            landed.append(signum)
            server_module._note_stop_requested_during_startup(signum, None)
        return previous

    def watchdog():
        watchdog_fired.append(True)
        server._request_stop(None, None)

    monkeypatch.setattr(signal, "signal", installing)
    monkeypatch.setattr(server, "_tick", lambda: ticks.append(1))
    timer = threading.Timer(5, watchdog)
    timer.daemon = True
    timer.start()
    started = time.monotonic()
    try:
        server.run()
    finally:
        timer.cancel()
        server._loop.close()
    elapsed = time.monotonic() - started
    assert landed, "run() never installed a handler for the stop to land in"
    assert not watchdog_fired, "run() was still looping after five seconds: the stop that landed was never read"
    assert ticks == [], "the loop ran: the stop that landed while the handlers were armed was not honoured"
    assert server._running is False
    assert elapsed < 5, elapsed


@pytest.mark.parametrize(
    "way_out", ["a usage error", "a refused snapshot", "an OSError out of construction"])
def test_a_stop_recorded_by_a_main_that_died_before_run_does_not_end_a_server_built_directly_afterwards(
        tmp_path, monkeypatch, servers_main_builds, stop_handlers, scene_for, way_out):
    # main() cleared the record on the way in and nowhere else, so a main() that recorded a stop and died
    # before run() left it set, and a Server built directly afterwards honoured a request that was made of
    # another one: it printed `listening on`, ran no iteration of its loop and returned. each way out is
    # one main() really has between the handler going in and run() being reached, and the signal is a real
    # one, delivered from inside it
    first = tmp_path / "first"
    first.mkdir()
    argv = ["--port", "0", "--snapshot-path", str(first / "dump.mrdb")]
    outcome = SystemExit
    # a context, so that the load and the parser are the real ones again for the server built below
    with monkeypatch.context() as patched:
        patched.setattr(logging, "basicConfig", lambda **kwargs: None)
        if way_out == "a usage error":
            real_parser = server_module.build_arg_parser

            def stopped_parser():
                os.kill(os.getpid(), signal.SIGTERM)
                return real_parser()

            patched.setattr(server_module, "build_arg_parser", stopped_parser)
            argv = ["--port", "not-a-port"]
        else:
            def stopped_load(path):
                os.kill(os.getpid(), signal.SIGTERM)
                if way_out == "a refused snapshot":
                    raise persistence.SnapshotError("injected: the snapshot is refused")
                raise OSError(5, "injected: an unexpected OSError out of construction")

            patched.setattr(persistence, "load", stopped_load)
            if way_out == "an OSError out of construction":
                outcome = OSError
        with pytest.raises(outcome):
            server_module.main(argv)
    assert stop_handlers.reached == [], "the signal reached the handler main() found, so main() had installed none"

    second = tmp_path / "second"
    second.mkdir()
    server = _server(second)
    scene = scene_for(server)
    answers = []

    def scenario():
        client = scene.connect()
        client.settimeout(5)
        try:
            client.sendall(_resp(b"PING"))
            answers.append(client.recv(64))
        except OSError as exc:
            # a loop that never ran has nothing to answer with, and a server that has already stopped resets the connection
            answers.append(repr(exc))

    scene.run(scenario)
    assert answers == [b"+PONG\r\n"], answers


def test_a_negative_drain_timeout_is_refused_without_recommending_zero(capsys):
    # --shutdown-drain-timeout is the one limit flag where 0 is the restrictive value, and -1 the likeliest spelling of unlimited: the refusal that tells everyone else to use 0 would tell this operator to switch the drain off
    with pytest.raises(SystemExit) as raised:
        build_arg_parser().parse_args(["--shutdown-drain-timeout", "-1"])
    assert raised.value.code == 2
    message = capsys.readouterr().err
    assert "shutdown drain timeout cannot be negative" in message, message
    assert "disables" not in message, message
    assert "_shutdown_drain_timeout" not in message and "_check_not_negative" not in message, message
    with pytest.raises(ValueError, match="shutdown_drain_timeout cannot be negative") as refused:
        Server(0, shutdown_drain_timeout=-1)
    assert "disables" not in str(refused.value), str(refused.value)
    # what it says instead is what 0 means here
    for text in (message, str(refused.value)):
        assert "unlimited" in text and "no drain" in text, text

    # the controls: the shared refusal is untouched for every other flag, so the difference above is this flag's own
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args(["--max-connections", "-1"])
    assert "0 disables the check, not -1" in capsys.readouterr().err
    with pytest.raises(ValueError, match="0 disables the check, not -1"):
        Server(0, max_connections=-1)
    # zero itself is accepted, at both doors, and means what the flag's help says
    assert build_arg_parser().parse_args(["--shutdown-drain-timeout", "0"]).shutdown_drain_timeout == 0
    server = Server(0, shutdown_drain_timeout=0)
    server._loop.close()


@pytest.mark.parametrize("too_big", [MAX_SCHEDULABLE_INTERVAL + 1, 10 ** 400])
def test_a_drain_timeout_that_cannot_be_scheduled_is_refused_at_startup(capsys, too_big):
    with pytest.raises(SystemExit) as raised:
        build_arg_parser().parse_args(["--shutdown-drain-timeout", str(too_big)])
    assert raised.value.code == 2
    message = capsys.readouterr().err
    assert "shutdown drain timeout cannot exceed" in message, message
    assert "_shutdown_drain_timeout" not in message and "_check_schedulable" not in message, message
    with pytest.raises(ValueError, match="shutdown_drain_timeout cannot exceed"):
        Server(0, shutdown_drain_timeout=too_big)


def test_the_drain_timeout_ceiling_is_reachable_and_what_it_guards_against_is_real():
    # the controls for the two refusals above: the ceiling itself is accepted, so they are the bound and not a flag that refuses everything large...
    assert build_arg_parser().parse_args(
        ["--shutdown-drain-timeout", str(MAX_SCHEDULABLE_INTERVAL)]).shutdown_drain_timeout == MAX_SCHEDULABLE_INTERVAL
    server = Server(0, shutdown_drain_timeout=MAX_SCHEDULABLE_INTERVAL)
    try:
        # ...and a value past what a float holds does not fail at startup when it is let through: it fails here, in the drain's own deadline arithmetic, which run() reaches on SIGTERM after the save
        with pytest.raises(OverflowError):
            server._drain_for(10 ** 400)
    finally:
        server._loop.close()


# docker stop's grace period, which is what the default has to finish inside
_KILL_TIMER_SECONDS = 10


def test_the_drain_timeout_default_reaches_both_doors_and_fits_inside_a_kill_timer():
    args = build_arg_parser().parse_args([])
    server = Server(0)
    try:
        # the parser's own default and the constructor's, each read from the one constant: a literal in either place would pass the checks below for the wrong reason
        assert args.shutdown_drain_timeout == DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SECONDS
        assert server.shutdown_drain_timeout == DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SECONDS
    finally:
        server._loop.close()
    # the properties the number has to have, and not the number: zero skips the drain for everyone who never typed the flag, and a bound that reaches the kill timer lets the timer land inside the drain and skip the teardown behind it
    assert DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SECONDS > 0
    assert DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SECONDS + SELECT_TIMEOUT_SECONDS < _KILL_TIMER_SECONDS
