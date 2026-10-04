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
clients and asks it to stop. The rest never run a server: they build a parser or a
`Server` and check how `--shutdown-drain-timeout` is validated and what its default is.
Everything the first two kinds start keeps its snapshot inside the test's own
`tmp_path`, because every SIGTERM now saves one.
"""

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
import threading
import time

import pytest

import commands
import persistence
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
# the stall these tests build -- tens of MiB of replies owed to a client that reads none of them -- is the state the high-water mark exists to prevent. at the defaults a connection is paused after the first large reply, the rest of its requests are never dispatched, and the server owes one reply where the test counts on many. the tests that follow are about the drain and not about the water marks, so the marks and the limit are switched off for them; a test that only needs the server to owe something at the moment of the stop runs at the defaults
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


def _stalled_client(port, replies=8):
    # asks for a 4 MiB value back several times and reads none of it, so the server owes tens of MiB that nothing will take
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
    # not before the timeout either: a server that owes a client eight unread replies and is gone in a tenth of a second drained nothing
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
        # the snapshot appearing is the sign the loop has stopped and the drain is next. the process must still be there, because the client has read nothing and a server already gone took what it owed with it
        _wait_until((tmp_path / "dump.mrdb").exists, 10, "the shutdown save")
        time.sleep(0.2)
        assert proc.poll() is None, "the server exited while the client had not read a byte"
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
        # the snapshot is written after the loop stops and before the drain starts, so a request sent once it exists and a margin has passed arrives after the drain has already taken its decision about reading
        _wait_until((tmp_path / "dump.mrdb").exists, 10, "the shutdown save")
        time.sleep(0.2)
        assert proc.poll() is None, "the server exited while the client had not read a byte"
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

    def spying_drain(seconds):
        # asked of the filesystem and not of a flag: what matters is that the bytes were on disk when the drain began
        at_entry["exists"] = snapshot.exists()
        if at_entry["exists"]:
            at_entry["keys"] = set(persistence.load(str(snapshot)).live_keys())
        real_drain(seconds)

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

    def exploding_drain(seconds):
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
        def spying_drain(seconds):
            log.collecting = True
            try:
                real_drain(seconds)
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

    def spying_drain(seconds):
        try:
            real_drain(seconds)
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

    def spying_drain(seconds):
        inside.append(True)
        try:
            real_drain(seconds)
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

        def spying_drain(seconds, real_drain=real_drain, inside=inside):
            inside.append(True)
            try:
                real_drain(seconds)
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
        def spying_drain(seconds):
            log.collecting = True
            try:
                real_drain(seconds)
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
    assert message.endswith(": 0"), message
    # a loss is a WARNING and a clean ending is not, so a configuration that shows warnings and above shows the line that says something was lost and not the one that says nothing was
    for ending, records in (("timed out", timed_out), ("skipped", skipped)):
        message = records[0].getMessage()
        assert records[0].levelno == logging.WARNING, (ending, message)
        assert message.startswith("shutdown drain incomplete"), (ending, message)
        assert message.endswith(": 1"), (ending, message)


def _drain_counts(record):
    message = record.getMessage()
    closed = re.search(r"closed while owed bytes: (\d+)", message)
    still = re.search(r"still owed bytes: (\d+)", message)
    assert closed and still, message
    return int(closed.group(1)), int(still.group(1))


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
        def spying_drain(seconds):
            log.collecting = True
            try:
                real_drain(seconds)
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
