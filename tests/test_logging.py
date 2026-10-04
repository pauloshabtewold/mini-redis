"""What the server logs, at which level, and the one place that decides.

Two kinds of test live here and they cannot share a harness. The ones about a process --
what importing `server` leaves behind, what `main()` configures, what an unknown level
does to the exit status, what an operator reading stderr sees at each level -- run a real
interpreter, because under pytest the root logger already carries handlers of the
runner's own and "importing this module configures nothing" cannot be asked of a process
that was configured before the question. The ones about a record -- that the exception
boundary still reports at ERROR, that the per-command call is not made at all at INFO --
run a `Server` in this process and read what reaches a handler attached to its logger.

Every test that reads a server's stderr builds its own `Popen` with `stderr=PIPE` and
drains it from a thread for as long as the process lives. The pipe holds 64 KiB, and a
server whose stderr nobody reads parks on a blocking write with no further line to say
so. Nothing here writes to the server's stdout either, which carries one line, the bind
line. Lines are counted whole: a log line is matched against the complete shape of the
line it should be, and never by a substring, because the drain's own line names a number
of connections, and a command name is also a part of ordinary words. Every server keeps
its snapshot under the test's own directory, because every SIGTERM saves one.
"""

import contextlib
import inspect
import json
import logging
import pathlib
import re
import select
import signal
import socket
import subprocess
import sys
import textwrap
import threading
import time
import types

import pytest

import server as server_mod
from commands import registry
from server import Server, build_arg_parser
from tests.test_server_lifecycle import listening, pump

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
# the module's own logger object, looked up the way the dispatch loop looks it up: as a global, with an attribute read on it
_server_logger = server_mod.logger

# the shapes of every line this server writes at INFO and below, whole. the prefix is what
# logging.basicConfig puts before a message by default: the level, then the logger's name,
# which is `__main__` when server.py is run as a script and `server` when it is imported, so
# it is matched as anything up to the next colon
def _at(level):
    return level + r":[^:]+:"


_PEER = r"\('127\.0\.0\.1', (\d+)\)"
_ACCEPTED = re.compile(_at("INFO") + r"accepted connection (\d+) from " + _PEER + r"; (\d+) connected")
_CLOSED = re.compile(_at("INFO") + r"closed connection (\d+) from " + _PEER + r"; (\d+) connected")
_DRAIN = re.compile(
    _at("INFO") + r"shutdown drain complete; connections closed while owed bytes: 0; "
    r"connections still owed bytes: 0")
_COMMAND = re.compile(_at("DEBUG") + r"connection (\d+): b'([^']*)' with (\d+) arguments")
_REFUSAL = re.compile(r"(\w+):[^:]+:refusing " + _PEER + r": .*--max-connections.*")
_IGNORED = re.compile(
    _at("WARNING") + r"ignoring the snapshot at .*dump\.mrdb; periodic saving is off, so the file is left in place")
_SHAPES = {
    "accepted": _ACCEPTED, "closed": _CLOSED, "drain": _DRAIN, "command": _COMMAND,
    "refusal": _REFUSAL,
}

# one pipelined write, and the replies it is owed. the name of every command in it is a word a
# substring search would find in other places
_SESSION = [(b"PING",), (b"ECHO", b"hi"), (b"SET", b"k", b"v"), (b"GET", b"k"), (b"DEL", b"k"), (b"PING",)]
_SESSION_REPLY = b"+PONG\r\n$2\r\nhi\r\n+OK\r\n$1\r\nv\r\n:1\r\n+PONG\r\n"
_COMMAND_NAMES = re.compile(r"\b(?:PING|ECHO|SET|GET|DEL)\b")


def _resp(*parts):
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts)


def _sorted_lines(lines):
    # every line is claimed by the one shape it matches whole, and a line that matches none is
    # returned as a stray, so "nothing else was written" is a statement about the stray list
    found = {name: [] for name in _SHAPES}
    stray = []
    for line in lines:
        for name, shape in _SHAPES.items():
            match = shape.fullmatch(line)
            if match:
                found[name].append(match)
                break
        else:
            stray.append(line)
    return found, stray


def _returns_within(proc, bound):
    # the bound is the join: a wait() on this thread would simply never return for a server that never exits, and there would be no measurement, only a hung suite
    outcome = {}
    waiter = threading.Thread(target=lambda: outcome.update(rc=proc.wait()), daemon=True)
    waiter.start()
    waiter.join(bound)
    return (not waiter.is_alive()), outcome.get("rc")


class _Running:
    """A real `server.py` with its stderr drained by a thread for as long as it lives."""

    def __init__(self, directory, flags):
        self.lines = []
        self.proc = subprocess.Popen(
            [sys.executable, "-B", str(REPO_ROOT / "server.py"), "--port", "0",
             "--snapshot-path", str(directory / "dump.mrdb"), "--snapshot-interval", "0", *flags],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=directory)
        self._reader = threading.Thread(target=self._read_stderr, daemon=True)
        self._reader.start()
        self.port = None

    def _read_stderr(self):
        for raw in self.proc.stderr:
            self.lines.append(raw.decode(errors="replace").rstrip("\r\n"))

    def await_bind(self):
        ready, _, _ = select.select([self.proc.stdout], [], [], 10)
        line = self.proc.stdout.readline().decode() if ready else ""
        assert line.startswith("listening on "), ("the server never said where it listened", line, list(self.lines))
        self.port = int(line.strip().rsplit(":", 1)[1])

    def wait_for(self, condition, what, seconds=10):
        end = time.monotonic() + seconds
        while not condition(list(self.lines)):
            assert time.monotonic() < end, ("timed out waiting for " + what, list(self.lines))
            time.sleep(0.005)

    def stop(self):
        """SIGTERM, a bounded wait for the exit, and every line the process wrote."""
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        returned, rc = _returns_within(self.proc, 20)
        assert returned, ("the server did not exit within 20 seconds of SIGTERM", list(self.lines))
        self._reader.join(5)
        assert not self._reader.is_alive(), "stderr never reached end of file"
        return rc, list(self.lines)

    def reap(self):
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait(timeout=10)
        self._reader.join(5)
        for pipe in (self.proc.stdout, self.proc.stderr):
            if pipe is not None:
                pipe.close()


@pytest.fixture
def start(tmp_path):
    started = []

    def launch(*flags):
        running = _Running(tmp_path, flags)
        started.append(running)
        running.await_bind()
        return running

    yield launch
    for running in started:
        running.reap()


@pytest.fixture
def connect():
    opened = []

    def open_one(port):
        client = socket.create_connection(("127.0.0.1", port), timeout=10)
        opened.append(client)
        return client

    yield open_one
    for client in opened:
        client.close()


def _read_exactly(client, wanted, seconds=10):
    end = time.monotonic() + seconds
    received = b""
    while len(received) < wanted:
        assert time.monotonic() < end, ("timed out waiting for a reply", received)
        chunk = client.recv(wanted - len(received))
        assert chunk, ("the server closed the connection", received)
        received += chunk
    return received


def _round_trip(client, request, reply):
    client.sendall(request)
    assert _read_exactly(client, len(reply)) == reply


def _until_closed(client):
    # a clean close and a reset are two spellings of one refusal. a timeout is the failure: the server kept the connection
    client.settimeout(10)
    try:
        while client.recv(4096):
            pass
    except ConnectionResetError:
        pass


def _count_of(shape):
    return lambda lines: sum(1 for line in lines if shape.fullmatch(line))


class _Records(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)


@contextlib.contextmanager
def _logging_at(level):
    # the level goes where main() puts it, on the root, and the server's own logger is left to inherit it, which is how a real process gets its level. a guard that read the logger's own level, which is unset, would read as always open there, and a test that set the level on the logger itself could not see it. the runner's root is lowered too, because it would otherwise hide every record below WARNING and make "nothing was logged" pass for the wrong reason
    root = logging.getLogger()
    own = server_mod.logger
    before = (root.level, own.level)
    root.setLevel(level)
    own.setLevel(logging.NOTSET)
    try:
        yield
    finally:
        root.setLevel(before[0])
        own.setLevel(before[1])


@contextlib.contextmanager
def _server_log(level):
    handler = _Records()
    handler.addFilter(lambda record: record.name == server_mod.logger.name)
    root = logging.getLogger()
    with _logging_at(level):
        root.addHandler(handler)
        try:
            yield handler.records
        finally:
            root.removeHandler(handler)


@contextlib.contextmanager
def _debug_calls():
    # records the calls made to the server logger's debug(), and passes each through. a call that is made and filtered by level leaves no record, so a count of records cannot tell a call that was skipped from one that was made and discarded: this can
    log = server_mod.logger
    calls = []
    real = log.debug

    def counting(*args, **kwargs):
        calls.append(args)
        return real(*args, **kwargs)

    log.debug = counting
    try:
        yield calls
    finally:
        del log.debug


def _run_probe(source, *args):
    # a fresh interpreter at the repository root, so `import server` resolves to this tree's copy, and with no bytecode written beside it
    return subprocess.run(
        [sys.executable, "-B", "-c", source, *args], cwd=REPO_ROOT, capture_output=True, text=True, timeout=60)


_IMPORT_PROBE = """
import json, logging

root = logging.getLogger()

def state():
    return {"handlers": len(root.handlers), "level": root.level}

before = state()
import server
after_import = state()
instance = server.Server(0)
try:
    after_construction = state()
finally:
    instance._loop.close()
print(json.dumps({
    "before": before, "after_import": after_import, "after_construction": after_construction,
    "own_handlers": len(server.logger.handlers), "own_level": server.logger.level,
}))
"""


def test_importing_server_configures_no_logging():
    done = _run_probe(_IMPORT_PROBE)
    assert done.returncode == 0, done.stderr
    state = json.loads(done.stdout)
    untouched = {"handlers": 0, "level": logging.WARNING}
    # the control first: a fresh interpreter starts with nothing attached and the library default level, so what follows is a comparison against a known starting point
    assert state["before"] == untouched, state
    assert state["after_import"] == untouched, "importing server configured logging for whoever imported it"
    assert state["after_construction"] == untouched, "building a Server configured logging"
    assert (state["own_handlers"], state["own_level"]) == (0, logging.NOTSET), "server attached to or levelled its own logger"
    assert done.stderr == ""


_MAIN_PROBE = """
import json, logging, sys

calls = []
real = logging.basicConfig

def counting(**kwargs):
    calls.append(kwargs)
    return real(**kwargs)

logging.basicConfig = counting
import server

server.Server.run = lambda self: self._loop.close()
server.main([
    "--port", "0", "--snapshot-path", sys.argv[1], "--snapshot-interval", "0", "--log-level", "debug",
])
root = logging.getLogger()
print(json.dumps({"calls": len(calls), "handlers": len(root.handlers), "level": root.level}))
"""


def test_main_configures_logging_once_at_the_level_it_was_given_and_before_the_server_is_built(
        tmp_path, start, connect):
    # run() is replaced by closing the selector the server was built with, so the process returns: what is under test is what main() leaves configured, and a server that serves would never hand back a root logger to look at. the level is spelled in lower case, so the configured level also shows that the spelling was normalised on its way in
    done = _run_probe(_MAIN_PROBE, str(tmp_path / "dump.mrdb"))
    assert done.returncode == 0, done.stderr
    state = json.loads(done.stdout)
    assert state["calls"] == 1, "logging was configured %d times by one start" % state["calls"]
    assert state["handlers"] == 1, "a second handler writes every line twice"
    assert state["level"] == logging.DEBUG, state

    # and it is configured before the Server is built, which is seen from outside: a server that ignores the snapshot warns about it while it is being constructed, and that warning has to obey the flag like every other line. configured later, it would reach stderr bare and at any level
    (tmp_path / "dump.mrdb").write_bytes(b"not a snapshot, and never read")
    loud = start("--ignore-snapshot")
    _rc, lines = loud.stop()
    assert len(lines) >= 1 and _IGNORED.fullmatch(lines[0]), ("the control: the warning is written, formatted", lines)
    quiet = start("--ignore-snapshot", "--log-level", "ERROR")
    client = connect(quiet.port)
    _round_trip(client, _resp(b"PING"), b"+PONG\r\n")
    client.close()
    _rc, lines = quiet.stop()
    assert lines == [], "the flag did not silence what the server writes while it starts and stops: %r" % lines


def test_log_level_is_accepted_in_any_case():
    for spelled in ("debug", "Info", "WARNING", "error", "wArNiNg"):
        args = build_arg_parser().parse_args(["--log-level", spelled])
        assert args.log_level == spelled.upper(), (spelled, args.log_level)
    # the control: a level nobody asked for is the default, and the default is the one that reports connections and nothing finer
    assert build_arg_parser().parse_args([]).log_level == "INFO"


def test_an_unknown_log_level_is_a_usage_error_at_exit_two(tmp_path):
    for level in ("LOUD", "CRITICAL", "10", "NOTSET"):
        done = subprocess.run(
            [sys.executable, "-B", str(REPO_ROOT / "server.py"), "--port", "0",
             "--snapshot-path", str(tmp_path / "dump.mrdb"), "--log-level", level],
            cwd=tmp_path, capture_output=True, text=True, timeout=20)
        assert done.returncode == 2, (level, done.returncode, done.stderr)
        assert "invalid choice: '%s'" % level in done.stderr, (level, done.stderr)
        # refused before anything started: no bind line, and no snapshot left where it would have been written
        assert done.stdout == "", (level, done.stdout)
    assert not (tmp_path / "dump.mrdb").exists()


def test_one_info_line_is_written_for_each_connection_opened_and_one_for_each_closed(start, connect):
    server = start()
    clients = []
    for _ in range(3):
        client = connect(server.port)
        clients.append(client)
        # a reply proves the accept callback has run, so the connections are accepted one at a time and the count each line carries is the count at that moment
        _round_trip(client, _resp(b"PING"), b"+PONG\r\n")
    server.wait_for(lambda lines: _count_of(_ACCEPTED)(lines) == 3, "three accepted lines")

    ports = [client.getsockname()[1] for client in clients]
    found, _ = _sorted_lines(list(server.lines))
    opened = [(int(m.group(1)), int(m.group(2)), int(m.group(3))) for m in found["accepted"]]
    assert [(port, live) for _id, port, live in opened] == list(zip(ports, (1, 2, 3))), opened
    ids = [conn_id for conn_id, _port, _live in opened]
    assert len(set(ids)) == 3, "two connections shared an id"
    id_of = dict(zip(ports, ids))

    # closed one at a time, each waited for, so the live count a closed line carries is the count at its own close
    expected_closes = []
    for index, live in ((1, 2), (0, 1), (2, 0)):
        clients[index].close()
        expected_closes.append((id_of[ports[index]], ports[index], live))
        server.wait_for(
            lambda lines, wanted=len(expected_closes): _count_of(_CLOSED)(lines) == wanted,
            "closed line number %d" % len(expected_closes))
        found, _ = _sorted_lines(list(server.lines))
        closed = [(int(m.group(1)), int(m.group(2)), int(m.group(3))) for m in found["closed"]]
        assert closed == expected_closes, closed

    # a connection still open when the server is told to stop is closed by the shutdown, and that close is logged too, once: the shutdown does not take a path around the line
    last = connect(server.port)
    _round_trip(last, _resp(b"PING"), b"+PONG\r\n")
    server.wait_for(lambda lines: _count_of(_ACCEPTED)(lines) == 4, "the fourth accepted line")
    rc, lines = server.stop()
    assert rc == 0, rc

    found, stray = _sorted_lines(lines)
    assert stray == [], "a line other than a connection or the drain at the default level: %r" % stray
    assert len(found["accepted"]) == 4 and len(found["closed"]) == 4
    accepted_ids = sorted(int(m.group(1)) for m in found["accepted"])
    closed_ids = sorted(int(m.group(1)) for m in found["closed"])
    assert closed_ids == accepted_ids, "a connection was not closed exactly once in the log"
    final = found["closed"][-1]
    assert (int(final.group(2)), int(final.group(3))) == (last.getsockname()[1], 0), final.group(0)
    # the drain's one line names connections and no peer, which is why it is its own shape and not a closed line
    assert len(found["drain"]) == 1


def test_nothing_names_a_command_at_info(start, connect):
    # from outside, at the default level: a real process, a pipelined session, and a stderr in which every line is one of the three INFO shapes and none names a command
    server = start()
    client = connect(server.port)
    _round_trip(client, b"".join(_resp(*argv) for argv in _SESSION), _SESSION_REPLY)
    client.close()
    server.wait_for(lambda lines: _count_of(_CLOSED)(lines) == 1, "the closed line")
    rc, lines = server.stop()
    assert rc == 0, rc
    found, stray = _sorted_lines(lines)
    assert stray == [], "stderr at the default level held a line that is not a connection or the drain: %r" % stray
    assert [len(found[name]) for name in ("accepted", "closed", "drain", "command")] == [1, 1, 1, 0]
    named = [line for line in lines if _COMMAND_NAMES.search(line) or "arguments" in line]
    assert named == [], named

    # from inside: the lines above cannot show that the debug call was skipped, because the logger drops a debug record below its level whether or not the call is guarded, and the output is the same either way. what differs is that an unguarded call builds its arguments for every dispatched command and then throws them away, and that shows as a call to debug() that nothing logs
    expected_calls = len(_SESSION)
    for level, calls_wanted in ((logging.INFO, 0), (logging.DEBUG, expected_calls)):
        with _server_log(level) as records, _debug_calls() as calls, listening() as (loop_server, connect_local, _listener):
            local = connect_local()
            pump(loop_server)
            local.sendall(b"".join(_resp(*argv) for argv in _SESSION))
            pump(loop_server)
            local.settimeout(5)
            assert _read_exactly(local, len(_SESSION_REPLY)) == _SESSION_REPLY, "the session was not dispatched"
        assert len(calls) == calls_wanted, (
            "debug() was called %d times for %d dispatched commands at %s"
            % (len(calls), expected_calls, logging.getLevelName(level)))
        if level == logging.INFO:
            assert [r.getMessage() for r in records if _COMMAND_NAMES.search(r.getMessage())] == []
            assert all(record.levelno >= logging.INFO for record in records), records


def test_debug_writes_one_line_per_dispatched_command_and_never_an_argument(start, connect):
    server = start("--log-level", "DEBUG")
    first, second = connect(server.port), connect(server.port)
    _round_trip(first, _resp(b"PING"), b"+PONG\r\n")
    _round_trip(second, _resp(b"PING"), b"+PONG\r\n")
    server.wait_for(lambda lines: _count_of(_ACCEPTED)(lines) == 2, "both accepted lines")
    ports = [first.getsockname()[1], second.getsockname()[1]]
    found, _ = _sorted_lines(list(server.lines))
    id_of = {int(m.group(2)): int(m.group(1)) for m in found["accepted"]}
    first_id, second_id = id_of[ports[0]], id_of[ports[1]]

    # a pipeline of three in one write, then singles, then a command name longer than the name is allowed to run to in a log line
    pipeline = b"".join(
        [_resp(b"PING"), _resp(b"SET", b"secretkey", b"secretvalue"), _resp(b"GET", b"secretkey")])
    _round_trip(first, pipeline, b"+PONG\r\n+OK\r\n$11\r\nsecretvalue\r\n")
    _round_trip(first, _resp(b"ECHO", b"payload"), b"$7\r\npayload\r\n")
    long_name = b"X" * 100
    unknown = b"-ERR unknown command '" + long_name + b"'\r\n"
    _round_trip(second, _resp(long_name), unknown)
    first.close()
    second.close()
    server.wait_for(lambda lines: _count_of(_CLOSED)(lines) == 2, "both closed lines")
    rc, lines = server.stop()
    assert rc == 0, rc

    found, stray = _sorted_lines(lines)
    assert stray == [], stray
    logged = [(int(m.group(1)), m.group(2), int(m.group(3))) for m in found["command"]]
    # one line for each dispatched command, in the order they were dispatched, naming the command and counting what followed it
    assert logged == [
        (first_id, "PING", 0), (second_id, "PING", 0),
        (first_id, "PING", 0), (first_id, "SET", 2), (first_id, "GET", 1),
        (first_id, "ECHO", 1),
        (second_id, "X" * 32, 0),
    ], logged
    text = "\n".join(lines)
    for argument in ("secretkey", "secretvalue", "payload"):
        assert argument not in text, "an argument reached the log: " + argument
    assert "X" * 33 not in text, "a command name was logged past its cut"
    # the connection lines are still there at this level: DEBUG adds to INFO and does not replace it
    assert len(found["accepted"]) == 2 and len(found["closed"]) == 2 and len(found["drain"]) == 1


class _Boom(Exception):
    pass


@contextlib.contextmanager
def _echo_raises(exc):
    # nothing in the shipped registry can raise, so the boundary is entered by swapping one handler for one that does and putting the original back on the way out
    original = registry.COMMANDS[b"ECHO"]

    def explode(store, conn, argv):
        raise exc

    registry.COMMANDS[b"ECHO"] = original._replace(handler=explode)
    try:
        yield
    finally:
        registry.COMMANDS[b"ECHO"] = original


def test_the_exception_boundary_still_reports_at_error_with_the_traceback():
    with _server_log(logging.DEBUG) as records, _echo_raises(_Boom("deliberate")), listening() as (server, connect, _listener):
        victim = connect()
        pump(server)
        assert len(server._connections) == 1
        records.clear()

        victim.sendall(_resp(b"ECHO", b"hi"))
        pump(server)
        assert len(server._connections) == 0, "the connection that raised was kept"
        peer = victim.getsockname()

    boundary = [record for record in records if record.exc_info]
    assert len(boundary) == 1, [record.getMessage() for record in records]
    assert boundary[0].levelno == logging.ERROR, logging.getLevelName(boundary[0].levelno)
    assert boundary[0].exc_info[0] is _Boom
    assert str(peer) in boundary[0].getMessage(), boundary[0].getMessage()
    # every level is visible here, so a report lowered beneath ERROR would still be in the list, and the boundary is the only thing in it at WARNING or above
    assert [record for record in records if record.levelno >= logging.WARNING] == boundary
    # the ordinary close line follows the report: abandoning a connection closes it the way every close does
    closes = [record for record in records if _CLOSED.fullmatch("INFO:server:" + record.getMessage())]
    assert len(closes) == 1 and closes[0].levelno == logging.INFO, [r.getMessage() for r in records]
    assert records.index(closes[0]) > records.index(boundary[0])


def _refuse(connect, port, refusals):
    # one client holds the only slot and the others are refused one at a time, each waited for, so the lines come out in the order the refusals were made and each names the client that caused it
    held = connect(port)
    _round_trip(held, _resp(b"PING"), b"+PONG\r\n")
    ports = []
    for _ in range(refusals):
        refused = connect(port)
        ports.append(refused.getsockname()[1])
        _until_closed(refused)
    return ports


def test_the_cap_refusal_warns_once_and_is_debug_after(start, connect):
    refusals = 5
    # at DEBUG every refusal is visible, and the level each line was written at is on the line
    verbose = start("--max-connections", "1", "--log-level", "DEBUG")
    ports = _refuse(connect, verbose.port, refusals)
    _rc, lines = verbose.stop()
    found, _ = _sorted_lines(lines)
    levels = [m.group(1) for m in found["refusal"]]
    assert levels == ["WARNING"] + ["DEBUG"] * (refusals - 1), levels
    assert [int(m.group(2)) for m in found["refusal"]] == ports, "a refusal line named someone other than the client refused"
    assert "1 connections is the --max-connections limit" in found["refusal"][0].group(0)

    # at the default, which is what an operator sees, a run of refusals is one line however long it goes on
    quiet = start("--max-connections", "1")
    _refuse(connect, quiet.port, refusals)
    _rc, lines = quiet.stop()
    found, _ = _sorted_lines(lines)
    assert [m.group(1) for m in found["refusal"]] == ["WARNING"], [m.group(0) for m in found["refusal"]]
    assert not [line for line in lines if line.startswith("DEBUG:")], "debug lines at the default level"


# the guarded call in the dispatcher's loop as server.py spells it: the level test and the one line it guards
_PER_COMMAND_CALL = re.compile(
    r"^[ \t]*if logger\.isEnabledFor\(logging\.DEBUG\):\n[ \t]*logger\.debug\([^\n]*\)\n", re.MULTILINE)


def _dispatch_batch_without_the_per_command_call():
    # the dispatcher's own source with that call cut out and nothing else changed, rebuilt as a function over server.py's own namespace. cutting and not rewriting is the point: a baseline written out by hand, like a copy of the guard, goes on passing whatever the dispatcher does, and this one is the dispatcher. nothing is run to build it: compile() turns the cut source into a code object, the function's own code is picked out of that, and it is bound to the module's globals
    source = textwrap.dedent(inspect.getsource(Server._dispatch_batch))
    cut, cuts = _PER_COMMAND_CALL.subn("", source)
    if cuts != 1:
        # a skip and not a failure: the dispatcher can be written another way -- the level asked once ahead of the loop and the flag tested inside it -- and behave identically, and this test measures what the call costs, not whether it is guarded, which test_nothing_names_a_command_at_info counts on a real server
        pytest.skip(
            "the per-command call is not spelt the way this measurement cuts it out of "
            "_dispatch_batch (%d matches, not 1), so there is nothing to subtract" % cuts)
    module_code = compile(cut, "<Server._dispatch_batch without the per-command call>", "exec")
    function_code = next(
        code for code in module_code.co_consts
        if isinstance(code, types.CodeType) and code.co_name == "_dispatch_batch")
    return types.FunctionType(function_code, vars(server_mod))


def test_the_guarded_debug_call_at_info_is_measured_and_printed():
    # printed and not asserted against a threshold: a bound on a timing is a flake. what is timed is Server._dispatch_batch itself at the default level, against the same function with the per-command call cut out of its source, so the figure is what that line costs where the dispatcher reaches it and not what a copy of it costs. at INFO the guard evaluates false and the call in its body is never reached. each function's time is the minimum of the alternating batches, and the difference of two minimums moves by several nanoseconds from one run to the next, so one run's figure is a point in a range and is quoted as a range across runs. PING is the cheapest command, so the share of a dispatch that the guard takes is the largest it can be
    batch_size = 1000
    repeats = 60
    batch = [[b"PING"]] * batch_size
    reply = b"+PONG\r\n" * batch_size
    with_the_call = Server._dispatch_batch
    without_the_call = _dispatch_batch_without_the_per_command_call()

    with _logging_at(logging.INFO), listening() as (server, connect, _listener):
        # the precondition that makes the number mean what it says: false at INFO, and true one level down, so the same expression is not always false
        assert not _server_logger.isEnabledFor(logging.DEBUG)
        client = connect()
        pump(server)
        conn, = server._connections

        def timed(dispatch):
            started = time.perf_counter_ns()
            dispatch(server, conn, batch)
            spent = time.perf_counter_ns() - started
            # read outside the timed region, so a batch never meets a kernel buffer the last one filled. it is also what shows the copy with the call cut out still dispatches correctly: both functions answer the batch identically
            assert _read_exactly(client, len(reply)) == reply, "the batch was not answered"
            return spent

        # alternated, so a slow stretch of the machine falls on both and not on one
        spent_with, spent_without = [], []
        for _ in range(repeats):
            spent_with.append(timed(with_the_call))
            spent_without.append(timed(without_the_call))
    with _logging_at(logging.DEBUG):
        assert _server_logger.isEnabledFor(logging.DEBUG)
    per_command = round((min(spent_with) - min(spent_without)) / batch_size)
    print(
        "\nthe per-command call at INFO: %d ns per dispatched command, of %d ns for the dispatch path with it"
        " (minimum of %d alternating batches of %d PING each, %s %d.%d.%d)"
        % (per_command, round(min(spent_with) / batch_size), repeats, batch_size,
           sys.implementation.name, *sys.version_info[:3]))

