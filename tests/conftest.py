"""Shared fixtures for the end-to-end tests: a free TCP port, a mini-redis server
subprocess, and a RESP2 redis-py client already pointed at it.
"""

import os
import pathlib
import select
import socket
import subprocess
import sys
import time

import pytest
import redis

import commands
from connection import Connection
from store import Store

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def pytest_configure(config):
    # pyproject.toml's `timeout = 300` is pytest-timeout's setting and not pytest's: without the plugin the
    # bound vanishes behind one PytestConfigWarning, and under -W error it becomes an INTERNALERROR instead
    if not config.pluginmanager.hasplugin("timeout"):
        # installed-and-disabled is named apart from absent: `-p no:timeout` gets this arm with the plugin
        # present, and the remedy that message used to print -- install the dev extra -- was already done
        try:
            import pytest_timeout  # noqa: F401
        except ImportError:
            raise pytest.UsageError(
                "pytest-timeout is not installed, so pyproject.toml's `timeout = 300` does "
                "nothing and a test that hangs will hang this run instead of failing it. "
                "Install the dev extra: pip install -e '.[dev]'") from None
        raise pytest.UsageError(
            "pytest-timeout is installed but switched off for this run, so pyproject.toml's "
            "`timeout = 300` does nothing and a test that hangs will hang this run instead of "
            "failing it. Drop the -p no:timeout that disabled it")
    # the EFFECTIVE bound: the ini key is a string, so "0" is truthy while the plugin arms nothing, and
    # --timeout=0 and PYTEST_TIMEOUT=0 never touch it -- the option is asked first because it is what wins
    bound = config.getoption("timeout", None)
    if bound is None:
        bound = config.getini("timeout")
    try:
        seconds = float(bound)
    except (TypeError, ValueError):
        raise pytest.UsageError(
            "pytest-timeout's bound is %r, which is not a number of seconds, so nothing bounds "
            "a hung test" % (bound,)) from None
    if seconds <= 0:
        raise pytest.UsageError(
            "pytest-timeout is installed but its bound is %s, and the plugin arms nothing at or "
            "below zero, so a test that hangs will hang this run instead of failing it. "
            "pyproject.toml sets `timeout = 300`; a zero here came from -o timeout, --timeout or "
            "PYTEST_TIMEOUT" % (bound,))


def free_port():
    # bound and released rather than fixed: free when chosen and takeable before the server binds it, but
    # a fixed port collides with a developer's own running instance, which is the likelier of the two
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until_listening(port, deadline):
    # answers "is anything listening there", which is not the same question as "did my
    # server start" -- see launch_server() below, which answers the second
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                return True
        except OSError:
            time.sleep(0.02)
    return False


def stop_server(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
    finally:
        # launch_server reads one line from this pipe and leaves it open: closing it here rather than at
        # garbage collection keeps a ResourceWarning out of a suite that starts a server per test
        if proc.stdout is not None:
            proc.stdout.close()


def _bound_this_port(proc, port, deadline):
    # the child's own bind line, not a TCP connect, proves the process on this port is the one we started:
    # free_port's number can be retaken, and the line is read with os.read under a deadline, never readline()
    expected = b"listening on "
    wanted_tail = (":%d" % port).encode()
    buffered = b""
    while time.monotonic() < deadline:
        ready, _, _ = select.select([proc.stdout], [], [], 0.05)
        if ready:
            chunk = os.read(proc.stdout.fileno(), 4096)
            if not chunk:
                # EOF: the child closed stdout without ever naming this port
                return False
            buffered += chunk
            while b"\n" in buffered:
                line, buffered = buffered.split(b"\n", 1)
                if line.startswith(expected) and line.rstrip().endswith(wanted_tail):
                    # the line proves the port is ours and the connect proves something serves it now; the
                    # poll after it is deliberately unpinned: a child racing to exit is a scheduling outcome
                    return wait_until_listening(port, deadline) and proc.poll() is None
        elif proc.poll() is not None:
            # exited without ever printing the line: the bind failed
            return False
    return False


def launch_server(snapshot_path, extra_args=(), attempts=5):
    """Start `server.py` on a port this process holds, and return `(proc, port)`.

    The launch site for every test that needs a real `server.py` on a port picked ahead
    of time, so the ownership check above is written once. A port another process took
    is retried rather than failed, because a collision is nobody's defect and a suite
    that goes red under concurrency is the same loss of signal as one that silently uses
    the wrong server. `stdout` is a pipe rather than `DEVNULL` for the same reason the
    redirection existed: the bind line stays out of the transcript under `-s`, and now
    it is also read.

    A module that starts its own asks the kernel for port 0 and reads the bind line
    itself, so there is no port to race over, and there are four of them:
    `test_graceful_shutdown.py`, because its tests assert how long a stop takes and what
    status the process exits with and `stop_server()` ignores the status;
    `test_connection_cap.py`, for its one test of the flag through `main()`;
    `test_crash_consistency.py`, because the shared teardown's `SIGTERM` writes a snapshot
    and a crash test has to end its server with `SIGKILL`; and `test_logging.py`, whose
    `_Running` helper drains the child's stderr on a thread. A list of them is a list that
    goes stale the next time a module starts its own, so it is the count that is stated
    first: four, and `grep -l 'Popen(' tests/*.py | grep -v /conftest.py` is what checks
    it. The call's opening parenthesis is in the pattern because a bare `Popen` also
    matches the comment in `test_redis_cli_smoke.py` that says it uses `launch_server`
    rather than one of its own, and `conftest.py` is filtered out because it is the
    launcher the four stand apart from: without the filter the same search prints five.

    Everything here is bounded: five attempts, each with its own five-second deadline that
    holds on every path through the read, so a server that says nothing, says half a line,
    or says the wrong thing costs one attempt rather than the run.
    """
    for _ in range(attempts):
        port = free_port()
        proc = subprocess.Popen(
            [sys.executable, str(REPO_ROOT / "server.py"), "--port", str(port),
             "--snapshot-path", str(snapshot_path), *extra_args],
            stdout=subprocess.PIPE,
        )
        if _bound_this_port(proc, port, time.monotonic() + 5):
            return proc, port
        stop_server(proc)
    # the reason is not knowable here -- a port taken, a server dead before it bound, one too slow -- so
    # the message says what was observed; an earlier wording blamed a taken port even when none had been
    raise AssertionError(
        "no server could be started: %d attempts in a row neither printed a bind line "
        "for the port they were given nor stayed alive to" % attempts)


@pytest.fixture
def mini_redis_server(tmp_path):
    # a path under this test's own tmp_path, not ./dump.mrdb: a snapshot in the repository would outlive
    # the test and load back into whichever test runs next. stderr is left inherited, so tracebacks surface
    proc, port = launch_server(tmp_path / "dump.mrdb")
    try:
        yield port
    finally:
        stop_server(proc)


@pytest.fixture
def redis_client(mini_redis_server):
    # redis-py defaults to RESP3 and sends HELLO 3, which this server cannot answer: protocol=2 belongs
    # here once rather than at every place a test builds its own client
    return redis.Redis(host="127.0.0.1", port=mini_redis_server, protocol=2)


# --- unit-test helpers below: shared by tests that call commands.dispatch() directly
# against a Store, unlike the end-to-end fixtures above, which drive a real subprocess

@pytest.fixture
def conn():
    a, b = socket.socketpair()
    a.setblocking(False)
    connection = Connection(a, ("127.0.0.1", 0))
    yield connection
    a.close()
    b.close()


FROZEN = 1_700_000_000_000


class FrozenStore(Store):
    def now_ms(self):
        return FROZEN


def r(store, *argv):
    return commands.dispatch(store, None, list(argv))
