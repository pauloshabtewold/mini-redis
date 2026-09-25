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


def free_port():
    # bound and released rather than fixed: the port is free when chosen and could in
    # principle be taken before the server binds it, but a fixed port collides with a
    # developer's own running instance, which is the more likely of the two
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until_listening(port, deadline):
    # answers "is anything listening there", which is not the same question as "did my
    # server start" -- see launch_server() below, which is what every launch site uses
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
        # launch_server reads one line from this pipe and leaves it open; closing it here
        # rather than at garbage collection keeps a ResourceWarning out of a suite that
        # starts a server per test
        if proc.stdout is not None:
            proc.stdout.close()


def _bound_this_port(proc, port, deadline):
    # server.py prints one flushed line naming the address it actually bound. That line,
    # and not a TCP connect, is what says the process listening on this port is this one:
    # free_port() binds a port and releases it, so the number is free when chosen and can
    # be taken before the server binds it, and the ephemeral range on this machine is
    # 16,384 wide -- once it wraps, two concurrent processes are handed the same number.
    # The loser then dies with EADDRINUSE while a plain connect still succeeds, because
    # the winner is listening, and its tests run against the other one's server and read
    # values they never wrote. That is the shape the 8 MiB round trip's own failure
    # message describes, and nothing here could see it
    #
    # Read through os.read on the raw descriptor rather than readline(): select() bounds
    # only the wait for readiness, so a readline() entered on one available byte blocks
    # with no deadline of its own if the rest of the line never comes, and the retry loop
    # above never gets control back. Today's server prints the whole line in one flushed
    # write well under PIPE_BUF, so it arrives atomically -- but the bound has to hold on
    # the path, not on the current server's good manners. Lines are consumed until the one
    # that matches, because stopping at the first means an unrelated line ahead of it
    # reads as a failure to bind.
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
                    # the line proves the port is this process's; a connect proves
                    # something is serving on it now. Neither alone is enough -- a connect
                    # answers for whoever holds the port, and a line a child printed before
                    # dying answers for nothing -- and no check can promise a server stays
                    # up, so this is the pair that is actually knowable here
                    return proc.poll() is None and wait_until_listening(port, deadline)
        elif proc.poll() is not None:
            # exited without ever printing the line: the bind failed
            return False
    return False


def launch_server(snapshot_path, extra_args=(), attempts=5):
    """Start `server.py` on a port this process holds, and return `(proc, port)`.

    The one launch site every test goes through, so the ownership check above is written
    once. A port another process took is retried rather than failed, because a collision
    is nobody's defect and a suite that goes red under concurrency is the same loss of
    signal as one that silently uses the wrong server. `stdout` is a pipe rather than
    `DEVNULL` for the same reason the redirection existed: the bind line stays out of the
    transcript under `-s`, and now it is also read.

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
    # the reason is not knowable from here -- a port taken, a server that died before it
    # bound, one too slow to answer inside the deadline -- so the message says what was
    # observed rather than naming a cause. An earlier wording blamed a taken port every
    # time, including when none had been
    raise AssertionError(
        "no server could be started: %d attempts in a row neither printed a bind line "
        "for the port they were given nor stayed alive to" % attempts)


@pytest.fixture
def mini_redis_server(tmp_path):
    # a path under this test's own tmp_path, not the default ./dump.mrdb: a snapshot
    # written to the repository's own working directory would outlive the test that
    # wrote it and load back into whichever test runs against this fixture next.
    # stderr is left inherited, so a traceback still surfaces
    proc, port = launch_server(tmp_path / "dump.mrdb")
    try:
        yield port
    finally:
        stop_server(proc)


@pytest.fixture
def redis_client(mini_redis_server):
    # redis-py defaults to RESP3 and sends HELLO 3 on connect, which this server cannot
    # answer -- the flat RESP2 array it gets back is not the mapping redis-py expects,
    # and every reply this server can give to that fails one way or another. protocol=2
    # belongs here, once, rather than at every place a test builds its own client
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
