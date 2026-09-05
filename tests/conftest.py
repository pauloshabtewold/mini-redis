"""Shared fixtures for the end-to-end tests: a free TCP port, a mini-redis server
subprocess, and a RESP2 redis-py client already pointed at it.
"""

import pathlib
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
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                return True
        except OSError:
            time.sleep(0.02)
    return False


@pytest.fixture
def mini_redis_server():
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, str(REPO_ROOT / "server.py"), "--port", str(port)],
        stdout=subprocess.DEVNULL,
    )
    # server.py prints its bind line to stdout on every launch, unconditionally. the
    # port here is ephemeral, so no expected test output could ever name it, and the
    # tests built on this fixture are run with -s, which would otherwise let that line
    # straight through into their transcript. redirecting removes it at the source --
    # readiness below is proved by a real TCP connect, a stronger signal than the print
    # ever was -- and stderr is left inherited, so a traceback still surfaces
    try:
        assert wait_until_listening(port, time.monotonic() + 5), "server never started listening"
        yield port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


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
