import selectors
import socket

import pytest

from connection import Connection
from server import Server


# a real send() only short-writes when the kernel's autotuned buffers happen to be full, which is machine-dependent and silently stops testing elsewhere; scripting send() makes the short write mandatory and deterministic here
class ScriptedSocket:
    def __init__(self, sock, script):
        self._sock = sock
        self._script = list(script)

    def fileno(self):
        return self._sock.fileno()

    def recv(self, bufsize):
        return self._sock.recv(bufsize)

    def close(self):
        self._sock.close()

    def send(self, data):
        outcome = self._script.pop(0)
        if outcome is BlockingIOError:
            raise BlockingIOError()
        return outcome


@pytest.fixture
def make_connection():
    server = Server(0)
    opened = []

    def make(script):
        real, peer = socket.socketpair()
        real.setblocking(False)
        opened.extend((real, peer))
        conn = Connection(ScriptedSocket(real, script), ("stub", 0))
        server._loop.register(conn)
        server._connections.add(conn)
        return server, conn

    yield make
    for sock in opened:
        sock.close()
    server._loop.close()


def test_partial_send_leaves_the_exact_remainder_buffered(make_connection):
    server, conn = make_connection([3, BlockingIOError])
    conn.queue(b"0123456789")
    server._flush(conn)
    assert conn.write_buffer == bytearray(b"3456789")


def test_write_interest_is_registered_while_the_buffer_is_non_empty(make_connection):
    server, conn = make_connection([3, BlockingIOError])
    conn.queue(b"0123456789")
    server._flush(conn)
    assert conn.write_buffer
    events = server._loop._selector.get_key(conn).events
    assert events & selectors.EVENT_WRITE


def test_the_writable_event_drains_across_repeated_calls(make_connection):
    server, conn = make_connection([3, BlockingIOError, 4, BlockingIOError, 3])
    conn.queue(b"0123456789")
    server._flush(conn)
    for _ in range(10):
        if not conn.write_buffer:
            break
        server._loop.run_once()
    assert conn.write_buffer == bytearray()


def test_write_interest_is_deregistered_the_moment_it_empties(make_connection):
    server, conn = make_connection([3, BlockingIOError, 7])
    conn.queue(b"0123456789")
    server._flush(conn)
    for _ in range(10):
        if not conn.write_buffer:
            break
        server._loop.run_once()
    assert conn.write_buffer == bytearray()
    events = server._loop._selector.get_key(conn).events
    assert not events & selectors.EVENT_WRITE


def test_a_send_that_reports_zero_leaves_the_buffer_untouched(make_connection):
    # not a documented outcome for a non-blocking socket -- the documented refusal is
    # BlockingIOError. treating it as progress would spin the loop; treating it as a disconnect
    # would close a connection on an undefined case
    _server, conn = make_connection([0, 3, 2])
    conn.queue(b"+OK\r\n")
    assert conn.flush() is True
    assert conn.write_buffer == bytearray(b"+OK\r\n"), "a zero return must not consume anything"
    assert conn.flush() is True
    assert conn.write_buffer == bytearray()
