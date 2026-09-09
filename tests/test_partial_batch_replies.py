import socket
import time

import pytest

import resp
from connection import BatchProtocolError, Connection
from tests.test_server_lifecycle import listening, pump

TWO_PINGS = b"*1\r\n$4\r\nPING\r\n" * 2
TWO_PONGS = b"+PONG\r\n" * 2


def _stream(tail, limits, prefix=TWO_PINGS, seconds=10):
    # read to the close, not to a byte count: a count has to be told in advance how
    # many replies to expect, which is exactly the thing that goes wrong when replies
    # are dropped -- a server that answers but never closes fails on the read here
    # rather than passing a comparison it was never asked to make
    with listening() as (server, connect, _listener):
        for name, value in limits.items():
            setattr(server, name, value)
        client = connect()
        pump(server)
        client.sendall(prefix + tail)
        client.setblocking(False)
        got, deadline = bytearray(), time.monotonic() + seconds
        while time.monotonic() < deadline:
            pump(server, times=1)
            try:
                chunk = client.recv(1 << 16)
            except BlockingIOError:
                continue
            if not chunk:
                return bytes(got), len(server._connections)
            got += chunk
        raise AssertionError(
            "the server never closed; %d bytes read: %r" % (len(got), bytes(got)))


@pytest.mark.parametrize("tail, limits, error", [
    pytest.param(
        b'ECHO "unterminated\r\n', {},
        b"-ERR Protocol error: unbalanced quotes in request\r\n",
        id="unbalanced quote"),
    pytest.param(
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$11\r\n", {"max_value_size": 10},
        b"-ERR Protocol error: invalid bulk length\r\n",
        id="over-cap bulk length"),
    pytest.param(
        b"*11\r\n", {"max_multibulk": 10},
        b"-ERR Protocol error: invalid multibulk length\r\n",
        id="over-cap multibulk count"),
])
def test_commands_ahead_of_a_protocol_error_are_answered_before_it(tail, limits, error):
    # two pipelined PINGs precede each malformed tail in one sendall(), so this
    # is the same read batch as the failure. The caps are set small -- 10, not their
    # defaults -- because what is under test is the ordering of the replies, not the
    # size that triggers the refusal
    body, tracked = _stream(tail, limits)
    assert body == TWO_PONGS + error, body
    assert tracked == 0, tracked


def test_a_batch_that_fails_on_its_first_bytes_answers_only_the_error():
    # the near miss: nothing parsed ahead of the failure means nothing is owed, so a
    # server that simply prefixed every error with whatever it had already answered
    # would be caught here rather than by the parametrised cases above
    body, tracked = _stream(b"*abc\r\n", {}, prefix=b"")
    assert body == b"-ERR Protocol error: invalid multibulk length\r\n", body
    assert tracked == 0


def test_take_commands_carries_the_commands_parsed_before_the_failure():
    a, b = socket.socketpair()
    a.setblocking(False)
    conn = Connection(a, ("stub", 0))
    try:
        b.sendall(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*abc\r\n")
        conn.receive()
        with pytest.raises(BatchProtocolError) as exc_info:
            conn.take_commands()
        exc = exc_info.value
        assert exc.commands == [[b"PING"], [b"PING"]], exc.commands
        assert exc.message == b"ERR Protocol error: invalid multibulk length", exc.message
        # the base-class catchers (tests/test_connection.py among them) must keep
        # catching this the way they always caught resp.ProtocolError
        assert isinstance(exc, resp.ProtocolError)
    finally:
        a.close()
        b.close()

    # a second connection whose first bytes are malformed: nothing was parsed ahead of
    # the failure, so the list is empty rather than the attribute being missing
    c, d = socket.socketpair()
    c.setblocking(False)
    second = Connection(c, ("stub", 1))
    try:
        d.sendall(b"*abc\r\n")
        second.receive()
        with pytest.raises(BatchProtocolError) as exc_info:
            second.take_commands()
        assert exc_info.value.commands == [], exc_info.value.commands
    finally:
        c.close()
        d.close()
