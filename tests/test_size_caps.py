import socket

import pytest

import resp
from connection import Connection
from server import Server, build_arg_parser
from tests.test_server_lifecycle import listening, pump


def _drive(wire, **caps):
    # a real Connection over a socketpair, driven through take_commands() rather than
    # resp.parse_command(): the live server reaches parse_multibulk_header and
    # parse_bulk_element straight from connection.py, never through parse_command for a
    # multibulk, so a cap threaded only into parse_command would leave this path
    # uncapped while the natural unit test stayed green
    a, b = socket.socketpair()
    a.setblocking(False)
    conn = Connection(a, ("stub", 0))
    try:
        b.sendall(wire)
        conn.receive()
        try:
            return conn, conn.take_commands(**caps)
        except resp.ProtocolError as exc:
            return conn, exc.message
    finally:
        a.close()
        b.close()


def test_both_flags_default_to_the_documented_values():
    args = build_arg_parser().parse_args([])
    assert args.max_value_size == 67108864, args.max_value_size
    assert args.max_multibulk == 1048576, args.max_multibulk
    server = Server(0)
    try:
        assert (server.max_value_size, server.max_multibulk) == (67108864, 1048576)
    finally:
        # Server.__init__ opens a selector that only run() would otherwise dispose of
        server._loop.close()


@pytest.mark.parametrize(
    "flag", ["--max-value-size", "--max-multibulk", "--output-buffer-limit"])
def test_a_negative_limit_is_refused_at_the_cli_for_all_three_limits(flag):
    # the shared rule covers all three -- including --output-buffer-limit,
    # whose own observable behaviour at this door must not have changed
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args([flag, "-1"])


def test_a_negative_limit_is_refused_by_the_constructor_for_all_three_limits():
    # the CLI is not the only door: Server is constructed directly by tests and by
    # anything embedding it. every limit is validated before self._loop is built, so a
    # refused construction opens no selector and leaves nothing to close
    for kwargs in (
        {"max_value_size": -1}, {"max_multibulk": -1}, {"output_buffer_limit": -1}
    ):
        with pytest.raises(ValueError):
            Server(0, **kwargs)


@pytest.mark.parametrize("flag", ["--max-value-size", "--max-multibulk"])
def test_a_non_numeric_limit_is_refused_at_the_cli(flag):
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args([flag, "notanumber"])


def test_zero_is_accepted_by_both_doors_and_disables_each_cap():
    args = build_arg_parser().parse_args(
        ["--max-value-size", "0", "--max-multibulk", "0"])
    assert (args.max_value_size, args.max_multibulk) == (0, 0)
    server = Server(0, max_value_size=0, max_multibulk=0)
    try:
        assert (server.max_value_size, server.max_multibulk) == (0, 0)
    finally:
        server._loop.close()


def test_a_declared_length_exactly_at_the_cap_is_accepted_through_a_connection():
    # the comparison is `>`, never `>=` -- a length exactly at the cap is kept,
    # driven through the same Connection path a live server uses
    wire = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\nabcd\r\n"
    conn, result = _drive(wire, max_value_size=4, max_multibulk=8)
    assert result == [[b"SET", b"k", b"abcd"]], result


def test_an_over_cap_bulk_length_is_refused_before_the_body_is_buffered():
    # the check sits ahead of body_end being computed, so the
    # connection never commits to a buffer-growth hint for the oversized body --
    # _parse_needed == 0 after the refusal is what proves it, and a criterion that
    # only reads the error body passes over a check placed one line too late
    header = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$68157440\r\n"    # 65 MiB declared, no body
    conn, message = _drive(header, max_value_size=67108864, max_multibulk=1048576)
    assert message == b"ERR Protocol error: invalid bulk length", message
    assert conn._parse_needed == 0, (
        "the connection recorded a buffer-growth commitment of %d before refusing -- "
        "the cap fired after body_end was computed" % conn._parse_needed)
    assert len(conn.read_buffer) <= len(header), len(conn.read_buffer)


def test_an_over_cap_multibulk_count_is_refused_through_a_connection():
    # the live path drives parse_multibulk_header directly out of connection.py,
    # never through parse_command -- the header-side twin of the test above
    conn, message = _drive(
        b"*1000000000\r\n", max_value_size=67108864, max_multibulk=1048576)
    assert message == b"ERR Protocol error: invalid multibulk length", message
    assert conn._elements_remaining == 0, "the header was accepted before it was refused"
    assert conn._argv is None


def test_a_live_server_refuses_an_over_cap_bulk_length_and_closes_the_sender():
    with listening() as (server, connect, _listener):
        server.max_value_size = 64
        server.max_multibulk = 8
        client = connect()
        pump(server)
        client.sendall(b"*1\r\n$65\r\n")
        pump(server)
        client.settimeout(0.5)
        assert client.recv(256) == b"-ERR Protocol error: invalid bulk length\r\n"
        assert len(server._connections) == 0, "a protocol error must close the sender"


def test_a_live_server_refuses_an_over_cap_multibulk_count_and_closes_the_sender():
    with listening() as (server, connect, _listener):
        server.max_value_size = 64
        server.max_multibulk = 8
        client = connect()
        pump(server)
        client.sendall(b"*9\r\n")
        pump(server)
        client.settimeout(0.5)
        assert client.recv(256) == b"-ERR Protocol error: invalid multibulk length\r\n"
        assert len(server._connections) == 0


def test_a_command_under_both_caps_dispatches_normally_through_a_live_server():
    # the caps must not over-refuse: an ordinary command well inside both limits is
    # dispatched and answered exactly as it would be with no cap at all
    with listening() as (server, connect, _listener):
        server.max_value_size = 64
        server.max_multibulk = 8
        client = connect()
        pump(server)
        client.sendall(b"*1\r\n$4\r\nPING\r\n")
        pump(server)
        client.settimeout(2)
        assert client.recv(64) == b"+PONG\r\n"
        assert len(server._connections) == 1


def test_a_value_inside_the_inbound_cap_can_exceed_the_outbound_limit():
    # the two limits sit on opposite sides and neither bounds the other: a value well
    # under --max-value-size can still be large enough, repeated across one pipelined
    # batch, to exceed --output-buffer-limit -- which then disconnects the very client
    # that stored it rather than throttling its read
    with listening() as (server, connect, _listener):
        server.max_value_size = 1024 * 1024
        server.output_buffer_limit = 64 * 1024
        client = connect()
        pump(server)
        value = b"v" * 32768
        client.sendall(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$%d\r\n" % len(value) + value + b"\r\n")
        pump(server)
        client.settimeout(2)
        assert client.recv(100) == b"+OK\r\n", \
            "storing it must succeed: it is under the inbound cap"

        # 200 GETs in one write and the client never reads, so nothing drains between
        # them -- the same shape test_server_lifecycle.py's own limit test uses
        client.sendall(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n" * 200)
        pump(server)
        assert len(server._connections) == 0, (
            "a value under the inbound cap must still respect the outbound limit "
            "once it is read back at enough volume")
