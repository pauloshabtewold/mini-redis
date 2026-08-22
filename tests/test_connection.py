import socket

import pytest

import resp
from connection import Connection, Role


@pytest.fixture
def pair():
    # real sockets, not mocks: receive() branches on errno behaviour no fake reproduces faithfully
    a, b = socket.socketpair()
    a.setblocking(False)
    yield Connection(a, ("test", 0)), b
    a.close()
    b.close()


def test_receive_buffers_what_the_peer_sent(pair):
    conn, peer = pair
    peer.sendall(b"*1\r\n$4\r\nPING\r\n")
    assert conn.receive() is True
    assert bytes(conn.read_buffer) == b"*1\r\n$4\r\nPING\r\n"


def test_receive_on_empty_socket_reports_connected_without_reading(pair):
    conn, peer = pair
    # nothing sent, so recv() raises BlockingIOError: still connected, no bytes
    assert conn.receive() is True
    assert conn.read_buffer == bytearray()


def test_receive_reports_disconnect_when_peer_closes(pair):
    conn, peer = pair
    peer.close()
    assert conn.receive() is False


def test_receive_reports_disconnect_on_socket_error(pair):
    conn, _peer = pair
    conn.close()                       # recv() on a closed socket raises OSError
    assert conn.receive() is False


def test_default_role_is_client(pair):
    conn, _peer = pair
    assert conn.role is Role.CLIENT


def test_fileno_is_the_underlying_descriptor(pair):
    conn, _peer = pair
    assert conn.fileno() >= 0


def test_take_commands_drains_a_pipelined_buffer(pair):
    conn, peer = pair
    peer.sendall(b"*1\r\n$4\r\nPING\r\n" + b"PING\r\n")
    conn.receive()
    assert conn.take_commands() == [[b"PING"], [b"PING"]]
    assert conn.read_buffer == bytearray()


def test_take_commands_leaves_a_partial_command_buffered(pair):
    conn, peer = pair
    peer.sendall(b"*2\r\n$3\r\nfoo\r\n$3\r\nba")
    conn.receive()
    assert conn.take_commands() == []
    assert bytes(conn.read_buffer) == b"*2\r\n$3\r\nfoo\r\n$3\r\nba"


def test_take_commands_consumes_input_that_yields_no_command(pair):
    conn, peer = pair
    # a blank inline line and a *0 header consume bytes and produce nothing; looping on argv instead of consumed spins forever here
    peer.sendall(b"\r\n*0\r\nPING\r\n")
    conn.receive()
    assert conn.take_commands() == [[b"PING"]]
    assert conn.read_buffer == bytearray()


def test_take_commands_propagates_protocol_error(pair):
    conn, peer = pair
    peer.sendall(b"*-1\r\n")
    conn.receive()
    with pytest.raises(resp.ProtocolError):
        conn.take_commands()


def test_oversized_length_header_is_a_protocol_error_not_a_crash(pair):
    # isdigit() passes this and int() refuses it, so an unguarded parser raises ValueError here and drops every connection instead of only this one
    conn, peer = pair
    peer.sendall(b"*" + b"9" * 4301 + b"\r\n")
    conn.receive()
    with pytest.raises(resp.ProtocolError):
        conn.take_commands()


def test_queue_is_the_only_thing_that_fills_the_write_buffer(pair):
    conn, _peer = pair
    conn.queue(b"+OK\r\n")
    conn.queue(b":1\r\n")
    assert bytes(conn.write_buffer) == b"+OK\r\n:1\r\n"


def test_close_is_idempotent(pair):
    conn, _peer = pair
    conn.close()
    assert conn.closed is True
    conn.close()                       # the read path and the shutdown path both call this
    assert conn.closed is True
