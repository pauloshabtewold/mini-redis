import contextlib
import socket

import pytest

from server import Server


@contextlib.contextmanager
def listening():
    # a real listener on an ephemeral port, driven a step at a time: exercises the accept, read and close wiring without a subprocess or a signal
    server = Server(0)
    listener = server._open_listener()
    server._loop.register_listener(listener)
    server._loop._timeout = 0.02
    port = listener.getsockname()[1]
    clients = []

    def connect():
        # every client the test opens is tracked here, so a failing assertion mid-test cannot leak the descriptor
        client = socket.create_connection(("127.0.0.1", port))
        clients.append(client)
        return client

    try:
        yield server, connect, listener
    finally:
        for client in clients:
            client.close()
        for conn in list(server._connections):
            server._close(conn)
        listener.close()
        server._loop.close()


def pump(server, times=4):
    for _ in range(times):
        server._loop.run_once()


@pytest.fixture
def server_and_client():
    with listening() as (server, connect, _listener):
        client = connect()
        pump(server)
        yield server, client


def test_accept_tracks_one_connection(server_and_client):
    server, _client = server_and_client
    assert len(server._connections) == 1


def test_parsed_command_gets_no_reply_and_keeps_the_connection(server_and_client):
    server, client = server_and_client
    client.sendall(b"*1\r\n$4\r\nPING\r\n")
    pump(server)
    assert len(server._connections) == 1
    conn = next(iter(server._connections))
    # the dispatcher and the write drain arrive together; nothing is queued before they exist
    assert conn.write_buffer == bytearray()
    assert conn.read_buffer == bytearray()


def test_bulk_body_containing_crlf_survives_a_real_socket(server_and_client):
    server, client = server_and_client
    body = b"*2\r\n$3\r\nfoo\r\n"
    client.sendall(b"*1\r\n$%d\r\n" % len(body) + body + b"\r\n")
    pump(server)
    conn = next(iter(server._connections))
    assert conn.read_buffer == bytearray()


def test_command_split_across_writes_is_reassembled(server_and_client):
    server, client = server_and_client
    for byte in b"*1\r\n$4\r\nECHO\r\n":
        client.sendall(bytes([byte]))
        pump(server, times=1)
    conn = next(iter(server._connections))
    assert conn.read_buffer == bytearray()
    assert len(server._connections) == 1


def test_protocol_error_closes_only_the_sending_connection():
    with listening() as (server, connect, _listener):
        good = connect()
        bad = connect()
        pump(server)
        assert len(server._connections) == 2

        bad.sendall(b"*-1\r\n")
        pump(server)

        assert len(server._connections) == 1
        good.sendall(b"*1\r\n$4\r\nPING\r\n")
        pump(server)
        assert len(server._connections) == 1


def test_oversized_length_header_does_not_take_the_server_down():
    # a length header of ~4.3 KB is refused by the interpreter, not the parser, so an unguarded server dies on it and takes every connection down
    with listening() as (server, connect, _listener):
        bystander = connect()
        attacker = connect()
        pump(server)

        attacker.sendall(b"*" + b"9" * 4301 + b"\r\n")
        pump(server)

        assert len(server._connections) == 1, "only the sender should be dropped"
        bystander.sendall(b"*1\r\n$4\r\nPING\r\n")
        pump(server)
        assert len(server._connections) == 1, "the bystander must survive"


def test_client_disconnect_is_reaped():
    with listening() as (server, connect, _listener):
        client = connect()
        pump(server)
        assert len(server._connections) == 1
        client.close()
        pump(server)
        assert server._connections == set()


def test_write_interest_is_unreachable_and_says_so():
    with listening() as (server, connect, _listener):
        connect()
        pump(server)
        conn = next(iter(server._connections))
        with pytest.raises(RuntimeError, match="no drain to service it"):
            server._on_writable(conn)


def test_accept_with_nothing_pending_adds_no_connection():
    # the listener is non-blocking, so accept() with an empty backlog raises BlockingIOError, which is the OSError branch: a real readiness-then-gone race, no mock needed
    with listening() as (server, _connect, listener):
        server._on_accept(listener)
        assert server._connections == set()
