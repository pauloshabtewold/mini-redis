import contextlib
import pathlib
import selectors
import socket
import subprocess
import sys

import pytest

from server import Server
from tests.int_ceiling import NO_CEILING_REASON, NO_CONVERSION_CEILING, OVERSIZED_DIGIT_RUN

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


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


def test_parsed_command_gets_its_reply_on_the_same_connection(server_and_client):
    server, client = server_and_client
    client.sendall(b"*1\r\n$4\r\nPING\r\n")
    pump(server)
    client.settimeout(2)
    assert client.recv(64) == b"+PONG\r\n"


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


@pytest.mark.skipif(NO_CONVERSION_CEILING, reason=NO_CEILING_REASON)
def test_oversized_length_header_does_not_take_the_server_down():
    # a length header past what the interpreter converts is refused by the interpreter, not the parser, so an unguarded server dies on it and takes every connection down
    with listening() as (server, connect, _listener):
        bystander = connect()
        attacker = connect()
        pump(server)

        # chunked, with the loop pumped between: the header is as long as the interpreter's
        # ceiling, which can exceed what a socket will buffer while nothing is reading it
        header = b"*" + OVERSIZED_DIGIT_RUN + b"\r\n"
        for start in range(0, len(header), 32768):
            attacker.sendall(header[start:start + 32768])
            pump(server, times=1)
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


def test_write_interest_is_serviced_by_the_drain():
    with listening() as (server, connect, _listener):
        connect()
        pump(server)
        conn = next(iter(server._connections))
        conn.queue(b"+OK\r\n")
        server._loop.set_write_interest(conn, True)
        server._on_writable(conn)
        assert conn.write_buffer == bytearray()
        assert server._loop._selector.get_key(conn).events == selectors.EVENT_READ


# delivers a real SIGTERM from inside _open_listener(), the one point where the flag and the handlers are both live but the loop has not started, so a stop recorded there is only honoured if run() checks the flag rather than assuming it starts true
_STOP_DURING_STARTUP = """
import os, signal, sys
sys.path.insert(0, %r)
from server import Server


class SignalledDuringSetup(Server):
    def _open_listener(self):
        listener = super()._open_listener()
        os.kill(os.getpid(), signal.SIGTERM)
        return listener


SignalledDuringSetup(0).run()
print("returned")
"""


def test_a_stop_signal_delivered_during_startup_is_not_lost():
    # a subprocess with a deadline, for two reasons: the failure is a run loop that never
    # returns, which in-process would hang the suite rather than fail it -- and run() installs
    # SIGINT and SIGTERM handlers it never restores, which would outlive this test
    probe = subprocess.run(
        [sys.executable, "-c", _STOP_DURING_STARTUP % str(REPO_ROOT)],
        capture_output=True, text=True, timeout=15,
    )
    assert probe.stdout.strip() == "returned", (probe.returncode, probe.stdout, probe.stderr)


def test_accept_with_nothing_pending_adds_no_connection():
    # the listener is non-blocking, so accept() with an empty backlog raises BlockingIOError, which is the OSError branch: a real readiness-then-gone race, no mock needed
    with listening() as (server, _connect, listener):
        server._on_accept(listener)
        assert server._connections == set()
