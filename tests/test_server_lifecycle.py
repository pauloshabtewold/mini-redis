import contextlib
import pathlib
import selectors
import signal
import socket
import subprocess
import sys

import pytest

from connection import Connection
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


def test_negative_multibulk_count_is_refused_before_the_command_after_it(server_and_client):
    # the error arrives and the connection goes, so the PING behind it is never dispatched. real
    # Redis consumes the header and answers that PING instead -- refusing it is deliberate
    server, client = server_and_client
    client.sendall(b"*-1\r\n*1\r\n$4\r\nPING\r\n")
    pump(server)
    client.settimeout(2)
    assert client.recv(64) == b"-ERR Protocol error: invalid multibulk length\r\n"
    assert client.recv(64) == b""
    assert len(server._connections) == 0


def test_protocol_error_closes_only_the_sending_connection():
    with listening() as (server, connect, _listener):
        good = connect()
        bad = connect()
        pump(server)
        assert len(server._connections) == 2

        bad.sendall(b"*abc\r\n")
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


def test_protocol_error_survives_a_peer_that_cannot_be_written_to():
    # the two conditions have to land in the same pass -- the parse fails AND the error
    # reply cannot be sent -- which is what makes _flush close the connection before the
    # caller does. A socketpair makes that deterministic: data already queued stays
    # readable after the peer closes, while the next send() raises EPIPE. Over TCP the
    # same pairing comes from a peer that resets after sending a malformed command.
    server = Server(0)
    doomed_sock, gone_peer = socket.socketpair()
    gone_peer.sendall(b"*1\r\n+PING\r\n")
    gone_peer.close()
    doomed_sock.setblocking(False)
    doomed = Connection(doomed_sock, ("stub", 0))
    server._loop.register(doomed)
    server._connections.add(doomed)

    bystander_sock, bystander_peer = socket.socketpair()
    bystander_sock.setblocking(False)
    bystander = Connection(bystander_sock, ("stub", 1))
    server._loop.register(bystander)
    server._connections.add(bystander)

    try:
        # the assertion is that this returns at all: the failure mode is a ValueError
        # raised by _guard's own recovery, which reaches the top of the only thread there is
        server._on_readable(doomed)
        assert server._connections == {bystander}, "only the sender should be dropped"

        bystander_peer.sendall(b"*1\r\n$4\r\nPING\r\n")
        server._on_readable(bystander)
        bystander_peer.settimeout(2)
        assert bystander_peer.recv(64) == b"+PONG\r\n", "the bystander must still be served"
    finally:
        for conn in list(server._connections):
            server._close(conn)
        bystander_peer.close()
        server._loop.close()


def test_close_flushes_a_still_queued_reply_to_the_peer():
    # a queued reply is owed to the client; _close discards it only if it skips the flush
    server = Server(0)
    sock, peer = socket.socketpair()
    sock.setblocking(False)
    conn = Connection(sock, ("stub", 0))
    server._loop.register(conn)
    server._connections.add(conn)

    conn.queue(b"+OK\r\n")
    try:
        server._close(conn)
        peer.settimeout(2)
        assert peer.recv(64) == b"+OK\r\n"
    finally:
        peer.close()
        server._loop.close()


def test_a_failed_registration_costs_one_connection_not_the_server(monkeypatch):
    # _on_accept runs outside _guard, so an unguarded raise here unwinds run_once and takes every other client with it
    with listening() as (server, connect, _listener):
        bystander = connect()
        pump(server)
        assert len(server._connections) == 1

        real_register = server._loop.register
        calls = {"n": 0}

        def register_failing_once(conn):
            calls["n"] += 1
            if calls["n"] == 1:
                raise KeyError("stale registration for this descriptor")
            return real_register(conn)

        monkeypatch.setattr(server._loop, "register", register_failing_once)
        doomed = connect()
        pump(server)

        assert len(server._connections) == 1, "the failed accept must not be tracked"
        monkeypatch.undo()
        survivor = connect()
        pump(server)
        assert len(server._connections) == 2, "the server must still accept after a failed registration"

        bystander.sendall(b"*1\r\n$4\r\nPING\r\n")
        pump(server)
        bystander.settimeout(2)
        assert bystander.recv(64) == b"+PONG\r\n", "the bystander must still be served"
        doomed.close()
        survivor.close()


def _stop_the_loop_from_inside(server):
    # run() sets _running itself, so a flag set before the call is overwritten; the only way out is from within the loop
    real_run_once = server._loop.run_once

    def run_once_then_stop():
        server._running = False
        return real_run_once()

    server._loop.run_once = run_once_then_stop


def test_a_close_that_raises_during_shutdown_still_releases_the_listener():
    # the shutdown sweep runs after the loop has exited, where _guard no longer applies, so one bad close must not strand the listener or the selector
    server = Server(0)
    sock, peer = socket.socketpair()
    sock.setblocking(False)
    conn = Connection(sock, ("stub", 0))
    server._loop.register(conn)
    server._connections.add(conn)

    def unregister_raising(_conn):
        raise KeyError("not registered")

    server._loop.unregister = unregister_raising
    _stop_the_loop_from_inside(server)
    try:
        server.run()
    finally:
        peer.close()
        sock.close()

    assert server._loop._selector.get_map() is None, "the selector must be closed even though a close raised"


def test_running_one_server_twice_names_the_reuse():
    # the selector is closed on the way out, so without this the second run dies four frames down in selectors naming kqueue
    server = Server(0)
    _stop_the_loop_from_inside(server)
    server.run()
    with pytest.raises(RuntimeError) as exc_info:
        server.run()
    assert "already run" in str(exc_info.value), str(exc_info.value)


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
    # a subprocess with a deadline, because the failure is a run loop that never returns, which
    # in-process would hang the suite rather than fail it
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


def test_run_gives_the_signal_handlers_back():
    # run() is called in-process here, so a handler left pointing at a discarded Server would
    # swallow every later signal in the pytest process -- Ctrl-C included
    before = (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM))
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    server = Server(port)

    def stop(signum, frame):
        server._running = False

    previous_alarm = signal.signal(signal.SIGALRM, stop)
    signal.setitimer(signal.ITIMER_REAL, 0.2)
    try:
        server.run()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_alarm)

    assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before


def test_a_failed_bind_leaves_the_server_usable():
    # _ran marks the selector as spent, and a bind that never reached the selector spends nothing
    squatter = socket.socket()
    squatter.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    squatter.bind(("127.0.0.1", 0))
    squatter.listen(1)
    port = squatter.getsockname()[1]
    server = Server(port)
    before = (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM))
    try:
        with pytest.raises(OSError):
            server.run()
        assert server._ran is False
        assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before
    finally:
        squatter.close()
