import contextlib
import logging
import pathlib
import selectors
import signal
import socket
import subprocess
import sys
import time

import pytest

import commands
import store as store_module
from commands import registry
from connection import Connection
from server import DEFAULT_PORT, Server, build_arg_parser
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
    # the body is framing bytes, so a parser scanning for CRLF instead of honouring the declared
    # length truncates it; ECHO carries the whole body back out, where a drained buffer alone
    # would say nothing about what came out of it
    server, client = server_and_client
    body = b"*2\r\n$3\r\nfoo\r\n"
    client.sendall(b"*2\r\n$4\r\nECHO\r\n$%d\r\n" % len(body) + body + b"\r\n")
    pump(server)
    client.settimeout(2)
    assert client.recv(64) == b"$%d\r\n" % len(body) + body + b"\r\n"
    conn = next(iter(server._connections))
    assert conn.read_buffer == bytearray()


def test_command_split_across_writes_is_reassembled(server_and_client):
    # one byte per write, so every element boundary lands mid-parse. the echoed argument is what
    # proves an argv was reassembled: an empty buffer is also what a byte still in the kernel looks
    # like, and what a parser inventing an argv out of nothing would leave behind
    server, client = server_and_client
    for byte in b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n":
        client.sendall(bytes([byte]))
        pump(server, times=1)
    pump(server)
    client.settimeout(2)
    assert client.recv(64) == b"$2\r\nhi\r\n"
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
        # surviving in the set is not being served: a reply is what says the loop still dispatches
        bystander.settimeout(2)
        assert bystander.recv(64) == b"+PONG\r\n", "the bystander must still be answered"


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


def test_a_failed_registration_costs_one_connection_not_the_server(monkeypatch, caplog):
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

    # _on_accept is outside the boundary and has no connection left to answer on, so the log line
    # is the only trace that a client was turned away at the door
    refused = [r for r in caplog.records if r.levelname == "ERROR" and r.exc_info]
    assert refused, caplog.records
    assert refused[0].exc_info[0] is KeyError, refused[0].exc_info


def _stop_the_loop_from_inside(server):
    # run() sets _running itself, so a flag set before the call is overwritten; the only way out is from within the loop
    real_run_once = server._loop.run_once

    def run_once_then_stop():
        server._running = False
        return real_run_once()

    server._loop.run_once = run_once_then_stop


def test_a_close_that_raises_during_shutdown_still_releases_the_listener(caplog):
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
    # the sweep runs with the loop gone and no client left to answer to, so the log line is the
    # only record that a connection was abandoned rather than closed
    abandoned = [r for r in caplog.records if r.levelname == "ERROR" and r.exc_info]
    assert abandoned, caplog.records
    assert abandoned[0].exc_info[0] is KeyError, abandoned[0].exc_info


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
_ANNOUNCE_ON_BIND = """
import socket, sys, threading
sys.path.insert(0, %r)
from server import Server

bound = threading.Event()
chosen = []


class Announcing(Server):
    def _open_listener(self):
        listener = super()._open_listener()
        chosen.append(listener.getsockname()[1])
        bound.set()
        return listener


server = Announcing(0)


def connect_then_stop():
    bound.wait(5)
    with socket.create_connection(("127.0.0.1", chosen[0]), timeout=5):
        pass
    print("connected", chosen[0], flush=True)
    server._running = False


threading.Thread(target=connect_then_stop, daemon=True).start()
server.run()
"""


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
    # the last line, not the whole stream: a successful bind announces itself first
    assert probe.stdout.split()[-1:] == ["returned"], (probe.returncode, probe.stdout, probe.stderr)


def test_accept_with_nothing_pending_adds_no_connection():
    # the listener is non-blocking, so accept() with an empty backlog raises BlockingIOError, which is the OSError branch: a real readiness-then-gone race, no mock needed
    with listening() as (server, _connect, listener):
        # asserted before the call, not after: a blocking listener waits in accept() for a peer
        # that is never coming, which hangs the suite instead of failing it
        assert listener.getblocking() is False
        server._on_accept(listener)
        assert server._connections == set()


def test_the_listener_is_bound_to_loopback_only():
    # the replication sync command is unauthenticated, so a wildcard bind offers it to the whole
    # network -- and every other test here connects over 127.0.0.1 either way, so nothing else
    # here can tell the two binds apart
    with listening() as (_server, _connect, listener):
        assert listener.getsockname()[0] == "127.0.0.1"


def test_the_listener_takes_the_port_back_from_a_lingering_close():
    # without SO_REUSEADDR a restart inside the TIME_WAIT window cannot rebind the port it just
    # released, and the server that was stopped a second ago refuses to come back for a minute
    with listening() as (_server, _connect, listener):
        assert listener.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR) != 0


def test_the_listener_asks_for_the_largest_backlog_the_kernel_allows(monkeypatch):
    # a full accept queue drops SYNs rather than refusing them, so a burst of clients waits out its
    # own retransmit timer instead of failing. the depth cannot be read back off the socket, which
    # leaves the call itself as the only place to see it
    server = Server(0)
    backlogs = []
    real_listen = socket.socket.listen

    def recording_listen(sock, backlog):
        backlogs.append(backlog)
        return real_listen(sock, backlog)

    monkeypatch.setattr(socket.socket, "listen", recording_listen)
    listener = server._open_listener()
    try:
        assert backlogs == [socket.SOMAXCONN]
    finally:
        listener.close()
        server._loop.close()


def test_the_default_port_is_the_one_every_client_assumes():
    # a client given no port connects to 6379, so this default is part of the wire contract
    assert build_arg_parser().parse_args([]).port == 6379


def test_the_loop_is_built_with_a_bounded_select_timeout():
    # the value the loop is constructed with rather than a measured wall clock, which goes red on a
    # loaded machine for reasons that have nothing to do with the timeout: this bounds how long a
    # stop signal waits to be noticed and floors every periodic interval this design will have
    server = Server(0)
    try:
        assert server._loop._timeout == 0.1
    finally:
        server._loop.close()


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
    # the loop is stopped from inside as well: this test is about the bind failing, and a bind that
    # succeeds instead leaves run() in select() forever rather than reporting the surprise
    _stop_the_loop_from_inside(server)
    try:
        with pytest.raises(OSError):
            server.run()
        assert server._ran is False
        assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before
    finally:
        squatter.close()


def test_a_close_that_raises_still_leaves_the_set_clean():
    # Connection.close() sets its flag before it touches the socket, so a raise there would strand
    # the connection in _connections with every later _close returning at the idempotence guard
    with listening() as (server, connect, _listener):
        client = connect()
        pump(server)
        conn = next(iter(server._connections))

        def explode():
            conn.closed = True
            raise OSError(9, "Bad file descriptor")

        conn.close = explode
        with pytest.raises(OSError):
            server._close(conn)
        assert conn not in server._connections
        client.close()


def _raising_command(name, exc):
    """Register a handler that raises, and give it back on the way out.

    Nothing in the shipped registry can raise, so the one construct this feature calls mandatory
    -- the per-connection boundary -- is never entered by any other test in this suite.
    """
    original = registry.COMMANDS[name]
    del registry.COMMANDS[name]

    @registry.command(name, arity=original.arity, kind=original.kind)
    def explode(store, conn, argv):
        raise exc

    return original


def test_the_boundary_closes_one_connection_and_leaves_the_others(caplog):
    original = registry.COMMANDS[b"ECHO"]
    _raising_command(b"ECHO", ZeroDivisionError("deliberate"))
    try:
        with listening() as (server, connect, _listener):
            victim, bystander = connect(), connect()
            pump(server)
            assert len(server._connections) == 2

            victim.sendall(b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n")
            pump(server)
            assert len(server._connections) == 1, "the raising connection must be closed"

            bystander.sendall(b"*1\r\n$4\r\nPING\r\n")
            pump(server)
            bystander.settimeout(2)
            assert bystander.recv(64) == b"+PONG\r\n", "the bystander must keep working"

            newcomer = connect()
            pump(server)
            newcomer.sendall(b"*1\r\n$4\r\nPING\r\n")
            pump(server)
            newcomer.settimeout(2)
            assert newcomer.recv(64) == b"+PONG\r\n", "a new connection must still be accepted"

            for c in (victim, bystander, newcomer):
                c.close()
    finally:
        registry.COMMANDS[b"ECHO"] = original

    # the boundary logs at ERROR with the traceback. caplog rather than stderr: pytest attaches a
    # root handler, so logging.lastResort -- which carries it to stderr in the real server -- is
    # never reached under the suite
    boundary = [r for r in caplog.records if r.levelname == "ERROR" and r.exc_info]
    assert boundary, caplog.records
    assert boundary[0].exc_info[0] is ZeroDivisionError, boundary[0].exc_info


def test_the_boundary_also_covers_the_write_side(caplog):
    # _on_writable routes through the same construct; without it a raise on the drain would take
    # the process down where the identical bug on the read path only costs one client
    with listening() as (server, connect, _listener):
        client = connect()
        pump(server)
        conn = next(iter(server._connections))
        conn.queue(b"+PONG\r\n")

        real_flush = conn.flush
        calls = []

        def explode_once():
            # one shot: _close calls flush() again on its way out, and a real flush cannot raise,
            # so a permanently-raising stub would test the boundary against a socket that cannot exist
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("deliberate write-side failure")
            return real_flush()

        conn.flush = explode_once
        server._on_writable(conn)

        assert calls, "the write path never reached flush()"
        assert conn.closed is True
        assert conn not in server._connections
        client.close()

    boundary = [r for r in caplog.records if r.levelname == "ERROR" and r.exc_info]
    assert boundary, caplog.records
    assert boundary[0].exc_info[0] is RuntimeError, boundary[0].exc_info


def test_replies_come_back_in_the_order_the_commands_were_sent():
    with listening() as (server, connect, _listener):
        client = connect()
        client.settimeout(2)
        pump(server)
        client.sendall(
            b"*1\r\n$4\r\nPING\r\n"
            b"*2\r\n$4\r\nECHO\r\n$1\r\na\r\n"
            b"*1\r\n$9\r\nNOSUCHCMD\r\n"
            b"*1\r\n$4\r\nECHO\r\n"
            b"*2\r\n$4\r\nECHO\r\n$1\r\nz\r\n"
        )
        pump(server)
        want = (
            b"+PONG\r\n"
            b"$1\r\na\r\n"
            b"-ERR unknown command 'NOSUCHCMD'\r\n"
            b"-ERR wrong number of arguments for 'echo' command\r\n"
            b"$1\r\nz\r\n"
        )
        got = b""
        while len(got) < len(want):
            chunk = client.recv(65536)
            if not chunk:
                break
            got += chunk
        assert got == want
        client.close()


def test_connection_ids_are_unique_across_descriptor_reuse():
    # descriptors are reused, so an id taken from fileno() would give two clients the same one
    ids, fds = [], []
    with listening() as (server, connect, _listener):
        for _ in range(4):
            client = connect()
            pump(server)
            conn = next(iter(server._connections))
            ids.append(conn.id)
            fds.append(conn.fileno())
            client.close()
            pump(server)
    assert len(set(ids)) == len(ids), ids
    assert ids == sorted(ids), ids
    assert all(i > 0 for i in ids), ids


def test_a_connection_closed_by_the_read_pass_is_not_reached_by_the_write_pass():
    # one select() return can carry read and write readiness for the same descriptor. this pins the
    # outcome, not the mechanism: deleting event_loop's two `not conn.closed` guards does NOT fail
    # this, because _flush and receive() each refuse a closed connection on their own. the loop's
    # guards are a second line of defence, and that is worth knowing before anyone removes one
    with listening() as (server, connect, _listener):
        client = connect()
        pump(server)
        conn = next(iter(server._connections))
        conn.queue(b"+PONG\r\n")
        server._loop.set_write_interest(conn, True)

        server._close(conn)
        assert conn.closed is True

        # whatever order the ready list carries them in, neither callback may touch it now
        server._on_writable(conn)
        server._on_readable(conn)
        assert conn.closed is True
        client.close()


def test_a_recovery_that_also_fails_costs_one_connection_not_the_process():
    # _guard's whole purpose is that one connection's failure is not every connection's. its own
    # recovery calls _close, whose three statements can each raise, and a raise there reaches the
    # top of the only thread this server has
    server = Server(0)
    sock, peer = socket.socketpair()
    sock.setblocking(False)
    conn = Connection(sock, ("stub", 0))
    server._loop.register(conn)
    server._connections.add(conn)

    def step_raising(_conn):
        raise RuntimeError("the primary failure")

    def flush_raising():
        raise RuntimeError("and the recovery failed too")

    conn.flush = flush_raising
    try:
        server._guard(conn, step_raising)          # must not propagate
    finally:
        peer.close()

    assert conn not in server._connections, "the connection must not stay tracked"
    assert conn.closed is True, "the descriptor must be taken back"
    assert sock.fileno() == -1
    server._loop.close()


def test_a_shutdown_close_raising_outside_the_selector_tuple_still_closes_every_socket():
    # the sweep's nesting protects the port and the selector; nothing protected the descriptors,
    # and a type outside the old (KeyError, ValueError, OSError) abandoned every connection after
    # the first in iteration order
    server = Server(0)
    pairs = [socket.socketpair() for _ in range(4)]
    conns = []
    for sock, _peer in pairs:
        sock.setblocking(False)
        conn = Connection(sock, ("stub", 0))
        server._loop.register(conn)
        server._connections.add(conn)
        conns.append(conn)

    def unregister_raising(_conn):
        raise RuntimeError("outside the tuple the sweep used to name")

    server._loop.unregister = unregister_raising
    _stop_the_loop_from_inside(server)
    try:
        server.run()
    finally:
        for _sock, peer in pairs:
            peer.close()

    assert server._connections == set(), "every connection must leave the set"
    assert [c.closed for c in conns] == [True] * 4, "every descriptor must be released"
    assert server._loop._selector.get_map() is None


def test_every_signal_handler_is_restored_even_if_an_earlier_restore_raises():
    # a flat restore loop ends at the first raise, leaving every handler after it bound to a
    # Server that is being discarded -- which is the leak the restore exists to prevent. the
    # property is order-independent: whichever restore raises, the other is still attempted
    def mark_int(_signum, _frame):
        pass

    def mark_term(_signum, _frame):
        pass

    previous = [signal.signal(signal.SIGINT, mark_int), signal.signal(signal.SIGTERM, mark_term)]
    real_signal = signal.signal
    calls = 0
    attempted = []

    def signal_raising_on_the_first_restore(signum, handler):
        nonlocal calls
        calls += 1
        if calls > 2:                              # run() installs two handlers before restoring any
            attempted.append(signum)
            if len(attempted) == 1:
                raise RuntimeError("a signal arrived during the restore")
        return real_signal(signum, handler)

    server = Server(0)
    _stop_the_loop_from_inside(server)
    try:
        signal.signal = signal_raising_on_the_first_restore
        with pytest.raises(RuntimeError):
            server.run()
    finally:
        signal.signal = real_signal
        signal.signal(signal.SIGINT, previous[0])
        signal.signal(signal.SIGTERM, previous[1])

    assert set(attempted) == {signal.SIGINT, signal.SIGTERM}, attempted


def test_a_descriptor_that_will_not_close_is_logged_rather_than_raised(caplog):
    # the innermost step of the last resort: _close has already failed, and conn.close() is all that
    # is left between this connection and a leaked descriptor. a raise here has nothing behind it
    server = Server(0)
    sock, peer = socket.socketpair()
    sock.setblocking(False)
    conn = Connection(sock, ("stub", 0))
    server._loop.register(conn)
    server._connections.add(conn)

    def unregister_raising(_conn):
        raise RuntimeError("forces _close to fail")

    def close_raising():
        raise OSError("and the descriptor will not come back")

    server._loop.unregister = unregister_raising
    conn.close = close_raising
    try:
        with caplog.at_level(logging.ERROR):
            server._abandon(conn)              # must not propagate
    finally:
        peer.close()
        sock.close()
        server._loop.close()

    assert conn not in server._connections
    assert [r.message for r in caplog.records if "descriptor" in r.message], caplog.records


def test_a_successful_bind_announces_the_address_it_actually_bound():
    # a foreground server that says nothing on success cannot be told apart from one that died,
    # and the documented way to check this one is alive -- a round trip on the default port --
    # answers just as happily from another Redis that already owns it. --port 0 is the sharper
    # case: only the kernel knows the port, so the process is the only thing that can report it
    probe = subprocess.run(
        [sys.executable, "-c", _ANNOUNCE_ON_BIND % str(REPO_ROOT)],
        capture_output=True, text=True, timeout=15,
    )
    lines = probe.stdout.splitlines()
    announced = [line for line in lines if line.startswith("listening on ")]
    connected = [line for line in lines if line.startswith("connected ")]
    assert announced and connected, (probe.returncode, probe.stdout, probe.stderr)

    host, _, port = announced[0].removeprefix("listening on ").partition(":")
    assert host == "127.0.0.1", announced[0]
    assert port.isdigit() and int(port) not in (0, DEFAULT_PORT), announced[0]
    # the announced port is the one a client actually reached, not self.port, which was 0
    assert connected[0].split()[-1] == port, (announced[0], connected[0])


def test_the_effect_queue_is_drained_once_per_dispatched_command():
    # a batch delivered in one readable event is what a per-batch drain and a per-command
    # drain cannot be told apart on -- either placement empties the queue exactly once for
    # that event. dispatch() is wrapped as well as take_effects(), the two counts are
    # compared after every run_once(), and the run must contain at least one event that
    # carried more than one command, or it could not have told the two placements apart
    drains, dispatches, per_event = [], [], []
    original_take = store_module.Store.take_effects
    original_dispatch = commands.dispatch
    store_module.Store.take_effects = lambda self: (drains.append(1), original_take(self))[1]

    def counting_dispatch(store, conn, argv):
        dispatches.append(1)
        return original_dispatch(store, conn, argv)

    commands.dispatch = counting_dispatch
    try:
        with listening() as (server, connect, _listener):
            client = connect()
            pump(server)
            client.sendall(b"*1\r\n$4\r\nPING\r\n" * 3)
            deadline = time.monotonic() + 5
            while len(dispatches) < 3 and time.monotonic() < deadline:
                before = (len(dispatches), len(drains))
                server._loop.run_once()
                per_event.append((len(dispatches) - before[0], len(drains) - before[1]))
                assert len(drains) == len(dispatches), (len(drains), len(dispatches), per_event)
            assert len(dispatches) == 3, (dispatches, per_event)
            assert max(d for d, _ in per_event) >= 2, (
                "the three commands never shared a readable event, so this run could not "
                "have distinguished a per-batch drain from a per-command one", per_event)
    finally:
        store_module.Store.take_effects = original_take
        commands.dispatch = original_dispatch


def test_one_readable_event_dispatches_every_buffered_command():
    # written as an invariant over every readable event rather than a count of events, so
    # TCP's freedom to split a 14 KiB write across segments cannot make this flaky
    N = 1000
    with listening() as (server, connect, _listener):
        client = connect()
        pump(server)
        conn = next(iter(server._connections))
        client.setblocking(False)
        client.sendall(b"*1\r\n$4\r\nPING\r\n" * N)
        replies = b""
        deadline = time.monotonic() + 20
        while len(replies) < N * 7 and time.monotonic() < deadline:
            server._loop.run_once()
            left = conn.take_commands()
            assert left == [], "%d commands were parsed and left undispatched" % len(left)
            try:
                replies += client.recv(1 << 20)
            except BlockingIOError:
                pass
        assert replies == b"+PONG\r\n" * N, (len(replies), N * 7)
