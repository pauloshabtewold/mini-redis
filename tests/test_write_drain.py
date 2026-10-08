import errno
import logging
import os
import pathlib
import re
import select
import selectors
import socket
import subprocess
import sys
import threading
import time
import types

import pytest

import connection as connection_module
import server as server_module
from connection import RECV_SIZE, Connection
from server import Server, build_arg_parser
from tests.test_graceful_shutdown import _BoundedAccepts, _unacknowledged_bytes, _wait_until
from tests.test_server_lifecycle import listening, pump

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


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
        # the far end, so a test can feed commands into a connection whose send() is scripted
        conn._sock.peer = peer
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
    # BlockingIOError -- so both readings close something. progress leaves write interest set
    # against a buffer nothing drains, which spins the loop at 100% CPU and stops every client;
    # a disconnect costs the one connection that produced an outcome the standard forbids
    _server, conn = make_connection([0, 3, 2])
    conn.queue(b"+OK\r\n")
    assert conn.flush() is False, "a zero return must not be reported as progress"
    assert conn.write_buffer == bytearray(b"+OK\r\n"), "a zero return must not consume anything"


def test_a_write_buffer_limit_of_zero_switches_the_check_off():
    # 0 is the way to ask for no limit since the default is a real one, and it has to
    # reach the server through both doors as 0 and not be read as a small number
    assert build_arg_parser().parse_args(["--write-buffer-limit", "0"]).write_buffer_limit == 0
    server = Server(0, 0)
    try:
        assert server.write_buffer_limit == 0
    finally:
        server._loop.close()


def test_no_limit_lets_a_queued_reply_grow(make_connection):
    server, conn = make_connection([BlockingIOError])
    # asked for explicitly: the default is a real limit, and a 10,000-byte reply would
    # pass under it without the zero doing anything
    server.write_buffer_limit = 0
    conn.queue(b"x" * 10_000)
    server._flush(conn)
    assert not conn.closed, "a limit of zero must not close anything"
    assert len(conn.write_buffer) == 10_000


def test_a_connection_over_the_limit_is_closed_not_slowed(make_connection):
    # closing rather than throttling: a client that writes every request before reading
    # a reply blocks in send() the moment the server stops reading it, waiting for room
    # only its own reading would create, so refusing to read is a deadlock, not a brake
    # two refusals: the flush that trips the limit, and the best-effort flush _close
    # makes on the way out to hand the peer whatever the kernel will still take
    server, conn = make_connection([BlockingIOError, BlockingIOError])
    server.write_buffer_limit = 4096
    conn.queue(b"x" * 5000)
    server._flush(conn)
    assert conn.closed, "a connection past the limit must be closed"


def test_the_limit_is_measured_after_the_send_not_before(make_connection):
    # what matters is what the kernel would not take, not what was queued a moment ago:
    # a reply larger than the limit that leaves in one send() costs nothing to hold
    server, conn = make_connection([5000])
    server.write_buffer_limit = 4096
    conn.queue(b"x" * 5000)
    server._flush(conn)
    assert not conn.closed, "a buffer the kernel accepted whole must not trip the limit"
    assert conn.write_buffer == bytearray()


def test_a_buffer_exactly_at_the_limit_is_kept(make_connection):
    server, conn = make_connection([BlockingIOError])
    server.write_buffer_limit = 5000
    conn.queue(b"x" * 5000)
    server._flush(conn)
    assert not conn.closed, "the limit is a ceiling to exceed, not to reach"


def test_the_second_positional_parameter_is_the_limit_that_closes_a_connection_past_it():
    # the flag is --write-buffer-limit and the constructor takes it as its second
    # positional parameter, which is where main() binds it. every test above assigns the
    # attribute directly, so a constructor that took the limit under another name or at
    # another position would hand it to a different setting, or to none, and all of them
    # would stay green. this one builds the server the way main() does
    args = build_arg_parser().parse_args(["--write-buffer-limit", "4096"])
    server = Server(args.port, args.write_buffer_limit)
    real, peer = socket.socketpair()
    real.setblocking(False)
    try:
        assert server.write_buffer_limit == 4096
        # two refusals: the flush that trips the limit, and the best-effort flush _close makes
        conn = Connection(ScriptedSocket(real, [BlockingIOError, BlockingIOError]), ("stub", 0))
        server._loop.register(conn)
        server._connections.add(conn)
        conn.queue(b"x" * 4097)
        server._flush(conn)
        assert conn.closed, "a connection one byte past the limit it was started with must be closed"
        assert server._connections == set()
    finally:
        real.close()
        peer.close()
        server._loop.close()


def test_the_write_buffer_limit_defaults_to_thirty_two_mebibytes():
    # a real limit by default, and below --max-value-size's 64 MiB: a value can be stored
    # that cannot be read back, which is what a hard limit does and is why neither default
    # is moved. asserted as the number, not as the name of the constant that holds it
    assert build_arg_parser().parse_args([]).write_buffer_limit == 33554432
    server = Server(0)
    try:
        assert server.write_buffer_limit == 33554432
    finally:
        server._loop.close()


@pytest.mark.parametrize("value", ["-1", "-4096"])
def test_the_parser_refuses_a_negative_write_buffer_limit(value):
    # `limit and len(buf) > limit` reads any non-zero value as enabled, and every buffer
    # length exceeds a negative number -- including zero. left unchecked, --write-buffer-
    # limit -1 prints a healthy startup line and then closes every connection after its
    # first reply. -1 is a conventional spelling of "unlimited", so it is the likeliest
    # value to be typed here by someone reaching for exactly the opposite behaviour
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args(["--write-buffer-limit", value])


def test_the_server_itself_refuses_a_negative_write_buffer_limit():
    # the parser is not the only door: tests construct Server directly, and so will
    # anything embedding it
    with pytest.raises(ValueError):
        Server(0, -1)


@pytest.mark.parametrize("value", ["-1", "65536", "99999"])
def test_the_parser_refuses_a_port_outside_the_sixteen_bit_range(value):
    # argparse's own int() lets these reach bind(), which answers with an OverflowError
    # traceback where a mistyped port answers with a usage message
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args(["--port", value])


@pytest.mark.parametrize("value", ["0", "65535", "6379"])
def test_the_parser_still_accepts_every_legal_port(value):
    assert build_arg_parser().parse_args(["--port", value]).port == int(value)


ECHO_ABCD = b"*2\r\n$4\r\nECHO\r\n$4\r\nabcd\r\n"
ECHO_REPLY = b"$4\r\nabcd\r\n"


def _dispatch_echoes(server, conn, count):
    # the real read-and-dispatch path over a socket that never accepts a byte, so the
    # buffer the in-loop check sees is the one the batch actually built. a socketpair
    # would drain into the kernel and the check would never meet a full buffer at all
    conn._sock.peer.sendall(ECHO_ABCD * count)
    server._read_and_dispatch(conn)


def test_the_in_loop_limit_keeps_a_buffer_that_lands_exactly_on_it(make_connection):
    # the limit is consulted in two places and only _flush's boundary was pinned, so
    # turning the in-loop `>` into `>=` passed the whole suite. a buffer exactly at the
    # limit is within it: the check is a ceiling to exceed, not to reach
    server, conn = make_connection([BlockingIOError] * 20)
    server.write_buffer_limit = 4 * len(ECHO_REPLY)
    _dispatch_echoes(server, conn, 4)
    assert not conn.closed, "four replies land exactly on the limit and must be kept"
    assert len(conn.write_buffer) == server.write_buffer_limit


def test_the_in_loop_limit_closes_one_reply_past_it(make_connection):
    server, conn = make_connection([BlockingIOError] * 20)
    server.write_buffer_limit = 4 * len(ECHO_REPLY)
    _dispatch_echoes(server, conn, 5)
    assert conn.closed, "the fifth reply carries the buffer past the limit"


def _mask(server, conn):
    # the selector's own mask, which is the one record of interest there is. get_key raises
    # for a connection the selector does not hold, so a connection that wants neither
    # interest cannot be mistaken here for one that is merely quiet
    return server._loop._selector.get_key(conn).events


READ = selectors.EVENT_READ
WRITE = selectors.EVENT_WRITE


def test_read_interest_is_cleared_above_the_high_water_mark(make_connection):
    server, conn = make_connection([BlockingIOError, BlockingIOError])
    server.write_buffer_high_water = 4096
    server.write_buffer_low_water = 1024
    # as if a command were half sent when the pause arrives
    conn.incomplete_since = 123.0

    conn.queue(b"x" * 4096)
    server._flush(conn)
    assert _mask(server, conn) == READ | WRITE, "a queue exactly at the mark is not above it"
    assert conn.incomplete_since == 123.0, "a connection that is still being read keeps its deadline"

    conn.queue(b"x")
    server._flush(conn)
    assert _mask(server, conn) == WRITE, "one byte above the mark stops the reading and keeps the writing"
    assert conn.incomplete_since is None, (
        "a paused connection cannot finish its command, so the deadline for it is suspended")
    assert not conn.closed, "pausing a connection is not closing it"


def test_read_interest_is_restored_at_the_low_water_mark(make_connection):
    # one flush per scripted send: paused, still above the high-water mark, inside the
    # band between the marks, exactly at the low-water mark, and empty
    server, conn = make_connection([
        BlockingIOError,         # 5000 queued: above the high mark, paused
        900, BlockingIOError,    # 4100 left: still above it
        200, BlockingIOError,    # 3900 left: below the high mark and above the low one
        2876, BlockingIOError,   # 1024 left: at the low mark
        1024,                    # nothing left
    ])
    server.write_buffer_high_water = 4096
    server.write_buffer_low_water = 1024
    conn.read_buffer.extend(b"GET")    # a command that has begun and not ended
    conn.queue(b"x" * 5000)

    server._flush(conn)
    assert _mask(server, conn) == WRITE and conn.incomplete_since is None

    server._flush(conn)
    assert len(conn.write_buffer) == 4100
    assert _mask(server, conn) == WRITE

    server._flush(conn)
    assert len(conn.write_buffer) == 3900
    assert _mask(server, conn) == WRITE, (
        "between the marks nothing changes: dropping under the high mark is not a resume, "
        "and equal marks are the flapping this band exists to prevent")
    assert conn.incomplete_since is None

    server._flush(conn)
    assert len(conn.write_buffer) == 1024
    assert _mask(server, conn) == READ | WRITE, "at the low mark the reading resumes"
    assert conn.incomplete_since is not None, "the deadline of the command still outstanding restarts on resume"

    # a level is not an edge: nothing more changed, so nothing is re-armed
    conn.incomplete_since = 555.0
    server._flush(conn)
    assert conn.write_buffer == bytearray()
    assert _mask(server, conn) == READ
    assert conn.incomplete_since == 555.0

    # a paused connection that empties in one send is never taken out of the selector on the
    # way: reading is restored before writing is dropped, so at no point does it want neither
    other = make_connection([BlockingIOError, 5000])[1]
    other.queue(b"x" * 5000)
    server._flush(other)
    assert _mask(server, other) == WRITE
    selector = server._loop._selector
    calls = []
    inside_modify = []
    real_modify, real_unregister = selector.modify, selector.unregister

    def spy_modify(fileobj, events, data=None):
        # the base class implements modify as an unregister and a register, so only a call
        # made from outside one says what the loop itself asked of the selector
        calls.append("modify")
        inside_modify.append(True)
        try:
            return real_modify(fileobj, events, data)
        finally:
            inside_modify.pop()

    def spy_unregister(fileobj):
        if not inside_modify:
            calls.append("unregister")
        return real_unregister(fileobj)

    selector.modify, selector.unregister = spy_modify, spy_unregister
    try:
        server._flush(other)
    finally:
        del selector.modify, selector.unregister
    assert other.write_buffer == bytearray()
    assert _mask(server, other) == READ
    assert other.incomplete_since is None, "a resume arms no deadline for a connection holding no command"
    assert calls == ["modify", "modify"], (
        "the connection left the selector between pausing and resuming: %r" % calls)


def test_a_flush_during_the_drain_does_not_restore_reading_that_end_of_input_switched_off(
        make_connection):
    # during the shutdown drain, end of input on a connection that still owes bytes switches
    # its reading off, because end of input stays readable and would wake the loop on every
    # pass. the resume in _flush fires as soon as the queue is at or under the low-water
    # mark, so without the guard it switches the reading back on and the drain spins on end
    # of input until its deadline. the end-to-end test of the half-close only notices when
    # the queue happens to cross the mark while the loop is still waking, which is why this
    # one sets the situation up directly
    server, conn = make_connection([BlockingIOError, 3000, BlockingIOError, BlockingIOError])
    server.write_buffer_high_water = 4096
    server.write_buffer_low_water = 1024
    conn.queue(b"x" * 4000)
    server._flush(conn)
    assert _mask(server, conn) == READ | WRITE, "inside the band nothing is paused"

    server._draining = True
    server._loop.set_read_interest(conn, False)        # what the end-of-input hold does
    assert _mask(server, conn) == WRITE

    server._flush(conn)
    assert len(conn.write_buffer) == 1000, "the queue is under the low-water mark"
    assert _mask(server, conn) == WRITE, (
        "a flush during the drain switched back on the reading that end of input switched off")

    # the control: the same queue outside the drain does resume the reading, so the
    # assertion above is the guard's doing and not an effect of the numbers
    server._draining = False
    server._flush(conn)
    assert _mask(server, conn) == READ | WRITE


def test_the_deadline_is_armed_before_the_dispatch_that_pauses_the_connection(make_connection):
    # the deadline for a half-sent command is armed when the batch is read and suspended by
    # the pause, and the order matters: a batch that fills the queue past the mark and ends
    # on the first half of a command pauses the connection while a command is outstanding,
    # and an arm that ran after the dispatch would start a deadline on a connection this
    # server has stopped reading
    server, conn = make_connection([BlockingIOError] * 20)
    server.write_buffer_high_water = 3 * len(ECHO_REPLY)
    server.write_buffer_low_water = len(ECHO_REPLY)
    conn._sock.peer.sendall(ECHO_ABCD * 5 + b"*2\r\n$4\r\nEC")
    server._read_and_dispatch(conn)
    assert conn.has_incomplete_command, "the batch was meant to end on half a command"
    assert _mask(server, conn) == WRITE, "five replies against a three-reply mark pause the connection"
    assert conn.incomplete_since is None


def test_each_mark_triggers_the_in_loop_flush_on_its_own(make_connection):
    # one recv can carry thousands of commands, so both the limit and the high-water mark
    # are consulted between replies as well as after the batch, and each by itself: the
    # mark is guarded by the mark and not by the limit, since a limit of 0 would otherwise
    # leave a pause decided once per batch, and the limit is guarded by the limit and not by
    # a mark that is off. the flushes are counted on a socket that never accepts a byte
    server, conn = make_connection([BlockingIOError] * 40)
    flushes = []
    real_flush = server._flush

    def counting(c):
        flushes.append(len(c.write_buffer))
        real_flush(c)

    server._flush = counting

    server.write_buffer_limit = 0
    server.write_buffer_high_water = 3 * len(ECHO_REPLY)
    server.write_buffer_low_water = len(ECHO_REPLY)
    _dispatch_echoes(server, conn, 8)
    assert len(flushes) > 1, "the mark was consulted once, after the batch, and not between replies"
    assert not conn.closed, "a mark pauses a connection and has nothing to do with closing it"

    other = make_connection([BlockingIOError] * 40)[1]
    server.write_buffer_limit = 4 * len(ECHO_REPLY)
    server.write_buffer_high_water = 0
    _dispatch_echoes(server, other, 8)
    assert other.closed
    assert len(other.write_buffer) == 5 * len(ECHO_REPLY), (
        "the limit was not consulted between replies: the whole batch was queued before it closed")


def test_the_sweep_closes_every_connection_past_the_incomplete_command_timeout(
        make_connection, monkeypatch):
    # two connections are past the limit in one sweep, so the sweep has to survive the first
    # close discarding from the set it is walking: iterating the set itself raises on the
    # first close, the boundary around the task logs it, and the second connection is left
    # holding its half-sent command
    server, first = make_connection([BlockingIOError] * 5)
    second = make_connection([BlockingIOError] * 5)[1]
    idle = make_connection([BlockingIOError] * 5)[1]
    server.incomplete_command_timeout = 30
    for conn in (first, second):
        conn._sock.peer.sendall(b"*1\r\n$4\r\nPI")
        server._read_and_dispatch(conn)
    assert first.incomplete_since is not None and second.incomplete_since is not None
    assert idle.incomplete_since is None, "a connection holding nothing has no deadline"

    server._tick()
    assert server._connections == {first, second, idle}, "nothing is past the limit yet"

    first.incomplete_since -= 60        # as if the command had begun a minute ago
    second.incomplete_since -= 60
    server.incomplete_command_timeout = 0
    server._tick()
    assert server._connections == {first, second, idle}, "0 means no limit, and nothing is closed for it"

    server.incomplete_command_timeout = 30
    server._tick()
    assert server._connections == {idle}, "both stalled connections must go in the one sweep"
    assert first.closed and second.closed and not idle.closed

    # the boundary itself, on an injected clock: older than the limit is closed, exactly at
    # it is not
    clock = [1000.0]
    monkeypatch.setattr(server_module, "time", types.SimpleNamespace(
        monotonic=lambda: clock[0], time=lambda: clock[0], sleep=lambda seconds: None))
    idle.incomplete_since = 970.0
    server._tick()
    assert server._connections == {idle}, "a command exactly at the limit is not past it"
    clock[0] = 1000.5
    server._tick()
    assert server._connections == set()


def test_a_half_sent_command_keeps_its_deadline_and_the_next_command_gets_its_own(make_connection):
    server, conn = make_connection([BlockingIOError] * 20)

    def feed(data):
        conn._sock.peer.sendall(data)
        server._read_and_dispatch(conn)
        # the clock is the real one here, so a re-arm is only visible once it has moved
        time.sleep(0.01)

    feed(b"*1\r\n$4\r\nP")
    first = conn.incomplete_since
    assert first is not None, "a command that has begun and not ended has a deadline"

    # more of the same command: restarting the deadline on each byte would let a client
    # sending a byte at a time hold its buffer indefinitely
    for piece in (b"I", b"N"):
        feed(piece)
        assert conn.incomplete_since == first

    # that command completes and the next has begun in the same read: a different command,
    # so a deadline of its own. a stream cut mid-command by every read would otherwise never
    # leave a clean buffer and would be closed at the limit however quickly each command
    # completed
    feed(b"G\r\n*1\r\n$4\r\nPI")
    assert conn.incomplete_since is not None and conn.incomplete_since > first

    feed(b"NG\r\n")
    assert conn.incomplete_since is None, "nothing is outstanding any more"


def test_the_drain_reads_and_discards_what_a_paused_connection_had_not_read():
    # a connection the high-water mark has stopped reading is stopped with whatever its peer
    # sent still in its receive queue, and the drain only reads and discards for a
    # connection it is reading. unless it switches the reading back on, those requests stay
    # where they are for the whole drain and the close that ends it is a reset, which
    # discards what the kernel had not yet put on the wire. the property is the empty
    # receive queue at the close and an orderly end of input for the peer, and the peer
    # receives exactly what it was owed: the request that was left unread is not answered
    with listening() as (server, connect, _listener):
        client = connect()
        pump(server)
        conn, = server._connections
        owed = 16 * 1024 * 1024
        conn.queue(b"x" * owed)
        server._flush(conn)
        assert len(conn.write_buffer) > server.write_buffer_high_water, (
            "the kernel took more than the test assumed, so nothing is paused")
        assert _mask(server, conn) == WRITE, "the connection is not paused"
        client.sendall(b"*1\r\n$4\r\nPING\r\n")

        received = bytearray()
        ended = []

        def read_until_the_end():
            client.settimeout(20)
            try:
                while True:
                    chunk = client.recv(1 << 20)
                    if not chunk:
                        ended.append("end of input")
                        return
                    received.extend(chunk)
            except OSError as exc:
                ended.append(repr(exc))

        reader = threading.Thread(target=read_until_the_end, daemon=True)
        reader.start()

        server._draining = True
        server._drain_for(20)
        assert conn.write_buffer == bytearray(), "the drain did not deliver everything it was owed"
        with pytest.raises(BlockingIOError):
            conn._sock.recv(1, socket.MSG_PEEK)
        server._abandon(conn)        # what the teardown at the end of the drain does to each connection

        reader.join(20)
        assert not reader.is_alive()
        assert ended == ["end of input"], ended
        assert len(received) == owed, (len(received), owed)


_PING = b"*1\r\n$4\r\nPING\r\n"


def test_the_drains_setup_pass_counts_and_clears_what_was_buffered_when_it_began(make_connection):
    # bytes that arrived before the stop are never dispatched, because the drain dispatches
    # nothing, so they are a loss and they are counted as one. clearing them here is what
    # keeps the count in the read path additive: from this point a buffer holds only what
    # arrived after this pass, so no byte is counted twice
    server, conn = make_connection([BlockingIOError])
    conn.read_buffer.extend(_PING * 3)
    buffered = len(conn.read_buffer)
    conn.queue(b"x" * 10)        # it owes bytes, so the drain keeps it rather than abandoning it
    server._draining = True

    server._drain_for(0)

    assert server._discarded_request_bytes == buffered, server._discarded_request_bytes
    assert not conn.read_buffer, "the setup pass clears every buffer it counts"


def test_a_read_during_the_drain_adds_what_it_threw_away_to_the_count(make_connection):
    # the site that catches the loss the pause creates: a connection the high-water mark
    # stopped reading has its requests waiting in the kernel's receive queue, the drain
    # switches reading back on and discards every one of them, and before this count
    # nothing anywhere reported that it had
    server, conn = make_connection([BlockingIOError])
    server._draining = True
    sent = _PING * 5
    conn._sock.peer.sendall(sent)

    server._read_and_dispatch(conn)

    assert server._discarded_request_bytes == len(sent), server._discarded_request_bytes
    assert not conn.read_buffer, "the buffer is emptied so it cannot grow across passes"
    assert not conn.write_buffer, "nothing is dispatched during the drain, so nothing is queued"


def test_a_stop_that_discards_nothing_reports_zero(make_connection):
    # the figure is a loss report, so the clean case has to read as clean rather than as a
    # number an operator has to interpret
    server, conn = make_connection([BlockingIOError])
    conn.queue(b"x" * 10)
    server._draining = True

    server._drain_for(0)

    assert server._discarded_request_bytes == 0, server._discarded_request_bytes


def test_the_drain_line_reports_the_bytes_it_discarded(make_connection, caplog):
    # the count exists to be read, and the drain's one line is the only place a stop says
    # anything at all. the two reply counts beside it cannot carry this, because a request
    # discarded undispatched is not a reply anybody is owed
    server, conn = make_connection([BlockingIOError])
    conn.read_buffer.extend(_PING * 4)
    discarded = len(conn.read_buffer)
    conn.queue(b"x" * 10)
    server._draining = True

    with caplog.at_level(logging.INFO, logger="server"):
        server._drain_for(0)

    lines = [r.getMessage() for r in caplog.records if "shutdown drain" in r.getMessage()]
    assert len(lines) == 1, lines
    assert "request bytes discarded undispatched: %d" % discarded in lines[0], lines[0]


def test_what_the_drain_never_read_is_counted_for_a_connection_it_closes(make_connection, caplog):
    # the shape that makes the figure worth having. a connection owing nothing is closed by the
    # setup pass with no read pass at all, so a count of what the drain read reports 0 for it
    # however much its peer sent -- and the close over a non-empty receive queue is a reset, which
    # is what makes those requests a loss rather than something a later pass would have reached
    server, conn = make_connection([BlockingIOError])
    sent = _PING * 5
    conn._sock.peer.sendall(sent)
    server._draining = True

    with caplog.at_level(logging.INFO, logger="server"):
        server._drain_for(0)

    assert conn.closed, "a connection owing nothing is closed by the setup pass"
    assert server._discarded_request_bytes == len(sent), server._discarded_request_bytes
    # the level decision, pinned where the figure is not zero, which is the only place it says
    # anything: no reply was owed to anyone, so the line is INFO and says complete although a
    # whole pipeline was thrown away. that is the documented choice and not an oversight, and
    # without this assertion promoting the level on a non-zero count would turn no test red
    line = [r for r in caplog.records if "shutdown drain" in r.getMessage()]
    assert len(line) == 1, [r.getMessage() for r in line]
    assert line[0].levelno == logging.INFO, line[0].getMessage()
    assert line[0].getMessage().startswith("shutdown drain complete"), line[0].getMessage()


def test_what_the_drain_never_read_is_counted_for_a_connection_still_owing_at_the_deadline(make_connection):
    # the same loss on the other arm: this connection owes a reply, so the setup pass keeps it, and
    # at a 0 timeout the loop runs no pass at all -- deadline = now + 0, and the clock does not go
    # backwards -- so every request its peer sent is still sitting in the receive queue when the
    # drain gives up. the figure has to carry those too, and nothing before it did
    server, conn = make_connection([BlockingIOError])
    sent = _PING * 7
    conn._sock.peer.sendall(sent)
    conn.queue(b"x" * 10)
    server._draining = True

    server._drain_for(0)

    assert not conn.closed and conn.write_buffer, "the case under test needs a connection still owed bytes"
    assert server._discarded_request_bytes == len(sent), server._discarded_request_bytes


def test_a_buffer_and_a_receive_queue_are_both_counted_and_not_one_instead_of_the_other(make_connection):
    # the setup pass's site and the close's site add. either one written as an assignment rather
    # than an addition reports one of the two and looks right doing it
    server, conn = make_connection([BlockingIOError])
    conn.read_buffer.extend(_PING * 3)
    buffered = len(conn.read_buffer)
    in_the_kernel = _PING * 9
    conn._sock.peer.sendall(in_the_kernel)
    conn.queue(b"x" * 10)
    server._draining = True

    server._drain_for(0)

    assert server._discarded_request_bytes == buffered + len(in_the_kernel), (
        server._discarded_request_bytes, buffered, len(in_the_kernel))


def test_a_buffer_and_what_a_drain_pass_reads_are_both_counted(make_connection):
    # the third pairing, and the one only a drain that actually runs a pass reaches: a 0 timeout
    # runs none -- deadline is now plus nothing and the clock does not go backwards -- so a test
    # that passes 0 cannot tell the read path's addition from an assignment. the connection is
    # left owing ten bytes the script blocks on once, so the first pass reads and the second send
    # empties the buffer and ends the loop rather than spinning it to a deadline
    server, conn = make_connection([BlockingIOError, 10])
    conn.queue(b"x" * 10)
    server._flush(conn)                             # the blocked send, which registers write interest
    conn.read_buffer.extend(_PING * 2)
    buffered = len(conn.read_buffer)
    arrives_later = _PING * 6
    conn._sock.peer.sendall(arrives_later)
    server._draining = True

    server._drain_for(1)

    assert not conn.write_buffer, "the drain was meant to end on an emptied buffer, not on its deadline"
    assert server._discarded_request_bytes == buffered + len(arrives_later), (
        server._discarded_request_bytes, buffered, len(arrives_later))


def test_the_setup_pass_sums_the_buffers_of_every_connection_and_not_the_last_one(make_connection):
    # one connection cannot tell an addition from an assignment, and a real stop has a population.
    # the buffers are deliberately different lengths so that the sum is not any one of them
    server, first = make_connection([BlockingIOError])
    _, second = make_connection([BlockingIOError])
    first.read_buffer.extend(_PING * 2)
    second.read_buffer.extend(_PING * 5)
    total = len(_PING) * 7
    for conn in (first, second):
        conn.queue(b"x" * 10)
    server._draining = True

    server._drain_for(0)

    assert server._discarded_request_bytes == total, server._discarded_request_bytes


def test_a_connection_that_owes_nothing_has_its_buffer_counted_before_it_is_abandoned(make_connection):
    # counted for every connection, owing or not, and this is the one the arm order could drop: a
    # client caught half way through a command has bytes in its read buffer and nothing queued for
    # it, so the setup pass abandons it. that is the case the figure's own level decision is argued
    # from, which makes it the one that has to be counted
    server, conn = make_connection([BlockingIOError])
    conn.read_buffer.extend(b"*1\r\n$4\r\nPI")       # a command that has begun and not ended
    buffered = len(conn.read_buffer)
    server._draining = True

    server._drain_for(0)

    assert conn.closed and not conn.write_buffer, "the case under test needs a connection owing nothing"
    assert server._discarded_request_bytes == buffered, server._discarded_request_bytes


def test_the_closes_after_the_drain_add_nothing_to_the_figure_it_already_reported(make_connection):
    # _shutdown closes every connection the drain left open, after the line has been written. those
    # closes go through _close like any other, so without the flag that scopes the accounting they
    # would count a receive queue the finally clause has already counted, and the attribute would
    # not match the figure anyone was shown
    server, conn = make_connection([BlockingIOError])
    conn._sock.peer.sendall(_PING * 4)
    conn.queue(b"x" * 10)
    server._draining = True

    server._drain_for(0)
    reported = server._discarded_request_bytes
    server._abandon(conn)                           # what _shutdown does to each connection

    assert server._discarded_request_bytes == reported, (server._discarded_request_bytes, reported)


def _raise_the_buffers(*socks):
    # a default socketpair holds 8 KiB unread, so a pipeline longer than that blocks sendall() before
    # there is a receive queue for the test to ask about, which is why no test that sends a few
    # hundred bytes could ever reach a drain that ends with a long queue still unread. raised on both
    # ends, because which end's limit governs depends on the platform
    for sock in socks:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 21)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 21)


@pytest.mark.parametrize("trailing", [b"", b"$1\r\nv"], ids=["buffer empty", "element half received"])
@pytest.mark.parametrize("owes_a_reply", [True, False], ids=["owing", "owing nothing"])
def test_the_setup_pass_counts_what_a_half_received_multibulk_has_already_consumed(
        make_connection, owes_a_reply, trailing):
    # take_commands deletes the bytes it parses, so an MSET that declared five elements and sent two
    # holds its header and both of them in the connection's own count and leaves the read buffer empty,
    # or holding only the element that is still arriving. a figure built from the buffer alone reported
    # 0 for 1,400,019 bytes of exactly this. the total is asserted against what went on the wire, which
    # is the one number that is right however the bytes are split between the buffer and the count, and
    # for a connection owing nothing as well, which the setup pass closes before either arm below it
    server, conn = make_connection([BlockingIOError])
    sent = b"*5\r\n$4\r\nMSET\r\n$1\r\nk\r\n" + trailing
    conn._sock.peer.sendall(sent)
    server._read_and_dispatch(conn)
    assert conn.consumed_for_incomplete_command > 0, "the case under test needs bytes held outside the buffer"
    assert len(conn.read_buffer) == len(trailing)
    if owes_a_reply:
        conn.queue(b"x" * 10)
    server._draining = True

    server._drain_for(0)

    assert conn.closed is not owes_a_reply, "a connection is kept exactly when it owes bytes"
    assert server._discarded_request_bytes == len(sent), (server._discarded_request_bytes, len(sent))
    assert conn.consumed_for_incomplete_command == 0, "the setup pass clears the count it has taken, as it does the buffer"


def test_a_half_received_multibulk_is_counted_once_across_the_setup_pass_and_a_later_drain_read(make_connection):
    # the count the setup pass takes is zeroed with the buffer, and what a pass of the drain reads
    # afterwards is added to a figure that already holds it. the two sites are disjoint by what they
    # measure and the sum is exact only if neither takes the other's bytes again
    server, conn = make_connection([BlockingIOError, 10])
    held = b"*5\r\n$4\r\nMSET\r\n$1\r\nk\r\n"
    conn._sock.peer.sendall(held)
    server._read_and_dispatch(conn)
    assert conn.consumed_for_incomplete_command == len(held) and conn.read_buffer == bytearray()
    conn.queue(b"x" * 10)
    server._flush(conn)                             # the blocked send, which registers write interest
    arrives_later = _PING * 6
    conn._sock.peer.sendall(arrives_later)
    server._draining = True

    server._drain_for(10)

    assert not conn.write_buffer, "the drain was meant to end on an emptied buffer, not on its deadline"
    assert server._discarded_request_bytes == len(held) + len(arrives_later), (
        server._discarded_request_bytes, len(held), len(arrives_later))
    assert conn.consumed_for_incomplete_command == 0


def test_a_drain_that_ends_on_an_emptied_buffer_still_counts_what_it_never_read(make_connection):
    # the survivor walk is the figure's last site and the one that carries it on a real stop: the drain
    # ends as soon as nothing is owed a reply, and a client with a pipeline longer than one recv() has
    # most of it still in the kernel when that happens. every other test of the figure leaves this walk
    # contributing nothing -- the setup pass closes the connection, or the 0 timeout runs no pass, or
    # the drain reads everything -- so a walk that skipped a connection whose buffer had emptied, which
    # is the case here, reported 65,542 of a measured 1,546,626 on a line still saying complete
    server, conn = make_connection([BlockingIOError, 10])
    conn.queue(b"x" * 10)
    server._flush(conn)                             # the blocked send, which registers write interest
    _raise_the_buffers(conn._sock._sock, conn._sock.peer)
    pipeline = _PING * 20_000
    # a timeout, so that a kernel that takes less is a failure here and not a send that never returns.
    # sendall() coming back under it is what shows the kernel took the whole pipeline: asking the
    # connection would be asking the very function the figure under test is made from
    conn._sock.peer.settimeout(5)
    conn._sock.peer.sendall(pipeline)
    assert len(pipeline) > 4 * RECV_SIZE, "the pipeline has to outlast the one recv() a pass makes by a wide margin"
    server._draining = True

    server._drain_for(10)

    assert not conn.closed and not conn.write_buffer, "the drain was meant to end on an emptied buffer with the connection still open"
    assert server._discarded_request_bytes == len(pipeline), (server._discarded_request_bytes, len(pipeline))


def test_the_survivor_walk_sums_the_receive_queues_of_every_connection_still_owing(make_connection):
    # two connections that both owe bytes at the deadline and both have requests the drain never read,
    # of different lengths so that the total is neither of them and not twice either. the only other
    # test with two connections puts nothing in either queue, so a walk that stopped after the first
    # reported a figure that was right for it
    server, first = make_connection([BlockingIOError])
    _, second = make_connection([BlockingIOError])
    first_sent = _PING * 100
    second_sent = _PING * 300
    first._sock.peer.sendall(first_sent)
    second._sock.peer.sendall(second_sent)
    for conn in (first, second):
        conn.queue(b"x" * 10)
    server._draining = True

    server._drain_for(0)

    assert not first.closed and not second.closed, "the case under test needs both connections still owing at the deadline"
    assert server._discarded_request_bytes == len(first_sent) + len(second_sent), (
        server._discarded_request_bytes, len(first_sent), len(second_sent))


# SKB_MAX_HEAD(0) + UNIX_SKB_FRAGS_SZ: what one buffer on an AF_UNIX stream receive queue holds --
# a page less the shared-info footer (4,096 - 320 = 3,776) plus the 32 KiB of paged fragments one
# sendmsg will attach to it. Measured rather than taken on faith: one non-blocking send() of 10 MiB
# into a socketpair whose SO_SNDBUF had been raised to 2 MiB was accepted 438,528 bytes at a time
# on Linux 6.8, which is twelve of these with nothing left over
_UNIX_SKB_BYTES = 36_544


def _a_kernel_that_answers_for_a_buffer_it_has_handed_over(total_sent):
    # the answer the kernel CI runs gives, reproduced on any kernel. It walks the receive queue
    # summing each queued buffer's WHOLE length, so a buffer whose front half a recv() has already
    # taken is reported as still unread in full; this host's kernel sums only the part of each that
    # nobody has taken yet. Derived from the real answer rather than from a table: what has been
    # read is total_sent minus what is really left, and the head buffer's consumed prefix is that
    # modulo one buffer's size, which is exactly the excess such a kernel reports
    real = connection_module.unread_in_kernel

    def answer(sock):
        really_left = real(sock)
        taken = total_sent - really_left
        return really_left + taken % _UNIX_SKB_BYTES

    return answer


def test_the_figure_is_right_on_a_kernel_that_answers_for_a_buffer_it_has_handed_over(
        make_connection, monkeypatch):
    # the one test here whose evidence comes from a kernel this host does not have. CI reported
    # 308,992 for this exact scenario against 280,000 sent, on both the 3.11 and the 3.13 leg and at
    # two different commits, because one recv() of RECV_SIZE empties the first 36,544-byte buffer and
    # leaves 28,992 bytes consumed inside the second, and a queue walk that sums whole lengths then
    # reports 243,456 still unread where 214,464 is the truth. 65,536 + 243,456 = 308,992, and the
    # 28,992 the read path had already counted is counted a second time -- which is not a figure
    # being wrong by a margin, it is the additivity the five shapes are specified to have failing.
    # Asserting against what the peer sent is what makes this independent of which kernel is
    # running it: the substituted answer is wrong by construction, and the figure has to come out
    # right anyway
    server, conn = make_connection([BlockingIOError, 10])
    conn.queue(b"x" * 10)
    server._flush(conn)
    _raise_the_buffers(conn._sock._sock, conn._sock.peer)
    pipeline = _PING * 20_000
    conn._sock.peer.settimeout(5)
    conn._sock.peer.sendall(pipeline)
    monkeypatch.setattr(
        connection_module, "unread_in_kernel",
        _a_kernel_that_answers_for_a_buffer_it_has_handed_over(len(pipeline)))
    # the control: the substituted kernel really does over-report for this scenario, and by the
    # amount CI reported. Without it a fix that stopped calling the ioctl at all would pass this
    # test against an answer that happened to be correct
    assert conn.unread_in_kernel() == len(pipeline), (
        "before a single read the whole pipeline sits in whole buffers, where the two kernels agree")
    server._draining = True

    server._drain_for(10)

    assert not conn.closed and not conn.write_buffer, (
        "the drain was meant to end on an emptied buffer with the connection still open")
    assert server._discarded_request_bytes == len(pipeline), (
        server._discarded_request_bytes, len(pipeline))


def test_the_close_site_reads_the_queue_rather_than_asking_about_it(make_connection, monkeypatch):
    # the sixth review's finding 47. the repair has two sites -- _close and the survivor walk --
    # and reverting the _close one to the bare ioctl passed all 1,096 tests, because the three
    # tests written for the repair only ever drove the walk. this drives _close: the connection
    # owes nothing, so the drain's setup pass abandons it through _close before any pass runs,
    # which is the site under test. one recv was read and DISPATCHED before the stop, so the
    # over-reporting kernel answers for bytes this server already handled
    # every send refuses, so the replies stay queued and are cleared below by hand: this test is
    # about what the CLOSE counts, and a send that succeeds would only add scheduling noise
    server, conn = make_connection([BlockingIOError] * 10)
    _raise_the_buffers(conn._sock._sock, conn._sock.peer)
    pipeline = _PING * 20_000
    conn._sock.peer.settimeout(5)
    conn._sock.peer.sendall(pipeline)
    # read and dispatch one recv's worth before the stop, so the kernel has a partly-consumed
    # buffer to lie about and those bytes are NOT discarded -- they were answered
    server._read_and_dispatch(conn)
    # counted from the replies, not from what the recv took: one recv of RECV_SIZE does not land
    # on a command boundary, so a few bytes stay in the read buffer unparsed. those ARE discarded
    # and the setup pass counts them correctly -- what must not be counted is the bytes that were
    # dispatched, which is one +PONG each
    dispatched = len(conn.write_buffer) // len(b"+PONG\r\n")
    assert dispatched > 0, "the setup for this test dispatched nothing, so it proves nothing"
    conn.write_buffer.clear()
    answered = dispatched * len(_PING)
    monkeypatch.setattr(
        connection_module, "unread_in_kernel",
        _a_kernel_that_answers_for_a_buffer_it_has_handed_over(len(pipeline)))
    server._draining = True

    server._drain_for(0)

    assert conn.closed, "the case under test needs the connection abandoned for owing nothing"
    assert server._discarded_request_bytes == len(pipeline) - answered, (
        "the close counted bytes it had already dispatched, so that site is trusting the ioctl",
        server._discarded_request_bytes, len(pipeline) - answered)


@pytest.mark.parametrize("raised", [ValueError("closed descriptor"), OSError(errno.EBADF, "bad fd")],
                         ids=["ValueError", "OSError"])
def test_a_recv_that_raises_inside_the_walk_does_not_cost_the_drain_its_line(
        make_connection, caplog, raised):
    # finding 48's third mutant: narrowing the walk's `except (OSError, ValueError)` to
    # BlockingIOError passed every test, because nothing here made recv raise anything else. the
    # ioctl's own broad catch is tested; the READ's was not, and it is the newer of the two. a
    # raise escaping here leaves the drain's finally clause without its one log line, which is the
    # loss DL-004's invariant exists to prevent -- and ValueError is what a socket object whose
    # descriptor has gone actually raises, which is why OSError alone is not enough
    server, conn = make_connection([BlockingIOError])
    conn.queue(b"x" * 10)
    conn._sock.peer.sendall(_PING * 10)

    def recv(_bufsize):
        raise raised

    conn._sock.recv = recv
    server._draining = True

    with caplog.at_level(logging.INFO, logger="server"):
        server._drain_for(0)

    drain_lines = [r for r in caplog.records if "shutdown drain" in r.getMessage()]
    assert len(drain_lines) == 1, (
        "the raise escaped the walk and the drain wrote no line, so a stop has no account of "
        "itself at all", [r.getMessage() for r in caplog.records])
    # and the figure keeps what the other sites had already counted rather than being lost
    assert "request bytes discarded undispatched" in drain_lines[0].getMessage()


def test_the_close_counts_nothing_when_the_drain_is_not_accounting(make_connection):
    # the companion gap, finding 48: replacing the `if self._counting_unread_at_close` gate with
    # `if True` also passed all 1,096 tests. it would make every ordinary close read and throw
    # away whatever the kernel held, outside any stop, and add it to a figure nobody is reporting
    server, conn = make_connection([BlockingIOError])
    conn._sock.peer.sendall(_PING * 10)
    assert not server._counting_unread_at_close, "this test needs the flag clear"

    server._close(conn)

    assert server._discarded_request_bytes == 0, (
        "an ordinary close added to the stop's figure, so the accounting gate is not being read",
        server._discarded_request_bytes)


def test_the_survivor_walk_takes_the_bytes_it_counts_off_the_socket(make_connection):
    # what makes the figure independent of the kernel's own arithmetic: the walk reads the remainder
    # and reports what it actually got, rather than asking and reporting the answer. An emptied
    # receive queue is the observable, and it is worth having for its own sake too -- the close
    # _shutdown makes next is a FIN over an empty queue where it was a reset over a full one, and a
    # reset discards whatever the flush in _close had just handed the kernel
    server, conn = make_connection([BlockingIOError, 10])
    conn.queue(b"x" * 10)
    server._flush(conn)
    _raise_the_buffers(conn._sock._sock, conn._sock.peer)
    pipeline = _PING * 20_000
    conn._sock.peer.settimeout(5)
    conn._sock.peer.sendall(pipeline)
    server._draining = True

    server._drain_for(10)

    assert server._discarded_request_bytes == len(pipeline), (
        server._discarded_request_bytes, len(pipeline))
    assert conn.unread_in_kernel() == 0, (
        "the walk counted the remainder without taking it, so the close that follows is still a reset",
        conn.unread_in_kernel())


def test_the_walk_reads_no_more_than_the_kernel_claimed_when_it_was_asked(make_connection):
    # the bound. Reading until the queue is empty would hand a peer that keeps sending a loop with no
    # end, at a point in the stop sequence that no deadline covers -- so the ioctl's answer is a
    # ceiling and the loop stops there whatever has arrived since. --shutdown-drain-timeout 0, so no
    # drain pass runs and the only recv() calls in the whole stop are the walk's own, which is what
    # lets the flood below be attributed to it
    server, conn = make_connection([BlockingIOError])
    conn.queue(b"x" * 10)
    waiting = _PING * 100
    conn._sock.peer.sendall(waiting)
    arrives_during_the_walk = _PING * 100
    reads = []
    real_recv = conn._sock.recv

    def recv(bufsize):
        # a peer that sends again between the ceiling and the read that was bounded by it
        conn._sock.peer.sendall(arrives_during_the_walk)
        reads.append(bufsize)
        return real_recv(bufsize)

    conn._sock.recv = recv
    server._draining = True

    server._drain_for(0)

    assert reads, "the walk never read, so this test bounded nothing"
    assert server._discarded_request_bytes == len(waiting), (
        server._discarded_request_bytes, len(waiting))
    assert conn.unread_in_kernel() == len(arrives_during_the_walk) * len(reads), (
        "the walk read past the ceiling it was given",
        conn.unread_in_kernel(), len(arrives_during_the_walk), len(reads))


def test_a_socket_closed_behind_its_connections_back_does_not_cost_the_drain_its_line(make_connection, caplog):
    # the survivor walk asks every connection it left open, from inside the finally clause that writes
    # the drain's one line, and a socket object that had given up its descriptor made that question
    # raise a ValueError, which escaped before the line was written and left a stop with no account of
    # itself at all. closed on the socket and not on the connection, because Connection.close() would
    # set the flag the walk reads and hide the case
    server, conn = make_connection([BlockingIOError])
    conn.queue(b"x" * 10)
    conn._sock.close()
    assert conn.closed is False, "the case under test needs a connection that does not know its socket is gone"
    server._draining = True

    with caplog.at_level(logging.INFO, logger="server"):
        server._drain_for(0)

    lines = [r.getMessage() for r in caplog.records if "shutdown drain" in r.getMessage()]
    assert len(lines) == 1, lines
    # still owed bytes, and asked about its receive queue without a raise: the answer is 0
    assert "connections still owed bytes: 1" in lines[0], lines[0]
    assert "request bytes discarded undispatched: 0" in lines[0], lines[0]


# the accept queue the stop leaves behind, as sockets a test hands over one at a time. a real
# listener's backlog fills a few hundred microseconds after a client's send() returns, with nothing on
# this side to wait on -- the connection is in nobody's hands -- so a count taken at once was short on
# about two runs in a hundred over loopback, and a test that waits for it with a sleep is a test of the
# sleep. an AF_UNIX pair delivers before send() returns, which makes the arithmetic and the order of
# the two sweeps exact; a real listener with real clients is what test_graceful_shutdown.py drives
class ScriptedListener:
    def __init__(self):
        self.pending = []
        self.socks = []
        self.events = []
        # set, accept() raises OSError where an empty queue would raise BlockingIOError: a listener the kernel refuses or whose descriptor has gone, which the sweep reaches only after it has taken whatever was pending
        self.broken = False
        self.refused = 0
        self.empty_asks = 0

    def join(self, payload, close_fails=False):
        accepted, client = socket.socketpair()
        self.socks.extend((accepted, client))
        client.sendall(payload)
        self.pending.append(_CloseRefused(accepted) if close_fails else accepted)
        return accepted

    def accept(self):
        if not self.pending:
            self.events.append("sweep")
            if self.broken:
                self.refused += 1
                # bounded, so that a sweep which went back to a listener that had just failed fails the test and does not park it
                assert self.refused <= 10, "the sweep went on asking a listener that had failed"
                raise OSError(errno.EMFILE, "Too many open files")
            # what a non-blocking listener raises for an empty queue, and what ends one sweep. bounded the way the broken path above is: a sweep that went back to a listener that had just said nothing was pending -- a break that became a continue -- would append to events for ever, and a timeout's own exception lands in _drain_for's finally clause, which sweeps again and re-enters the loop, so the runaway would be a hang and not a failure. a drain sweeps twice, and no test here sweeps more than a few times
            self.empty_asks += 1
            assert self.empty_asks <= 20, "the sweep went on asking a listener that had nothing pending"
            raise BlockingIOError()
        return self.pending.pop(0), ("stub", 0)


# a swept socket whose close() reports an error. the descriptor is released first and the error raised after, which is what close(2) does when it reports one: the sweep is being asked about an error and not about a leak, and the test can still see that the socket was let go
class _CloseRefused:
    def __init__(self, sock):
        self._sock = sock

    def fileno(self):
        return self._sock.fileno()

    def close(self):
        self._sock.close()
        raise OSError(errno.EIO, "Input/output error")


@pytest.fixture
def scripted_listener():
    listener = ScriptedListener()
    yield listener
    for sock in listener.socks:
        sock.close()


def test_the_accept_backlog_is_counted_and_closed_at_the_stop(scripted_listener):
    # clients that connected and sent before the stop and that nothing served: they are in no
    # connection set, so nothing else will ever ask what they sent, and the listener's own close would
    # have reset them. three of different lengths, so that the figure is a sum and not one of them
    server = Server(0)
    try:
        sent = [_PING * 50, _PING * 120, _PING * 300]
        swept = [scripted_listener.join(payload) for payload in sent]
        server._draining = True

        server._drain_for(0, scripted_listener)

        assert server._discarded_request_bytes == sum(len(payload) for payload in sent), server._discarded_request_bytes
        assert [sock.fileno() for sock in swept] == [-1, -1, -1], "a swept connection was left open"
    finally:
        server._loop.close()


def test_a_client_that_arrives_while_the_drain_is_running_is_counted(make_connection, scripted_listener):
    # one sweep at entry was not enough: the kernel goes on completing handshakes into the backlog for
    # the whole of the save and the drain, and a client that connects once the drain is under way
    # arrives after the only look that had been taken. the connection joins on the first pass, so the
    # sweep that finds it is the one in the finally clause
    server, conn = make_connection([BlockingIOError, 10])
    conn.queue(b"x" * 10)
    server._flush(conn)                             # the blocked send, which registers write interest
    arriving = _PING * 40
    arrived = []
    real_run_once = server._loop.run_once

    def run_once_with_an_arrival():
        if not arrived:
            arrived.append(scripted_listener.join(arriving))
        real_run_once()

    server._loop.run_once = run_once_with_an_arrival
    server._draining = True

    server._drain_for(10, scripted_listener)

    assert arrived, "no pass of the drain ran, so nothing arrived during it"
    assert not conn.write_buffer, "the drain was meant to end on an emptied buffer, not on its deadline"
    assert server._discarded_request_bytes == len(arriving), (server._discarded_request_bytes, len(arriving))
    assert arrived[0].fileno() == -1, "the late arrival was left open"


def test_the_backlog_is_swept_before_the_first_drain_pass_and_again_after_the_last(make_connection, scripted_listener):
    # the two looks in order, which no count can say: a client already waiting at the stop is found by
    # either sweep, so only the sequence shows that there is one ahead of the passes and one behind them
    server, conn = make_connection([BlockingIOError, 10])
    conn.queue(b"x" * 10)
    server._flush(conn)                             # the blocked send, which registers write interest
    real_run_once = server._loop.run_once

    def recording_run_once():
        scripted_listener.events.append("pass")
        real_run_once()

    server._loop.run_once = recording_run_once
    server._draining = True

    server._drain_for(10, scripted_listener)

    events = scripted_listener.events
    assert "pass" in events, "the drain ran no pass"
    assert events[0] == "sweep", events
    assert events[-1] == "sweep", events
    assert events.count("sweep") == 2, events


def test_a_listener_that_cannot_be_accepted_from_costs_the_drain_neither_its_line_nor_what_it_had_counted(scripted_listener, caplog):
    # the sweep's one way out that is not the ordinary one. the line the drain writes is the only account
    # a stop gives of itself, and the sweep runs inside the clause that writes it, so an OSError the sweep
    # let through would put a traceback where the figure should be. two clients are pending and the
    # listener fails once they are gone: that the figure still holds both is what separates a sweep that
    # stops where it failed from one that gives up what it had already counted, and the one error per
    # sweep, with no second ask, is what separates a stop from a retry
    server = Server(0)
    try:
        sent = [_PING * 50, _PING * 120]
        swept = [scripted_listener.join(payload) for payload in sent]
        scripted_listener.broken = True
        server._draining = True

        with caplog.at_level(logging.INFO, logger="server"):
            server._drain_for(0, scripted_listener)

        lines = [r.getMessage() for r in caplog.records if "shutdown drain" in r.getMessage()]
        assert len(lines) == 1, lines
        discarded = sum(len(payload) for payload in sent)
        assert "request bytes discarded undispatched: %d" % discarded in lines[0], lines[0]
        assert [sock.fileno() for sock in swept] == [-1, -1], "a swept connection was left open"
        # one failed accept for each of the two sweeps, and the sweep ended on it
        assert scripted_listener.events == ["sweep", "sweep"], scripted_listener.events
        failures = [r for r in caplog.records if r.getMessage().startswith("could not sweep the accept backlog")]
        assert len(failures) == 2, [r.getMessage() for r in caplog.records]
        # the error is in the message and there is no traceback, at a level a configuration that hides INFO
        # still shows. a traceback is rendered by opening source files, and descriptor exhaustion is the
        # likeliest reason to be here, where that open fails too and only "--- Logging error ---" is left
        for record in failures:
            assert record.levelno == logging.ERROR, record.levelname
            assert record.exc_info is None, record.exc_info
            assert "Too many open files" in record.getMessage(), record.getMessage()
            assert "does not count what it still held" in record.getMessage(), record.getMessage()
    finally:
        server._loop.close()


def test_a_swept_socket_that_will_not_close_is_still_counted_and_does_not_end_the_sweep(scripted_listener, caplog):
    # the count is taken before the close and the close is in a finally clause, so a close that raises
    # is the one place the sweep could lose what it had just learned. the refusing socket comes first
    # and a healthy one behind it, so that the figure is a sum that includes the one that refused and
    # the second socket's closure shows the sweep went on past it. one sweep and not the drain, because
    # the drain looks twice and a sweep that stopped at the refusal would have the second look find
    # what the first had left, which is exactly the loss this is here to see
    server = Server(0)
    try:
        refusing_sent = _PING * 70
        closing_sent = _PING * 130
        refusing = scripted_listener.join(refusing_sent, close_fails=True)
        closing = scripted_listener.join(closing_sent)

        with caplog.at_level(logging.INFO, logger="server"):
            total = server._count_unaccepted_backlog(scripted_listener)

        assert total == len(refusing_sent) + len(closing_sent), total
        assert closing.fileno() == -1, "the sweep stopped at the socket that would not close"
        assert refusing.fileno() == -1, "the socket that reported the error was never closed"
        failures = [r for r in caplog.records if r.getMessage() == "could not close a swept backlog socket"]
        assert len(failures) == 1, [r.getMessage() for r in caplog.records]
        # a traceback and not a bare message, at a level a configuration that hides INFO still shows
        assert failures[0].levelno == logging.ERROR, failures[0].levelname
        assert isinstance(failures[0].exc_info[1], OSError), failures[0].exc_info
    finally:
        server._loop.close()


def test_a_swept_socket_that_will_not_close_costs_the_drain_neither_its_line_nor_the_figure(scripted_listener, caplog):
    # the same refusal seen from the drain, whose one line is written from a finally clause: an error
    # the sweep let out of its own finally would replace the line with a traceback, and the bytes the
    # refusing socket held are in the figure only if the count was kept when the close failed
    server = Server(0)
    try:
        sent = _PING * 90
        refusing = scripted_listener.join(sent, close_fails=True)
        server._draining = True

        with caplog.at_level(logging.INFO, logger="server"):
            server._drain_for(0, scripted_listener)

        lines = [r.getMessage() for r in caplog.records if "shutdown drain" in r.getMessage()]
        assert len(lines) == 1, lines
        assert "request bytes discarded undispatched: %d" % len(sent) in lines[0], lines[0]
        assert refusing.fileno() == -1, "the socket that reported the error was never closed"
    finally:
        server._loop.close()


def test_a_swept_socket_is_closed_when_asking_what_it_holds_raises_something_unexpected(scripted_listener, monkeypatch):
    # the close is in a finally clause so that it does not depend on the question being answered: the
    # question catches OSError and ValueError inside itself, and anything else it raised would otherwise
    # leave the swept socket open for as long as the exception took to be handled. nothing the kernel
    # does makes it raise a RuntimeError, so the question is replaced
    server = Server(0)
    try:
        swept = scripted_listener.join(_PING * 20)

        def exploding(sock):
            raise RuntimeError("injected: neither an OSError nor a ValueError")

        monkeypatch.setattr(server_module, "unread_in_kernel", exploding)
        with pytest.raises(RuntimeError):
            server._count_unaccepted_backlog(scripted_listener)
        assert swept.fileno() == -1, "the swept socket was left open when the question about it raised"
    finally:
        server._loop.close()


def _how_a_waiting_client_sees_its_connection(client, seconds):
    # "open" is the answer to a connection nothing has touched: it is not readable, so the wait for it
    # runs to its end, and that is the only answer that costs the whole of `seconds`
    ready, _, _ = select.select([client], [], [], seconds)
    if not ready:
        return "open"
    try:
        data = client.recv(1)
    except ConnectionResetError:
        return "reset"
    return "EOF" if data == b"" else "data"


@pytest.mark.parametrize("sent, ends_as", [
    (b"", "EOF"), (_PING, "reset"), (_PING[:11], "reset"),
], ids=["idle", "sent a command", "half-sent command"])
def test_a_client_waiting_in_the_backlog_at_the_stop_is_closed_when_the_drain_starts_and_not_when_it_ends(
        make_connection, sent, ends_as):
    # the entry sweep's one effect, and the only reason it is there: the sweep in the finally clause counts
    # everything this one does, so the figure is the same without it. what differs is when a client that was
    # already waiting when the stop landed finds out -- at the start of the drain, or when the drain is over,
    # which at the shipped timeout is up to five seconds of a connected client whose requests nothing will
    # read. the client is observed from inside the drain's first pass, which comes after the entry sweep and
    # before the sweep in the finally clause, and a client left to the later sweep is still open there. one
    # client per case, and the wait for the listener to be readable is what says its handshake has completed
    # and it is pending: with several, readable would say only that one of them is. a swept client that had
    # sent bytes sees the reset the listener's close was going to give it anyway, and an idle one sees an
    # orderly close
    server, conn = make_connection([BlockingIOError, 10])
    conn.queue(b"x" * 10)
    server._flush(conn)                             # the blocked send, which holds the drain open for a pass
    listener = socket.socket()
    client = None
    try:
        listener.bind(("127.0.0.1", 0))
        listener.listen(8)
        listener.setblocking(False)
        client = socket.create_connection(listener.getsockname())
        ready, _, _ = select.select([listener], [], [], 10)
        assert ready, "the client's handshake never completed into the backlog"
        if sent:
            client.sendall(sent)
            _wait_until(lambda: not _unacknowledged_bytes(client), 10, "the server's kernel to take the bytes")
        seen = []
        real_run_once = server._loop.run_once

        def observing_run_once():
            if not seen:
                seen.append(_how_a_waiting_client_sees_its_connection(client, 2))
            real_run_once()

        server._loop.run_once = observing_run_once
        server._draining = True

        server._drain_for(10, _BoundedAccepts(listener))

        assert seen, "no pass of the drain ran, so nothing was observed"
        assert seen == [ends_as], "the client was %s in the drain's first pass, where %s was expected" % (seen[0], ends_as)
    finally:
        if client is not None:
            client.close()
        listener.close()


def test_the_sweep_does_not_take_the_reserve_back_from_whoever_already_has_it(scripted_listener):
    # the sweep used to release the reserve itself, one statement before its first accept. it no
    # longer does: run() releases it before the shutdown save, the step whose failure costs the
    # keyspace, and the save closes its temporary file before returning so the slot is free again
    # here. a sweep that released it a second time would be closing a descriptor the kernel has
    # since handed to something else
    server = Server(0)
    try:
        scripted_listener.join(_PING * 20)
        server._hold_spare_descriptor()
        spare = server._spare_fd
        assert spare is not None
        os.fstat(spare)                             # the control: it is an open descriptor
        real_accept = scripted_listener.accept
        held_at_accept = []

        def watching_accept():
            held_at_accept.append(server._spare_fd)
            return real_accept()

        scripted_listener.accept = watching_accept

        server._count_unaccepted_backlog(scripted_listener)

        assert held_at_accept and held_at_accept[0] == spare, (
            "the sweep released a reserve it is no longer responsible for", held_at_accept)
        os.fstat(spare)                             # still open: the sweep closed nothing
        server._release_spare_descriptor()
        with pytest.raises(OSError) as raised:
            os.fstat(spare)
        assert raised.value.errno == errno.EBADF, raised.value
    finally:
        server._loop.close()


def test_a_reserve_that_cannot_be_opened_is_reported_without_a_traceback_and_does_not_stop_the_server(caplog, monkeypatch):
    server = Server(0)
    try:
        def refusing(path, flags):
            raise OSError(errno.EMFILE, "Too many open files")

        with monkeypatch.context() as patched, caplog.at_level(logging.WARNING, logger="server"):
            patched.setattr(server_module.os, "open", refusing)
            server._hold_spare_descriptor()

        assert server._spare_fd is None
        warnings = [r for r in caplog.records if "could not reserve a descriptor" in r.getMessage()]
        assert len(warnings) == 1, [r.getMessage() for r in caplog.records]
        assert warnings[0].levelno == logging.WARNING and warnings[0].exc_info is None, warnings[0]
        assert "Too many open files" in warnings[0].getMessage(), warnings[0].getMessage()
    finally:
        server._loop.close()


# a real descriptor table that is really full, because the failure is the kernel's and nothing scripted
# stands in for it faithfully: accept() raises EMFILE only when there is no descriptor to give. the child
# builds its own Server and listener, says which port, and waits to be told that the client has sent; it then
# lowers its own limit, fills what is left, and sweeps. the limit is lowered in the child and not here, where
# it would take the rest of the suite's descriptors with it
_FULL_TABLE_DRIVER = """
import errno, logging, os, resource, sys
sys.path.insert(0, %r)
from server import Server

logging.basicConfig(level=logging.INFO)
server = Server(0, shutdown_drain_timeout=0)
listener = server._open_listener()
if sys.argv[1] == "reserve":
    server._hold_spare_descriptor()
print(listener.getsockname()[1], flush=True)
# run() releases the reserve before the shutdown save and the save closes its temporary file,
# so by the time the sweeps run the slot is free. driven directly here, this stands in for both
sys.stdin.readline()
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (64, hard))
held = []
while True:
    try:
        held.append(os.open(os.devnull, os.O_RDONLY))
    except OSError as exc:
        assert exc.errno == errno.EMFILE, exc
        break
server._release_spare_descriptor()
server._draining = True
server._drain_for(0, listener)
"""

_SENT_TO_THE_BACKLOG = 180


@pytest.mark.parametrize("mode", ["reserve", "no reserve"])
def test_the_backlog_sweep_counts_a_waiting_client_when_the_descriptor_table_is_full(tmp_path, mode):
    # measured under `ulimit -n 30`: accept() raised EMFILE, the figure read 0 where 180 bytes had been
    # sent, and the line that was meant to say so could not be printed. with a descriptor held in reserve
    # the sweep counts them. the second case is the control that this instrument sees the failure at all,
    # and the one that pins what the operator is left with when there is no reserve: a figure of 0 that
    # says, in so many words and without a traceback, that it is short
    proc = subprocess.Popen(
        [sys.executable, "-c", _FULL_TABLE_DRIVER % str(REPO_ROOT), mode],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=tmp_path)
    client = None
    try:
        ready, _, _ = select.select([proc.stdout], [], [], 10)
        port_line = proc.stdout.readline().decode() if ready else ""
        assert port_line.strip().isdigit(), ("the driver never said which port it listened on", port_line)
        client = socket.create_connection(("127.0.0.1", int(port_line)))
        client.sendall(b"x" * _SENT_TO_THE_BACKLOG)
        _wait_until(lambda: not _unacknowledged_bytes(client), 10, "the server's kernel to take every byte sent")
        _, stderr = proc.communicate(b"go\n", timeout=60)
    finally:
        if client is not None:
            client.close()
        if proc.returncode is None:
            proc.kill()
            proc.communicate(timeout=10)
    stderr = stderr.decode()
    assert proc.returncode == 0, (proc.returncode, stderr)
    assert "Logging error" not in stderr, stderr
    found = re.findall(r"request bytes discarded undispatched: (\d+)", stderr)
    assert len(found) == 1, stderr
    if mode == "reserve":
        assert int(found[0]) == _SENT_TO_THE_BACKLOG, stderr
        assert "could not sweep" not in stderr, stderr
    else:
        assert int(found[0]) == 0, stderr
        assert "could not sweep the accept backlog: OSError(24, 'Too many open files')" in stderr, stderr


@pytest.mark.parametrize("flag, value", [
    ("--port", "abc"), ("--port", ""), ("--port", "1.5"),
    ("--write-buffer-limit", "abc"), ("--write-buffer-limit", "1e6"),
])
def test_a_non_numeric_flag_names_the_flag_rather_than_the_validator(capsys, flag, value):
    # argparse builds its own message from type='s __name__, which for these is a private
    # function -- "invalid _port value: 'abc'" puts an internal identifier in front of a
    # user who typed a bad port
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args([flag, value])
    message = capsys.readouterr().err
    assert flag in message, message
    assert "_port" not in message and "_write_buffer_limit" not in message, message


def test_the_parser_accepts_the_limit_value_its_own_help_text_recommends():
    # 0 reaches the class default without ever running the type= callback -- argparse
    # calls it only on a value actually typed -- so every existing check of 0 goes around
    # the validator rather than through it. Tightening `< 0` to `<= 0` therefore passes
    # the whole suite while refusing to start for anyone who writes the flag explicitly,
    # with an error that contradicts itself: "cannot be negative ... not 0"
    assert build_arg_parser().parse_args(
        ["--write-buffer-limit", "0"]).write_buffer_limit == 0
