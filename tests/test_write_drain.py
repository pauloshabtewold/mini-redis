import logging
import selectors
import socket
import threading
import time
import types

import pytest

import server as server_module
from connection import Connection
from server import Server, build_arg_parser
from tests.test_server_lifecycle import listening, pump


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
