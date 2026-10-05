"""The incomplete-command timeout: a connection that has begun a command and not finished
it is closed once --incomplete-command-timeout has passed, and a connection holding nothing
never is.

Driven on an injected monotonic clock, installed the way tests/test_periodic_tasks.py
installs it, so nothing here waits out a real thirty seconds and nothing can flake on a
loaded machine. The bytes arrive over a real socketpair and are dispatched by the event
loop's own run_once(), so the deadline is observed being armed where the server arms it,
and the sweep is reached through _tick(), the way the loop reaches it. Two tests run the
sweep's body directly instead, and say why: _tick() puts the sweep inside a boundary that
logs a failure and carries on, which would turn the failure they look for into a line in a
log. The one stand-in is the socket a Connection sends through, which can be told to take
nothing: it is the only way to put a reply queue over the high-water mark on a loopback
pair without writing megabytes. Every Server a test builds closes its selector in a
finally.
"""

import contextlib
import selectors
import socket
import types

import pytest

import server as server_mod
from connection import Connection
from server import Server


class _Clock:
    def __init__(self, start=1_000.0):
        self.t = start

    def monotonic(self):
        return self.t


@contextlib.contextmanager
def _injected_clock(start=1_000.0):
    # the server module's own `time` is rebound, not time.monotonic itself: patching the
    # real function reaches every module in this process.
    # a local copy rather than an import of the same helper in tests/test_periodic_tasks.py:
    # that name is private there, so reaching across for it means a rename in that file
    # breaks this one silently, and this module would be the one that looked broken. the
    # three-name namespace is the contract -- monotonic, time and sleep -- and a drift in it
    # is a loud AttributeError here rather than a quiet wrong answer
    clock = _Clock(start)
    real = server_mod.time
    server_mod.time = types.SimpleNamespace(
        monotonic=clock.monotonic, time=real.time, sleep=real.sleep
    )
    try:
        yield clock
    finally:
        server_mod.time = real

TIMEOUT = 30
START = 1_000.0

PING = b"*1\r\n$4\r\nPING\r\n"
PONG = b"+PONG\r\n"
ECHO_ABCD = b"*2\r\n$4\r\nECHO\r\n$4\r\nabcd\r\n"
ECHO_REPLY = b"$4\r\nabcd\r\n"

# the start of an inline command, and the start of a multibulk. the second is the case a
# check on the read buffer alone misses: the header and the first element are consumed, the
# second element is still owed, and the read buffer is empty
HALF_INLINE = b"PIN"
HALF_MULTIBULK = b"*2\r\n$4\r\nECHO\r\n"

# marks in units of one reply, so that five replies queued on a socket that takes nothing are
# over the high-water mark and the queue reaches the low-water mark exactly when one reply
# is left
PAUSING = {
    "write_buffer_high_water": 3 * len(ECHO_REPLY),
    "write_buffer_low_water": len(ECHO_REPLY),
}

READ = selectors.EVENT_READ
WRITE = selectors.EVENT_WRITE


class _Socket:
    """What a Connection sends and receives through: the real socket, except that a test
    can limit how much send() takes."""

    def __init__(self, sock):
        self._sock = sock
        # None passes sends through to the real socket. a number is how many more bytes
        # send() will accept, and at zero it refuses, as a full kernel buffer does
        self.room = None

    def fileno(self):
        return self._sock.fileno()

    def recv(self, size):
        return self._sock.recv(size)

    def close(self):
        self._sock.close()

    def send(self, data):
        if self.room is None:
            return self._sock.send(data)
        taken = min(self.room, len(data))
        if not taken:
            raise BlockingIOError()
        self.room -= taken
        return taken


class _Peer:
    """One connection as the server holds it, and the far end of its socket."""

    def __init__(self, rig, conn, sock, far):
        self.rig = rig
        self.conn = conn
        self.sock = sock
        self.far = far

    def send(self, data):
        # one readable event: the bytes arrive and the loop dispatches them
        self.far.sendall(data)
        self.rig.server._loop.run_once()

    def stall(self):
        self.sock.room = 0

    def take(self, size):
        # send() accepts this many bytes, and one writable event delivers them
        self.sock.room = size
        self.rig.server._loop.run_once()

    def received(self):
        # everything the server has sent so far. non-blocking, so a server that sent
        # nothing is b"" and not a hang
        try:
            return self.far.recv(65536)
        except BlockingIOError:
            return b""

    def at_end_of_input(self):
        # true once the server has closed its end and everything it sent has been read
        try:
            return self.far.recv(1) == b""
        except BlockingIOError:
            return False


class _Rig:
    def __init__(self, clock, **overrides):
        self.clock = clock
        self._sockets = []
        settings = {"incomplete_command_timeout": TIMEOUT}
        settings.update(overrides)
        self.server = Server(0, **settings)

    def connect(self):
        server_end, far = socket.socketpair()
        self._sockets += [server_end, far]
        server_end.setblocking(False)
        far.setblocking(False)
        sock = _Socket(server_end)
        conn = Connection(sock, ("peer", len(self._sockets)))
        self.server._loop.register(conn)
        self.server._connections.add(conn)
        conn.server = self.server
        return _Peer(self, conn, sock, far)

    def sweep(self):
        self.server._tick()
        # a sweep that raises is caught and logged by the tick's own boundary, so a
        # connection left open could be one the sweep never got to
        assert not self.server._failures_in_a_row, (
            "the sweep failed inside the tick: %r" % self.server._failures_in_a_row)

    def close(self):
        for sock in self._sockets:
            sock.close()
        self.server._loop.close()


@pytest.fixture
def build():
    rigs = []
    with _injected_clock(START) as clock:
        def make(**overrides):
            rig = _Rig(clock, **overrides)
            rigs.append(rig)
            return rig

        try:
            yield make
        finally:
            for rig in rigs:
                rig.close()


def _mask(peer):
    # the selector's own mask, the one record of what the loop dispatches a connection for.
    # get_key raises for a connection the selector does not hold, so one that has dropped out
    # of it is not mistaken for one that is merely quiet
    return peer.rig.server._loop._selector.get_key(peer.conn).events


def _assert_closed_only_past_the_timeout(rig, peer, armed_at):
    # a command is closed for when it is older than the limit, so one exactly at the limit is
    # still open, and so is one a second short of it
    for age in (TIMEOUT - 1, TIMEOUT):
        rig.clock.t = armed_at + age
        rig.sweep()
        assert not peer.conn.closed, "closed at %s seconds, inside a %d second limit" % (age, TIMEOUT)
    rig.clock.t = armed_at + TIMEOUT + 0.5
    rig.sweep()
    assert peer.conn.closed, "still open at %s seconds, past a %d second limit" % (TIMEOUT + 0.5, TIMEOUT)
    assert peer.conn not in rig.server._connections
    assert peer.at_end_of_input(), "the server closed its end and the peer was not told"


def _pause_on_a_half_sent_command(peer):
    # a batch of five replies on a socket that takes nothing, ending on the start of a sixth
    # command: the queue is over the high-water mark, so reading stops, with a command outstanding
    peer.stall()
    peer.send(ECHO_ABCD * 5 + HALF_INLINE)
    assert peer.conn.has_incomplete_command, "the batch was meant to end on half a command"
    assert len(peer.conn.write_buffer) == 5 * len(ECHO_REPLY)
    assert _mask(peer) == WRITE, "five replies against a three-reply mark should have paused the connection"


# the deadline for a half-sent command


def test_a_half_sent_inline_line_is_closed_once_the_timeout_has_passed(build):
    rig = build()
    peer = rig.connect()
    peer.send(HALF_INLINE)
    assert peer.conn.has_incomplete_command
    assert peer.conn.incomplete_since == START, "the deadline starts when the command does"
    _assert_closed_only_past_the_timeout(rig, peer, START)


def test_a_half_sent_multibulk_with_an_empty_read_buffer_is_closed_once_the_timeout_has_passed(build):
    rig = build()
    peer = rig.connect()
    peer.send(HALF_MULTIBULK)
    assert not peer.conn.read_buffer, (
        "every byte sent was consumed: what is owed is held in the parse state, not the buffer")
    assert peer.conn.has_incomplete_command
    assert peer.conn.incomplete_since == START, (
        "a command whose bytes have all been parsed is still a command that has not finished")
    _assert_closed_only_past_the_timeout(rig, peer, START)


def test_a_connection_holding_nothing_is_never_closed_however_long_it_idles(build):
    rig = build()
    silent = rig.connect()
    served = rig.connect()
    served.send(PING)
    assert served.received() == PONG, "the command was meant to be asked and answered"
    for peer in (silent, served):
        assert not peer.conn.has_incomplete_command
        assert peer.conn.incomplete_since is None
    for idle_for in (TIMEOUT + 1, 10 * TIMEOUT, 10_000 * TIMEOUT):
        rig.clock.t = START + idle_for
        rig.sweep()
        for peer in (silent, served):
            assert not peer.conn.closed, "closed after idling %d seconds holding nothing" % idle_for


def test_a_byte_at_a_time_trickle_is_closed_on_the_original_arm_time(build):
    # the client the deadline is for: it sends a byte a second, never finishes the line, and
    # holds a buffer for as long as it is allowed to. a deadline restarted by each byte would
    # never fire
    rig = build()
    peer = rig.connect()
    peer.send(b"x")
    armed_at = peer.conn.incomplete_since
    assert armed_at == START, "the first byte of a command starts its deadline"
    for second in range(1, TIMEOUT + 1):
        rig.clock.t = START + second
        peer.send(b"x")
        assert peer.conn.incomplete_since == armed_at, (
            "the byte at second %d restarted the deadline: it reads %r, armed at %r"
            % (second, peer.conn.incomplete_since, armed_at))
        rig.sweep()
        assert not peer.conn.closed, "closed at %d seconds, inside a %d second limit" % (second, TIMEOUT)
    # one byte arrives a second past the limit, and does not buy the connection more time
    rig.clock.t = START + TIMEOUT + 1
    peer.send(b"x")
    rig.sweep()
    assert peer.conn.closed, "a byte a second kept a half-sent command alive past its limit"


def test_the_deadline_is_cleared_when_the_command_completes(build):
    rig = build()
    peer = rig.connect()
    peer.send(b"*1\r\n$4\r\nPI")
    assert peer.conn.incomplete_since == START
    rig.clock.t = START + 10
    peer.send(b"NG\r\n")
    assert peer.received() == PONG, "the command was meant to complete"
    assert peer.conn.incomplete_since is None, "nothing is outstanding any more"
    # long past the limit counted from when that command began: it finished, so nothing is
    # late, and the connection must not be closed for a command it completed
    rig.clock.t = START + 10 * TIMEOUT
    rig.sweep()
    assert not peer.conn.closed
    # and the next command's deadline is its own, not one the last command left behind
    peer.send(HALF_INLINE)
    assert peer.conn.incomplete_since == START + 10 * TIMEOUT


def test_a_timeout_of_zero_disables_the_check(build):
    off = build(incomplete_command_timeout=0)
    on = build()
    unlimited = off.connect()
    control = on.connect()
    for peer in (unlimited, control):
        peer.send(HALF_INLINE)
    # one clock serves both servers. the control is the same command on the same clock under
    # a limit, so that a connection left open here is the zero's doing and not a sweep that
    # never ran
    off.clock.t = START + 10 ** 6
    off.sweep()
    on.sweep()
    assert control.conn.closed, "the control should be long past a %d second limit" % TIMEOUT
    assert not unlimited.conn.closed, "a limit of 0 means no limit, and nothing is closed for it"


# the deadline and the pause


def test_a_paused_connection_keeps_no_deadline_for_the_command_it_cannot_finish(build):
    # the server stopped reading this client, so the command it began cannot finish, and a
    # deadline that kept running would close the client for the backpressure applied to it.
    # the other side of that is that the sweep never sees a paused connection: this flag bounds
    # a command that was begun and not finished, and says nothing about a client that has sent
    # every request it means to and stopped reading, which holds no deadline at all
    rig = build(**PAUSING)
    peer = rig.connect()
    _pause_on_a_half_sent_command(peer)
    assert peer.conn.incomplete_since is None
    # a long time later, and with part of the queue taken but the rest still over the
    # high-water mark and then between the marks: reading is still off, and nothing is armed
    for drained in (len(ECHO_REPLY), 2 * len(ECHO_REPLY)):
        rig.clock.t += 10 * TIMEOUT
        peer.take(drained)
        assert _mask(peer) == WRITE, "reading resumed before the low-water mark"
        assert peer.conn.incomplete_since is None
        rig.sweep()
        assert not peer.conn.closed, "closed for a command the server itself had stopped reading"


def test_the_deadline_is_rearmed_from_the_moment_reading_resumes(build):
    rig = build(**PAUSING)
    peer = rig.connect()
    _pause_on_a_half_sent_command(peer)
    peer.take(len(ECHO_REPLY))
    peer.take(2 * len(ECHO_REPLY))
    assert len(peer.conn.write_buffer) == 2 * len(ECHO_REPLY), "the queue was meant to be between the marks"
    assert peer.conn.incomplete_since is None

    rig.clock.t = START + 10
    peer.take(len(ECHO_REPLY))
    assert len(peer.conn.write_buffer) == len(ECHO_REPLY), "the queue was meant to be at the low-water mark"
    assert _mask(peer) == READ | WRITE, "reading did not resume at the low-water mark"
    assert peer.conn.incomplete_since == START + 10, (
        "the command is outstanding again, and its deadline runs from the resume")

    # a flush that finds reading already on is not an edge, and arms nothing
    rig.clock.t = START + 12
    peer.take(len(ECHO_REPLY))
    assert not peer.conn.write_buffer
    assert peer.conn.incomplete_since == START + 10

    # counted from the resume and not from when the command began: past the limit measured from
    # the first byte, and the connection is still open
    rig.clock.t = START + TIMEOUT + 1
    rig.sweep()
    assert not peer.conn.closed, "the time spent paused was counted against the client"
    _assert_closed_only_past_the_timeout(rig, peer, START + 10)


def test_a_batch_that_pauses_the_connection_leaves_the_deadline_suspended(build):
    # the deadline is armed before the batch is dispatched, and dispatching can pause the
    # connection, which clears it. the clear has to be the last write: a deadline armed after
    # the dispatch would start running on a connection this server has just stopped reading.
    # the control is the same bytes on the same stalled socket with the pause switched off,
    # and it is the one holding a deadline
    rig = build(**PAUSING)
    unpaused_rig = build(write_buffer_high_water=0)
    paused = rig.connect()
    unpaused = unpaused_rig.connect()
    for peer in (paused, unpaused):
        peer.stall()
        peer.send(ECHO_ABCD * 5 + HALF_INLINE)
        assert peer.conn.has_incomplete_command
    assert _mask(unpaused) == READ | WRITE
    assert unpaused.conn.incomplete_since == START, "the control should hold a running deadline"
    assert _mask(paused) == WRITE
    assert paused.conn.incomplete_since is None, (
        "a connection the server stopped reading holds a running deadline")


def test_a_resume_arms_no_deadline_for_a_connection_holding_no_command(build):
    rig = build(**PAUSING)
    peer = rig.connect()
    peer.stall()
    peer.send(ECHO_ABCD * 5)
    assert _mask(peer) == WRITE, "five replies against a three-reply mark should have paused the connection"
    assert not peer.conn.has_incomplete_command
    rig.clock.t = START + 10
    peer.take(5 * len(ECHO_REPLY))
    assert not peer.conn.write_buffer
    assert _mask(peer) == READ, "reading did not resume once the queue emptied"
    assert peer.conn.incomplete_since is None, "a resume armed a deadline for a connection with nothing outstanding"
    rig.clock.t = START + 10 * TIMEOUT
    rig.sweep()
    assert not peer.conn.closed


# the sweep itself


def test_the_sweep_survives_closing_connections_out_of_the_set_it_is_walking(build):
    # runs the sweep's body and not the tick: iterating the set itself raises RuntimeError
    # the first time a close discards from it, and the tick's boundary would log that and
    # leave the second stale connection open, which this test would then report as an
    # assertion and not as the error that explains it
    rig = build()
    stale = [rig.connect(), rig.connect()]
    stale[0].send(HALF_INLINE)
    stale[1].send(HALF_MULTIBULK)
    rig.clock.t = START + 20
    recent = rig.connect()
    recent.send(HALF_INLINE)
    idle = rig.connect()
    rig.clock.t = START + TIMEOUT + 1
    rig.server._close_stalled_commands()
    assert all(peer.conn.closed for peer in stale), "both stale connections go in the one sweep"
    assert rig.server._connections == {recent.conn, idle.conn}
    assert not recent.conn.closed, "a command 11 seconds old is not past a %d second limit" % TIMEOUT
    assert not idle.conn.closed


def test_a_close_that_fails_does_not_end_the_sweep(build):
    # runs the sweep's body and not the tick, for the same reason: through the tick a close
    # that raised would be logged and the connections behind it left holding their commands,
    # which is a different failure from the one that matters here, the sweep ending early
    rig = build()
    peers = [rig.connect() for _ in range(3)]
    for peer in peers:
        peer.send(HALF_INLINE)
    rig.clock.t = START + TIMEOUT + 1
    real_close = rig.server._close
    failed = []

    def close_that_fails_first(conn):
        if not failed:
            failed.append(conn)
            raise OSError("the selector refused")
        real_close(conn)

    rig.server._close = close_that_fails_first
    rig.server._close_stalled_commands()
    assert len(failed) == 1, "no close failed, so this test did not reach what it is about"
    assert all(peer.conn.closed for peer in peers), (
        "the connections behind the one whose close failed were left open")
    assert rig.server._connections == set()


def test_a_pipeline_cut_mid_command_by_every_read_is_not_closed_while_commands_complete(build):
    # a client streaming commands whose every read ends partway through one never presents a
    # clean buffer. a deadline armed once, at the first read, and cleared only by a clean
    # buffer, would close it at the limit however quickly each command completed. a read that
    # completes a command means whatever tail is held is a new command, with a deadline of its
    # own. the first chunk is a command and five bytes, every chunk after it is fourteen
    # bytes, so every read ends five bytes into a command
    limit = 5
    reads = 20
    rig = build(incomplete_command_timeout=limit)
    peer = rig.connect()
    stream = PING * (reads + 2)
    cuts = [19 + 14 * i for i in range(reads)]
    chunks = [stream[start:end] for start, end in zip([0] + cuts, cuts)]
    assert len(chunks) == reads and all(chunk for chunk in chunks)
    assert all(end % len(PING) for end in cuts), "every read was meant to end inside a command"

    armed = []
    closed_after = None
    for number, chunk in enumerate(chunks, start=1):
        rig.clock.t = START + number
        peer.send(chunk)
        armed.append(peer.conn.incomplete_since)
        rig.sweep()
        if peer.conn.closed:
            closed_after = number
            break

    assert closed_after is None, (
        "closed after %d reads, %d seconds after the first byte, while every read completed a command"
        % (closed_after, closed_after - 1))
    assert armed == [START + number for number in range(1, reads + 1)], (
        "each read completed a command and left the start of another, and the deadline "
        "should be that one's: %r" % armed)
    assert peer.received() == PONG * reads, "every command in the stream was meant to be answered"
