import selectors
import socket

import pytest

from connection import Connection
from server import Server, build_arg_parser


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


def test_the_output_buffer_limit_is_off_by_default():
    # the reference leaves this off for an ordinary client, and a server that disconnects
    # where the reference does not is a divergence nobody asked for
    assert build_arg_parser().parse_args([]).output_buffer_limit == 0
    assert Server(0).output_buffer_limit == 0


def test_no_limit_lets_a_queued_reply_grow(make_connection):
    server, conn = make_connection([BlockingIOError])
    conn.queue(b"x" * 10_000)
    server._flush(conn)
    assert not conn.closed, "an unset limit must not close anything"
    assert len(conn.write_buffer) == 10_000


def test_a_connection_over_the_limit_is_closed_not_slowed(make_connection):
    # closing rather than throttling: a client that writes every request before reading
    # a reply blocks in send() the moment the server stops reading it, waiting for room
    # only its own reading would create, so refusing to read is a deadlock, not a brake
    # two refusals: the flush that trips the limit, and the best-effort flush _close
    # makes on the way out to hand the peer whatever the kernel will still take
    server, conn = make_connection([BlockingIOError, BlockingIOError])
    server.output_buffer_limit = 4096
    conn.queue(b"x" * 5000)
    server._flush(conn)
    assert conn.closed, "a connection past the limit must be closed"


def test_the_limit_is_measured_after_the_send_not_before(make_connection):
    # what matters is what the kernel would not take, not what was queued a moment ago:
    # a reply larger than the limit that leaves in one send() costs nothing to hold
    server, conn = make_connection([5000])
    server.output_buffer_limit = 4096
    conn.queue(b"x" * 5000)
    server._flush(conn)
    assert not conn.closed, "a buffer the kernel accepted whole must not trip the limit"
    assert conn.write_buffer == bytearray()


def test_a_buffer_exactly_at_the_limit_is_kept(make_connection):
    server, conn = make_connection([BlockingIOError])
    server.output_buffer_limit = 5000
    conn.queue(b"x" * 5000)
    server._flush(conn)
    assert not conn.closed, "the limit is a ceiling to exceed, not to reach"


@pytest.mark.parametrize("value", ["-1", "-4096"])
def test_the_parser_refuses_a_negative_output_buffer_limit(value):
    # `limit and len(buf) > limit` reads any non-zero value as enabled, and every buffer
    # length exceeds a negative number -- including zero. left unchecked, --output-buffer-
    # limit -1 prints a healthy startup line and then closes every connection after its
    # first reply. -1 is a conventional spelling of "unlimited", so it is the likeliest
    # value to be typed here by someone reaching for exactly the opposite behaviour
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args(["--output-buffer-limit", value])


def test_the_server_itself_refuses_a_negative_output_buffer_limit():
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
    server.output_buffer_limit = 4 * len(ECHO_REPLY)
    _dispatch_echoes(server, conn, 4)
    assert not conn.closed, "four replies land exactly on the limit and must be kept"
    assert len(conn.write_buffer) == server.output_buffer_limit


def test_the_in_loop_limit_closes_one_reply_past_it(make_connection):
    server, conn = make_connection([BlockingIOError] * 20)
    server.output_buffer_limit = 4 * len(ECHO_REPLY)
    _dispatch_echoes(server, conn, 5)
    assert conn.closed, "the fifth reply carries the buffer past the limit"


@pytest.mark.parametrize("flag, value", [
    ("--port", "abc"), ("--port", ""), ("--port", "1.5"),
    ("--output-buffer-limit", "abc"), ("--output-buffer-limit", "1e6"),
])
def test_a_non_numeric_flag_names_the_flag_rather_than_the_validator(capsys, flag, value):
    # argparse builds its own message from type='s __name__, which for these is a private
    # function -- "invalid _port value: 'abc'" puts an internal identifier in front of a
    # user who typed a bad port
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args([flag, value])
    message = capsys.readouterr().err
    assert flag in message, message
    assert "_port" not in message and "_output_buffer_limit" not in message, message
