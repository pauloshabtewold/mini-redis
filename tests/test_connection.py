import socket

import pytest

import resp
from connection import Connection, Role
from tests.int_ceiling import NO_CEILING_REASON, NO_CONVERSION_CEILING, OVERSIZED_DIGIT_RUN


def _deliver(conn, peer, payload):
    # drains as it sends: a socketpair holds 8 KiB unread, and a header sized from the
    # interpreter's conversion ceiling can be larger than that, which blocks sendall() forever
    view = memoryview(payload)
    while view:
        sent = peer.send(view[:4096])
        view = view[sent:]
        conn.receive()


class RecordingSocket:
    # a socket carries no attribute of its own, so the size receive() asks the kernel for is
    # observable only through a wrapper standing in front of it
    def __init__(self, sock, asked):
        self._sock = sock
        self._asked = asked

    def recv(self, bufsize):
        self._asked.append(bufsize)
        return self._sock.recv(bufsize)


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


def test_one_readable_event_reads_at_most_sixty_four_kibibytes(pair):
    # one recv() per readable event, so this size is what the loop allocates for every connection
    # it services in a pass -- a megabyte here is a megabyte of churn per client per poll
    conn, peer = pair
    asked = []
    conn._sock = RecordingSocket(conn._sock, asked)
    peer.sendall(b"PING\r\n")
    assert conn.receive() is True
    assert asked == [65536], asked


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


def test_every_role_is_a_distinct_value():
    # StrEnum makes a duplicated value an alias rather than an error, so a FOLLOWER that collides
    # with CLIENT reads as an ordinary client everywhere and nothing raises at the collision
    assert len(list(Role)) == 3
    assert len({role.value for role in Role}) == 3


def test_fileno_is_the_underlying_descriptor(pair):
    # the selector registers the Connection itself and watches whatever number this returns, so
    # any other descriptor has it waiting on readiness that belongs to something else entirely
    conn, _peer = pair
    assert conn.fileno() == conn._sock.fileno()


def test_take_commands_drains_a_pipelined_buffer(pair):
    conn, peer = pair
    peer.sendall(b"*1\r\n$4\r\nPING\r\n" + b"PING\r\n")
    conn.receive()
    assert conn.take_commands() == [[b"PING"], [b"PING"]]
    assert conn.read_buffer == bytearray()


def test_take_commands_yields_nothing_until_a_partial_command_completes(pair):
    # the contract is that a partial command produces no command and loses no argument, not that
    # its bytes stay in the buffer: a completed element is consumed as it arrives, by design
    conn, peer = pair
    peer.sendall(b"*2\r\n$3\r\nfoo\r\n$3\r\nba")
    conn.receive()
    assert conn.take_commands() == []
    peer.sendall(b"r\r\n")
    conn.receive()
    assert conn.take_commands() == [[b"foo", b"bar"]]
    assert conn.read_buffer == bytearray()


def test_take_commands_consumes_input_that_yields_no_command(pair):
    conn, peer = pair
    # a blank inline line and a *0 header consume bytes and produce nothing; looping on argv instead of consumed spins forever here
    peer.sendall(b"\r\n*0\r\nPING\r\n")
    conn.receive()
    assert conn.take_commands() == [[b"PING"]]
    assert conn.read_buffer == bytearray()


def test_take_commands_does_not_reparse_a_large_bulk_from_byte_zero(pair, monkeypatch):
    # a growing buffer re-parsed from byte zero on every readable event turns one large
    # command into a quadratic number of parses; _parse_needed exists to make this O(1)
    conn, _peer = pair
    calls = 0
    real_parse_command = resp.parse_command

    def counting_parse_command(buf):
        nonlocal calls
        calls += 1
        return real_parse_command(buf)

    monkeypatch.setattr(resp, "parse_command", counting_parse_command)

    body = b"a" * 200_000
    wire = b"*1\r\n$%d\r\n" % len(body) + body + b"\r\n"

    chunk_size = 500
    commands = []
    for start in range(0, len(wire), chunk_size):
        conn.read_buffer.extend(wire[start:start + chunk_size])
        commands.extend(conn.take_commands())

    assert commands == [[body]]
    assert calls <= 3, calls


def test_take_commands_propagates_protocol_error(pair):
    conn, peer = pair
    peer.sendall(b"*abc\r\n")
    conn.receive()
    with pytest.raises(resp.ProtocolError):
        conn.take_commands()


@pytest.mark.skipif(NO_CONVERSION_CEILING, reason=NO_CEILING_REASON)
def test_oversized_length_header_is_a_protocol_error_not_a_crash(pair):
    # isdigit() passes this and int() refuses it, so an unguarded parser raises ValueError here and drops every connection instead of only this one
    conn, peer = pair
    _deliver(conn, peer, b"*" + OVERSIZED_DIGIT_RUN + b"\r\n")
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


def test_take_commands_locates_each_multibulk_element_once(pair, monkeypatch):
    # a whole-command re-parse locates element k by re-walking elements 1..k-1, so one command
    # delivered in fragments costs a length parse per element per element rather than one each
    conn, _peer = pair
    calls = 0
    real_parse_length = resp._parse_length

    def counting_parse_length(field, error_message):
        nonlocal calls
        calls += 1
        return real_parse_length(field, error_message)

    monkeypatch.setattr(resp, "_parse_length", counting_parse_length)

    count = 400
    wire = b"*%d\r\n" % count
    for index in range(count):
        argument = b"arg%d" % index
        wire += b"$%d\r\n%s\r\n" % (len(argument), argument)

    commands = []
    for start in range(len(wire)):
        conn.read_buffer.extend(wire[start:start + 1])
        commands.extend(conn.take_commands())

    assert len(commands) == 1 and len(commands[0]) == count
    # one header plus one per element; re-walking would make this quadratic in count
    assert calls <= 4 * count, calls


def test_an_incomplete_inline_command_is_held_until_its_newline_arrives(pair):
    # the inline path reports no bound -- a partial line says nothing about when a newline comes --
    # so the short-circuit must not strand it: a client typing at a terminal sits here between keys
    conn, peer = pair
    peer.sendall(b"PIN")
    conn.receive()
    assert conn.take_commands() == []
    assert conn._parse_needed == 0, "a partial inline line bounds nothing"
    peer.sendall(b"G\r\n")
    conn.receive()
    assert conn.take_commands() == [[b"PING"]]
    assert conn.read_buffer == bytearray()


class ScanCountingBuffer(bytearray):
    # a bytearray whose find() reports the span it was asked to search, because the cost
    # this pins is not a call count but how much of the buffer each call walks
    def __init__(self, *args):
        super().__init__(*args)
        self.scanned = 0

    def find(self, *args):
        start = args[1] if len(args) > 1 else 0
        self.scanned += max(0, len(self) - start)
        return super().find(*args)


@pytest.mark.parametrize(
    "opening",
    [
        b"",                      # inline: no framing at all
        b"*",                     # a multibulk count that never ends
        b"*2\r\n$4\r\nECHO\r\n$",  # a bulk length that never ends
    ],
    ids=["inline", "multibulk-count", "bulk-length"],
)
def test_a_header_arriving_in_pieces_is_scanned_once_not_once_per_read(opening):
    # a header line's length is declared nowhere, so _parse_needed can never bound one.
    # without a resume position the search restarts at byte zero on every readable event,
    # which is quadratic in what one connection has sent: 300 reads of a 1 KiB chunk walk
    # ~150x the bytes received, and one connection can hold the whole single-threaded loop
    sock, peer = socket.socketpair()
    try:
        conn = Connection(sock, ("127.0.0.1", 0))
        conn.read_buffer = ScanCountingBuffer(opening)
        chunks, chunk = 300, b"1" * 1024
        for _ in range(chunks):
            conn.read_buffer.extend(chunk)
            assert conn.take_commands() == []
        received = len(conn.read_buffer)
        assert conn.read_buffer.scanned <= 2 * received, (
            "scanned %d bytes to receive %d -- the search is restarting at byte zero"
            % (conn.read_buffer.scanned, received)
        )
    finally:
        sock.close()
        peer.close()


@pytest.mark.parametrize(
    "first, second, expected",
    [
        (b"*1\r", b"\n$4\r\nPING\r\n", [[b"PING"]]),
        (b"*1\r\n$4\r", b"\nPING\r\n", [[b"PING"]]),
        (b"*2\r\n$4\r\nECHO\r\n$1\r", b"\nx\r\n", [[b"ECHO", b"x"]]),
        # the inline path resumes at the end rather than one byte short, because its
        # terminator is a single \n -- but the \r before it is still part of the line,
        # so a line split between the two has to come back as one command, not two
        (b"PING\r", b"\n", [[b"PING"]]),
        (b"ECHO hi\r", b"\nPING\r\n", [[b"ECHO", b"hi"], [b"PING"]]),
    ],
    ids=["multibulk-count", "bulk-length", "second-element", "inline", "inline-then-more"],
)
def test_a_header_crlf_split_across_two_reads_is_still_found(pair, first, second, expected):
    # the resume position has to stop one byte short of the end: a \r already buffered
    # pairs with a \n that has not arrived, and resuming at the end steps over that pair
    # and the header is never located -- the connection then hangs with no error anywhere
    conn, peer = pair
    peer.sendall(first)
    conn.receive()
    assert conn.take_commands() == []
    peer.sendall(second)
    conn.receive()
    assert conn.take_commands() == expected
    assert conn.read_buffer == bytearray()
