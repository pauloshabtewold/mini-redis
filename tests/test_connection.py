import socket

import pytest

import resp
from connection import Connection, Role, unread_in_kernel
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


def test_unread_in_kernel_answers_what_the_peer_sent_and_nothing_was_read(pair):
    # asked rather than read, so the answer must not consume what it counts: the server asks this
    # of a connection it is about to close during a stop, and a question that took the bytes would
    # be a read the drain decided not to do
    conn, peer = pair
    assert conn.unread_in_kernel() == 0, "nothing has been sent yet"
    peer.sendall(b"*1\r\n$4\r\nPING\r\n")
    assert conn.unread_in_kernel() == 14
    assert conn.unread_in_kernel() == 14, "the question consumed what it counted"
    conn.receive()
    assert conn.unread_in_kernel() == 0, "the receive queue is empty once it has been read"
    assert bytes(conn.read_buffer) == b"*1\r\n$4\r\nPING\r\n"


def test_unread_in_kernel_answers_zero_for_a_closed_connection(pair):
    # the one caller is assembling a figure for a log line written from a finally clause, and it
    # asks every connection the drain disposed of, some of which are already closed. a descriptor
    # of -1 raises from the ioctl, and a report that raises on the way out is worse than a short one
    conn, peer = pair
    peer.sendall(b"PING\r\n")
    conn.close()
    assert conn.unread_in_kernel() == 0


def test_unread_in_kernel_answers_zero_for_a_socket_closed_behind_the_connections_back(pair):
    # the connection's closed flag is its own and says nothing about its socket: a socket object that
    # has given up its descriptor leaves closed False and answers -1 from fileno(), and what ioctl
    # raises for that is a ValueError and not the OSError a refused question raises. the drain asks
    # this of every connection it left open, from inside the finally clause that writes its one line,
    # so a raise here is the line not being written
    conn, peer = pair
    peer.sendall(b"PING\r\n")
    conn._sock.close()
    assert conn.closed is False, "the case under test needs a connection that does not know its socket is gone"
    assert conn.unread_in_kernel() == 0


def test_the_module_level_unread_in_kernel_answers_zero_for_a_closed_socket_object():
    # the function the server asks of a socket no Connection owns, the accept backlog's, and the one
    # the method above delegates to. the same ValueError as above, reached without a connection in
    # the way: a closed socket object answers -1 from fileno()
    a, b = socket.socketpair()
    try:
        b.sendall(b"PING\r\n")
        a.close()
        assert unread_in_kernel(a) == 0
    finally:
        a.close()
        b.close()


def test_the_module_level_unread_in_kernel_answers_zero_when_the_kernel_refuses_the_question():
    # the other family, which is an OSError and not the ValueError above: a descriptor number the
    # process holds nothing under is refused by the kernel with EBADF. a number this far up is never
    # open in a test process, so the refusal does not depend on what the rest of the suite left open
    class Unheld:
        def fileno(self):
            return 1 << 20

    assert unread_in_kernel(Unheld()) == 0


def test_unread_in_kernel_answers_a_queue_longer_than_a_sixteen_bit_count_holds(pair):
    # the answer is written into a buffer the call is handed, and CPython copies a mutable buffer
    # that small through its own and copies back only as many bytes as the buffer was long, so one
    # sized for a short is not refused: it truncates, to the low sixteen bits and signed. 140,000
    # reads back as 8,928, and a stop that had thrown away 40,000 bytes published a negative figure.
    # the two tests above send at most a few bytes and could not see any of it
    conn, peer = pair
    # a socketpair holds 8 KiB by default in either direction, so the queue could not get this long
    # without both ends being raised, which end's limit governs depending on the platform
    for sock in (conn._sock, peer):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 21)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 21)
    queued = 140_000
    # a timeout so that a kernel that takes less than this is a failure and not a send that never returns
    peer.settimeout(5)
    peer.sendall(b"x" * queued)
    assert conn.unread_in_kernel() == queued
    assert conn.unread_in_kernel() == queued, "the question consumed what it counted"
    assert unread_in_kernel(conn._sock) == queued


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
    # command into a quadratic number of parses; _parse_needed exists to make this O(1).
    # what is counted is parse_multibulk_header and parse_bulk_element, the two functions
    # take_commands() actually drives for this wire shape -- it never calls parse_command
    # for a multibulk, so a shim on parse_command would count zero calls no matter how
    # much rescanning happened, and a bound compared against zero would pass either way
    conn, _peer = pair
    header_calls = 0
    element_calls = 0
    real_parse_multibulk_header = resp.parse_multibulk_header
    real_parse_bulk_element = resp.parse_bulk_element

    def counting_parse_multibulk_header(buf, search_from=0, *, max_multibulk=0):
        nonlocal header_calls
        header_calls += 1
        return real_parse_multibulk_header(buf, search_from, max_multibulk=max_multibulk)

    def counting_parse_bulk_element(buf, search_from=1, *, max_value_size=0):
        nonlocal element_calls
        element_calls += 1
        return real_parse_bulk_element(buf, search_from, max_value_size=max_value_size)

    monkeypatch.setattr(resp, "parse_multibulk_header", counting_parse_multibulk_header)
    monkeypatch.setattr(resp, "parse_bulk_element", counting_parse_bulk_element)

    body = b"a" * 200_000
    wire = b"*1\r\n$%d\r\n" % len(body) + body + b"\r\n"

    chunk_size = 500
    invocations = 0
    commands = []
    for start in range(0, len(wire), chunk_size):
        conn.read_buffer.extend(wire[start:start + chunk_size])
        commands.extend(conn.take_commands())
        invocations += 1

    assert commands == [[body]]
    # the counter has to have moved: a shim wired to the wrong function leaves it at
    # zero, and an upper bound alone does not catch that -- <= 3 is satisfied by 0 too
    assert header_calls > 0 and element_calls > 0, (header_calls, element_calls)
    total_calls = header_calls + element_calls
    # the real assertion is the ratio: three parses total against `invocations` readable
    # events on a buffer that grew to 200 KB in 500-byte steps. a parser re-scanning from
    # byte zero on every event would make total_calls track invocations, not stay flat at 3
    assert total_calls <= 3, (total_calls, invocations)


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
        # the width of the count is what makes the header's own reset observable: at one
        # digit the position left behind lands on the next element's terminator anyway,
        # and only from two digits up does it point past that terminator, into the middle
        # of a header that starts at byte zero -- a bogus protocol error on a valid command
        (b"*10\r", b"\n" + b"$1\r\na\r\n" * 10, [[b"a"] * 10]),
        (b"*1\r\n$4\r", b"\nPING\r\n", [[b"PING"]]),
        (b"*2\r\n$4\r\nECHO\r\n$1\r", b"\nx\r\n", [[b"ECHO", b"x"]]),
        # the inline path resumes at the end rather than one byte short, because its
        # terminator is a single \n -- but the \r before it is still part of the line,
        # so a line split between the two has to come back as one command, not two
        (b"PING\r", b"\n", [[b"PING"]]),
        (b"ECHO hi\r", b"\nPING\r\n", [[b"ECHO", b"hi"], [b"PING"]]),
    ],
    ids=["multibulk-count", "multibulk-count-two-digits", "bulk-length", "second-element",
         "inline", "inline-then-more"],
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


def test_a_completed_element_clears_the_resume_position_for_the_next_one(pair):
    # _scan_from is only non-zero while a header's own terminator search is unfinished.
    # if that search resolves and the body completes in the same pass, the reset is the
    # only thing stopping a stale offset from being applied to the NEXT element's buffer,
    # where it points into the middle of a header that starts at byte zero. the symptom
    # is a bogus protocol error on a well-formed command, and a dropped connection
    conn, peer = pair
    peer.sendall(b"*2\r\n$1")           # element one's header arrives incomplete
    conn.receive()
    assert conn.take_commands() == []
    assert conn._scan_from > 0, "the unfinished header search must have left a position"

    peer.sendall(b"\r\na\r\n")          # its terminator and its body land together
    conn.receive()
    assert conn.take_commands() == []
    assert conn._scan_from == 0, "a completed element must not leave a resume position"

    peer.sendall(b"$1\r\nb\r\n")        # a short header for element two
    conn.receive()
    assert conn.take_commands() == [[b"a", b"b"]]


class CountingSocket:
    # a socket refuses attribute assignment, so counting close() takes a wrapper -- the
    # same shape RecordingSocket above uses to observe what recv() was asked for
    def __init__(self, sock):
        self._sock = sock
        self.closes = 0

    def fileno(self):
        return self._sock.fileno()

    def close(self):
        self.closes += 1
        self._sock.close()


def test_close_really_is_idempotent_not_merely_survivable():
    # socket.close() is itself safe to call twice, so asserting `closed is True` after
    # two calls passes with the guard deleted. what the guard is for is the descriptor:
    # ids are counted rather than taken from fileno() precisely because descriptors get
    # reused, and a second close on a stale Connection would shut a socket it never owned
    sock, peer = socket.socketpair()
    try:
        counting = CountingSocket(sock)
        conn = Connection(counting, ("127.0.0.1", 0))
        conn.close()
        conn.close()
        assert conn.closed is True
        assert counting.closes == 1, (
            "the second close reached the socket: %d calls" % counting.closes)
    finally:
        sock.close()
        peer.close()


def test_a_connection_holding_nothing_reports_no_incomplete_command(pair):
    conn, _peer = pair
    assert conn.has_incomplete_command is False
    # a readable event that delivers nothing changes nothing
    assert conn.receive() is True
    assert conn.take_commands() == []
    assert conn.has_incomplete_command is False


def test_a_partial_inline_line_reports_an_incomplete_command(pair):
    # the inline path keeps its bytes in the buffer and has no argv, so only the buffer clause sees it
    conn, peer = pair
    # a byte at a time, so the first reading is a one-byte buffer: a single byte is an incomplete command too
    for byte in (b"P", b"I", b"N"):
        peer.sendall(byte)
        conn.receive()
        assert conn.take_commands() == []
        assert conn._argv is None
        assert conn.has_incomplete_command is True, bytes(conn.read_buffer)


def test_a_partial_multibulk_header_reports_an_incomplete_command(pair):
    # no header has been consumed yet, so _argv is still None and the buffer is what holds the evidence
    conn, peer = pair
    for byte in (b"*", b"2", b"\r"):
        peer.sendall(byte)
        conn.receive()
        assert conn.take_commands() == []
        assert conn._argv is None
        assert conn.has_incomplete_command is True, bytes(conn.read_buffer)


def test_a_multibulk_awaiting_elements_with_an_empty_buffer_reports_an_incomplete_command(pair):
    # the header and the first of two elements are consumed and the buffer ends exactly there, so a
    # buffer-length check says nothing is owed while the connection is still waiting on a second element
    conn, peer = pair
    peer.sendall(b"*2\r\n$3\r\nGET\r\n")
    conn.receive()
    assert conn.take_commands() == []
    assert conn.read_buffer == bytearray(), "the case under test needs an empty buffer"
    assert conn._argv is not None, "the case under test needs a multibulk still owed elements"
    assert conn.has_incomplete_command is True
    peer.sendall(b"$3\r\nkey\r\n")
    conn.receive()
    assert conn.take_commands() == [[b"GET", b"key"]]
    assert conn.has_incomplete_command is False


def test_a_completed_batch_reports_no_incomplete_command(pair):
    conn, peer = pair
    peer.sendall(b"*1\r\n$4\r\nPING\r\n" + b"PING\r\n" + b"\r\n*0\r\n")
    conn.receive()
    assert conn.take_commands() == [[b"PING"], [b"PING"]]
    assert conn.read_buffer == bytearray()
    assert conn.has_incomplete_command is False


def test_incomplete_since_is_none_on_a_new_connection_and_is_never_written_by_connection(pair):
    # the slot belongs to the server, which owns the clock: whatever this class is asked to do, a
    # reading it wrote itself would arm a deadline the server never asked for
    conn, peer = pair
    assert conn.incomplete_since is None
    peer.sendall(b"*2\r\n$3\r\nGET\r\n")
    conn.receive()
    conn.take_commands()
    assert conn.incomplete_since is None
    peer.sendall(b"$3\r\nkey\r\n")
    conn.receive()
    conn.take_commands()
    assert conn.incomplete_since is None
    peer.sendall(b"*abc\r\n")
    conn.receive()
    with pytest.raises(resp.ProtocolError):
        conn.take_commands()
    assert conn.incomplete_since is None
    conn.queue(b"+OK\r\n")
    assert conn.flush() is True
    assert conn.incomplete_since is None
    conn.close()
    assert conn.incomplete_since is None


def test_a_multibulk_awaiting_elements_reports_the_bytes_it_has_consumed_with_an_empty_buffer(pair):
    # take_commands deletes the bytes it parses, so the read buffer alone says a half-received
    # command holds nothing at exactly the points where has_incomplete_command needs _argv as well.
    # the shutdown drain reports the bytes it throws away, and these are among them: the count is
    # kept because nothing afterwards can recover it from the buffer
    conn, peer = pair
    stages = [
        (b"*3\r\n", 4),                 # the header alone
        (b"$3\r\nSET\r\n", 13),         # and the first element
        (b"$1\r\nk\r\n", 20),           # and the second, the buffer ending exactly there each time
    ]
    for chunk, held in stages:
        peer.sendall(chunk)
        conn.receive()
        assert conn.take_commands() == []
        assert conn.read_buffer == bytearray(), "the case under test needs an empty buffer"
        assert conn.consumed_for_incomplete_command == held
    peer.sendall(b"$1\r\nv\r\n")
    conn.receive()
    assert conn.take_commands() == [[b"SET", b"k", b"v"]]
    assert conn.consumed_for_incomplete_command == 0, "a completed command holds nothing"


def test_what_a_connection_holds_for_an_incomplete_command_is_accounted_at_every_byte(pair):
    # the count and the buffer together are every byte of the command that has not completed, and
    # that is asserted after each byte of a stream rather than at chosen points: the stream mixes a
    # multibulk, an inline line, an empty line, an empty multibulk and a second multibulk, so every
    # branch of take_commands that touches the count is crossed, and a figure that drifts at any
    # byte is reported at that byte. the sum is what the drain publishes, and it is only right if
    # a byte is in one of the two and never in both or neither
    conn, peer = pair
    pieces = [
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
        b"PING\r\n",
        b"\r\n",
        b"*0\r\n",
        b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n",
    ]
    stream = b"".join(pieces)
    # the offsets at which a step has finished and nothing is held
    boundaries = {0}
    total = 0
    for piece in pieces:
        total += len(piece)
        boundaries.add(total)
    for fed in range(1, len(stream) + 1):
        peer.sendall(stream[fed - 1:fed])
        conn.receive()
        conn.take_commands()
        held = fed - max(boundary for boundary in boundaries if boundary <= fed)
        accounted = conn.consumed_for_incomplete_command + len(conn.read_buffer)
        assert accounted == held, (fed, accounted, held)
    assert conn.consumed_for_incomplete_command == 0 and conn.read_buffer == bytearray()


@pytest.mark.parametrize("step", [b"PING\r\n", b"\r\n", b"*0\r\n"], ids=["inline command", "empty line", "empty multibulk"])
def test_a_step_that_leaves_no_command_outstanding_leaves_the_count_at_zero(pair, step):
    # the three ways a step ends with nothing owed: an inline command, an empty line, and a header that
    # declares no elements. none can be reached with anything held, so the count is put there by hand,
    # which is the only way to see that each of them resets it rather than relying on the reset that
    # completing a multibulk makes: a count that drifted up would put bytes in the drain's figure that
    # were never on the wire
    conn, peer = pair
    conn.consumed_for_incomplete_command = 99
    peer.sendall(step)
    conn.receive()
    conn.take_commands()
    assert conn.consumed_for_incomplete_command == 0
