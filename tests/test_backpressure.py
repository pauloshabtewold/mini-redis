"""The write-side water marks, observed through the selector and through a real socket.

A connection owed more than the high-water mark stops being read, and starts being read
again once what it is owed has fallen to the low-water mark; between the two marks nothing
changes. The selector's own mask is the one record of interest there is, so that is what
most of these read, through a real listener and a real client that does not read. The
kernel is what makes them hard: loopback absorbs a megabyte or more of replies before a
send() reports anything, and a reply queue is judged after the send, so a reply a socket
buffer can hold is never over any limit. Every in-process test here either queues far more
than the kernel will take, or saturates it first and then chooses the marks around the queue
it is left with, which is the only way to put a queue exactly on a boundary without a
stand-in for the socket.
"""

import contextlib
import select
import selectors
import socket
import threading
import time

from server import build_arg_parser
from tests.conftest import launch_server, stop_server
from tests.test_server_lifecycle import listening, pump

READ = selectors.EVENT_READ
WRITE = selectors.EVENT_WRITE

# a value of this size is a reply of this size, far enough above a send() that a pipeline of
# them leaves the kernel full and the rest in the server's own buffer
VALUE = bytes(range(256)) * 256
REPLY = b"$%d\r\n%s\r\n" % (len(VALUE), VALUE)
GET = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
PING = b"*1\r\n$4\r\nPING\r\n"
PONG = b"+PONG\r\n"

# 200 replies of 64 KiB is 12.8 MB, past what any socket buffer on a loopback pair holds and
# well under the 32 MiB default limit, so a batch this size pauses a connection and does not
# close it
BURST = 200

# a bound on every wait that depends on another party, so a stalled test fails instead of
# hanging the run
PATIENCE_SECONDS = 30


def mask_of(server, conn):
    # the selector's own mask. get_key raises for a connection the selector does not hold, so
    # a connection that has dropped out of it cannot be mistaken for one that is merely quiet
    return server._loop._selector.get_key(conn).events


def mask_while_owing(server, conn):
    try:
        return mask_of(server, conn)
    except (KeyError, ValueError):
        raise AssertionError(
            "the connection is not registered with the selector while it owes %d bytes"
            % len(conn.write_buffer)
        ) from None


def assert_same_stream(got, want):
    # a mismatch between megabytes of replies reports where it begins and not the bytes
    if got == want:
        return
    first = next((i for i, (a, b) in enumerate(zip(got, want)) if a != b), min(len(got), len(want)))
    raise AssertionError(
        "received %d bytes against %d expected; the first difference is at offset %d"
        % (len(got), len(want), first)
    )


class SelectorSpy:
    """Records what the loop asks of the selector, and passes every call through to it.

    Only the outermost call is recorded: the base class implements modify() as an
    unregister() and a register(), and those two say nothing about what the loop asked for.
    """

    def __init__(self, selector):
        self._selector = selector
        self._depth = 0
        self.calls = []

    def _wrap(self, name, real):
        def spy(*args, **kwargs):
            if self._depth == 0:
                self.calls.append((name, args[1] if name != "unregister" else None))
            self._depth += 1
            try:
                return real(*args, **kwargs)
            finally:
                self._depth -= 1

        return spy

    def __enter__(self):
        for name in ("register", "modify", "unregister"):
            setattr(self._selector, name, self._wrap(name, getattr(self._selector, name)))
        return self

    def __exit__(self, *exc_info):
        for name in ("register", "modify", "unregister"):
            delattr(self._selector, name)

    @property
    def masks_asked_for(self):
        return [events for _name, events in self.calls if events is not None]


def take(client, limit=1 << 20):
    # whatever the client's own kernel holds right now, at most limit bytes, and never a wait
    previous = client.gettimeout()
    client.settimeout(0)
    try:
        return client.recv(limit)
    except BlockingIOError:
        return b""
    finally:
        client.settimeout(previous)


def pump_until(server, condition, what, patience=PATIENCE_SECONDS):
    deadline = time.monotonic() + patience
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("gave up waiting for %s" % what)
        pump(server, times=1)


def receive(server, client, wanted, step=1 << 18):
    # the client reads a step at a time and the server runs between steps, which is what a
    # single thread has to do to be both ends of one connection
    received = bytearray()
    deadline = time.monotonic() + PATIENCE_SECONDS
    while len(received) < wanted:
        if time.monotonic() > deadline:
            raise AssertionError("received %d of %d bytes" % (len(received), wanted))
        received.extend(take(client, step))
        pump(server, times=1)
    return bytes(received)


def read_to_the_end(client):
    client.settimeout(PATIENCE_SECONDS)
    received = bytearray()
    try:
        while True:
            chunk = client.recv(1 << 20)
            if not chunk:
                break
            received.extend(chunk)
    except ConnectionResetError:
        pass
    return bytes(received)


def store_value(server, client, value, key=b"k"):
    client.sendall(
        b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n" % (len(key), key, len(value), value)
    )
    assert receive(server, client, 5) == b"+OK\r\n"


def unread_in_the_kernel(conn):
    # what the server's own socket has been sent and has not read, looked at without taking it
    try:
        return conn._sock.recv(1 << 20, socket.MSG_PEEK)
    except BlockingIOError:
        return b""


def owed_once_quiet(server, conn):
    # the length of the write buffer once two passes in a row have left it where it was
    previous = None
    for _ in range(200):
        pump(server, times=2)
        now = len(conn.write_buffer)
        if now == previous:
            return now
        previous = now
    raise AssertionError("the write buffer never stopped moving")


def fill_the_kernel(server, conn):
    """Queue and send until the kernel takes nothing more, and return what is left queued.

    Nobody reads the other end, so what remains in the write buffer is what the kernel
    refused, and it stays exactly that length for as long as the peer reads nothing. Both
    controls are switched off first, so nothing this does is itself paused or closed.
    """
    server.write_buffer_limit = 0
    server.write_buffer_high_water = 0
    chunk = b"x" * 65536
    refused_in_a_row = 0
    for _ in range(4096):
        before = len(conn.write_buffer)
        conn.queue(chunk)
        server._flush(conn)
        if len(conn.write_buffer) - before == len(chunk):
            refused_in_a_row += 1
            if refused_in_a_row == 25:
                return len(conn.write_buffer)
            time.sleep(0.004)
        else:
            refused_in_a_row = 0
    raise AssertionError("the kernel never stopped taking bytes")


def judge(server, conn, queued, high, low):
    """Put the marks where the test says and let _flush decide, with the queue unchanged.

    The queue is the premise: a flush that sent anything would have moved it, and the
    decision under test would no longer be about the length the test named.
    """
    server.write_buffer_high_water = high
    server.write_buffer_low_water = low
    server._flush(conn)
    assert len(conn.write_buffer) == queued, "the kernel took bytes this test assumed it would refuse"
    return mask_of(server, conn)


@contextlib.contextmanager
def serving(**settings):
    """A listener, one connection that has been accepted, and a client that is not reading."""
    with listening() as (server, connect, _listener):
        for name, setting in settings.items():
            setattr(server, name, setting)
        client = connect()
        # a send that cannot complete fails the test instead of parking it
        client.settimeout(PATIENCE_SECONDS)
        pump(server)
        conn, = server._connections
        yield server, client, conn, connect


# --- the pause ----------------------------------------------------------------------------


def test_a_queue_above_the_high_water_mark_stops_the_reading():
    with serving() as (server, client, conn, _connect):
        store_value(server, client, VALUE)
        assert mask_of(server, conn) == READ, "an idle connection is read and has nothing to write"

        client.sendall(GET * BURST)
        pump(server)

        assert len(conn.write_buffer) > server.write_buffer_high_water, (
            "the kernel took more than this test assumed, so nothing is over the mark")
        assert mask_of(server, conn) == WRITE, (
            "past the high-water mark the connection is not read, and it is still written to")
        assert not conn.closed and conn in server._connections, "pausing a connection is not closing it"
        assert server.connected_clients == 1


def test_the_pause_starts_one_byte_above_the_high_water_mark_and_not_at_it():
    with serving() as (server, client, conn, _connect):
        queued = fill_the_kernel(server, conn)
        assert mask_of(server, conn) == READ | WRITE

        assert judge(server, conn, queued, high=queued, low=queued - 1) == READ | WRITE, (
            "a queue exactly at the mark is not above it")
        assert judge(server, conn, queued, high=queued - 1, low=queued - 2) == WRITE, (
            "one byte above the mark stops the reading")


# --- the resume ---------------------------------------------------------------------------


def test_reading_resumes_when_the_queue_reaches_the_low_water_mark_and_not_one_byte_above_it():
    with serving() as (server, client, conn, _connect):
        queued = fill_the_kernel(server, conn)
        assert judge(server, conn, queued, high=queued - 1, low=queued - 2) == WRITE

        assert judge(server, conn, queued, high=queued, low=queued - 1) == WRITE, (
            "a queue one byte above the low-water mark has not fallen to it")
        assert judge(server, conn, queued, high=queued + 1, low=queued) == READ | WRITE, (
            "a queue exactly at the low-water mark has")


def test_reading_resumes_when_the_queue_is_under_the_low_water_mark():
    with serving() as (server, client, conn, _connect):
        queued = fill_the_kernel(server, conn)
        assert judge(server, conn, queued, high=queued - 1, low=queued - 2) == WRITE

        assert judge(server, conn, queued, high=queued + 10, low=queued + 5) == READ | WRITE, (
            "a queue under the low-water mark resumes reading, whether or not it landed on it")


def test_between_the_marks_nothing_changes_in_either_direction():
    with serving() as (server, client, conn, _connect):
        queued = fill_the_kernel(server, conn)
        assert mask_of(server, conn) == READ | WRITE
        selector = server._loop._selector

        with SelectorSpy(selector) as reading_in_the_band:
            assert judge(server, conn, queued, high=queued + 10, low=queued - 10) == READ | WRITE
        assert reading_in_the_band.calls == [], (
            "a connection being read was touched inside the band: %r" % reading_in_the_band.calls)

        # the control for the two empty lists: the same spy sees the one change a pause is
        with SelectorSpy(selector) as pausing:
            assert judge(server, conn, queued, high=queued - 1, low=queued - 10) == WRITE
        assert pausing.calls == [("modify", WRITE)], pausing.calls

        with SelectorSpy(selector) as paused_in_the_band:
            assert judge(server, conn, queued, high=queued + 10, low=queued - 10) == WRITE
            assert judge(server, conn, queued, high=queued, low=queued - 1) == WRITE, (
                "the top of the band is inside it")
        assert paused_in_the_band.calls == [], (
            "a paused connection was touched inside the band: %r" % paused_in_the_band.calls)


# --- never a mask of nothing --------------------------------------------------------------


def test_a_full_pause_and_resume_never_asks_the_selector_for_a_mask_of_nothing():
    # the property is that the selector is never asked for an empty mask, which kqueue answers
    # with an error and a dropped registration, and that a connection owing bytes is registered
    # every time the loop looks. it is not that every call is a modify: a pause that begins
    # from a connection registered for reading alone drops the one bit it had, leaving nothing,
    # which the applier spells as an unregister and a register in the same flush. whether a run
    # takes that route or modifies a registration that already has write interest depends on how
    # much the kernel had room for at the first send that overflowed, so the calls are checked
    # for what holds either way
    with serving() as (server, client, conn, _connect):
        store_value(server, client, VALUE)
        total = BURST * len(REPLY)
        reading = True
        history = []
        received = bytearray()

        with SelectorSpy(server._loop._selector) as spy:
            client.sendall(GET * BURST)
            deadline = time.monotonic() + PATIENCE_SECONDS
            while True:
                pump(server, times=1)
                queued = len(conn.write_buffer)
                events = mask_while_owing(server, conn)
                assert events != 0
                # the oracle is the marks and nothing else: above the high mark the reading
                # is off, at or under the low mark it is on, and in between it is whatever it
                # last was. write interest is exactly whether anything is owed
                high, low = server.write_buffer_high_water, server.write_buffer_low_water
                if queued > high:
                    assert not events & READ, "%d bytes owed and still being read" % queued
                elif queued <= low:
                    assert events & READ, "%d bytes owed, under the low mark, and not being read" % queued
                else:
                    assert bool(events & READ) == reading, "the registration changed inside the band"
                assert bool(events & WRITE) == bool(queued)
                reading = bool(events & READ)
                history.append(reading)
                if len(received) == total and not queued:
                    break
                assert time.monotonic() < deadline, "received %d of %d bytes" % (len(received), total)
                received.extend(take(client, 1 << 18))

        assert history[0] is False, "the burst never paused the connection, so the cycle exercised nothing"
        assert history[-1] is True
        assert_same_stream(bytes(received), REPLY * BURST)
        assert mask_of(server, conn) == READ
        assert spy.calls, "the cycle never touched the selector, so it exercised nothing"
        assert 0 not in spy.masks_asked_for, "an empty mask was asked of the selector: %r" % spy.calls
        # an unregister is only ever the first half of a change of mask, and the register that
        # completes it follows at once: the connection does not leave the selector and stay out
        for call, following in zip(spy.calls, spy.calls[1:] + [None]):
            if call[0] == "unregister":
                assert following is not None and following[0] == "register" and following[1], (
                    "the connection left the selector on the way: %r" % spy.calls)


def test_a_pause_from_a_registration_for_reading_alone_never_asks_for_a_mask_of_nothing():
    # the common way a connection is paused, and the one the full cycle above reaches only when
    # the kernel had little room at the first send that overflowed: one reply far larger than
    # the mark, queued for a connection that is registered for reading and nothing else. the
    # pause drops its only bit, which is a mask of nothing, and what the selector is shown
    # instead is the connection leaving and coming back for writing within the one flush
    with serving() as (server, client, conn, _connect):
        server.write_buffer_limit = 0
        assert mask_of(server, conn) == READ, "the premise is a connection registered for reading alone"
        conn.queue(b"x" * (24 * 1024 * 1024))

        with SelectorSpy(server._loop._selector) as spy:
            server._flush(conn)

        assert len(conn.write_buffer) > server.write_buffer_high_water, (
            "the kernel took more than this test assumed, so nothing is over the mark")
        assert spy.calls, "the flush never touched the selector, so it paused nothing"
        assert 0 not in spy.masks_asked_for, "an empty mask was asked of the selector: %r" % spy.calls
        assert mask_while_owing(server, conn) == WRITE, (
            "a paused connection owing bytes is written to and not read")


def test_a_paused_connection_that_empties_in_one_send_is_never_unregistered_on_the_way():
    # with the low-water mark at 0 a paused connection resumes only when its queue is empty,
    # so the send that empties it is always made by a paused connection, and the one flush
    # that follows has to restore its reading before it drops its writing. in the other order
    # the connection wants neither for the length of one call, which the selector can only
    # express by forgetting it
    with serving(write_buffer_low_water=0) as (server, client, conn, _connect):
        store_value(server, client, VALUE)
        total = BURST * len(REPLY)
        client.sendall(GET * BURST)
        pump(server)
        assert mask_of(server, conn) == WRITE

        received = bytearray()
        deadline = time.monotonic() + PATIENCE_SECONDS
        with SelectorSpy(server._loop._selector) as spy:
            while len(received) < total:
                assert time.monotonic() < deadline, "received %d of %d bytes" % (len(received), total)
                received.extend(take(client))
                pump(server, times=1)
                events = mask_while_owing(server, conn)
                owed = len(conn.write_buffer)
                assert events == (WRITE if owed else READ), "%d bytes owed and the mask is %d" % (owed, events)

        assert_same_stream(bytes(received), REPLY * BURST)
        assert spy.calls == [("modify", READ | WRITE), ("modify", READ)], spy.calls


# --- what a pause leaves where it was -----------------------------------------------------


def test_a_paused_connections_requests_wait_in_the_kernel_and_are_answered_after_the_resume():
    with serving() as (server, client, conn, _connect):
        store_value(server, client, VALUE)
        client.sendall(GET * BURST)
        pump(server)
        assert mask_of(server, conn) == WRITE

        client.sendall(PING)
        pump_until(
            server, lambda: unread_in_the_kernel(conn) == PING,
            "the request to reach the server's kernel", patience=5)
        pump(server, times=10)

        assert unread_in_the_kernel(conn) == PING, (
            "the request is not where a paused connection leaves it, unread in the kernel")
        assert not conn.read_buffer, "a paused connection was read"

        received = receive(server, client, BURST * len(REPLY) + len(PONG))
        assert_same_stream(received, REPLY * BURST + PONG)
        assert mask_of(server, conn) == READ


def test_a_client_that_never_reads_holds_the_server_to_the_batch_that_paused_it():
    with serving() as (server, client, conn, _connect):
        store_value(server, client, VALUE)
        client.sendall(GET * BURST)
        owed = owed_once_quiet(server, conn)
        assert mask_of(server, conn) == WRITE

        # twenty more bursts of the same size: dispatched, each would queue another 12.8 MB
        more = GET * BURST * 20
        client.sendall(more)
        pump_until(
            server, lambda: len(unread_in_the_kernel(conn)) == len(more),
            "the requests sent after the pause to reach the server's kernel", patience=5)
        pump(server, times=10)

        now = len(conn.write_buffer)
        assert now <= owed, "the queue grew from %d to %d while the connection was paused" % (owed, now)
        assert not conn.read_buffer
        assert len(unread_in_the_kernel(conn)) == len(more), "the server read requests it had stopped reading"
        assert not conn.closed, "a paused connection holds what it holds and is not closed for it"
        assert mask_of(server, conn) == WRITE


# --- the controls -------------------------------------------------------------------------


def test_a_high_water_mark_of_zero_never_pauses_a_connection():
    with serving(write_buffer_high_water=0) as (server, client, conn, _connect):
        store_value(server, client, VALUE)
        client.sendall(GET * BURST)
        owed = owed_once_quiet(server, conn)

        shipped = build_arg_parser().parse_args([]).write_buffer_high_water
        assert owed > shipped, "the queue never passed the mark that would have paused it"
        assert mask_of(server, conn) == READ | WRITE, "with the pause off the connection is still being read"

        # read while it owes megabytes: the request leaves the kernel, which a paused
        # connection's does not
        client.sendall(PING)
        pump_until(
            server, lambda: unread_in_the_kernel(conn) == b"" and not conn.read_buffer,
            "a connection owing megabytes to read the request that followed", patience=5)
        assert mask_of(server, conn) == READ | WRITE

        received = receive(server, client, BURST * len(REPLY) + len(PONG))
        assert_same_stream(received, REPLY * BURST + PONG)


def test_one_reply_larger_than_the_write_buffer_limit_closes_the_connection():
    # one reply, so there is no queue to blame: it is larger than the limit and larger than
    # everything the kernel will take at once. an idle peer is the usual case and a reply
    # under the limit is always sent whole, which is why the value has to be this big
    size = 16 * 1024 * 1024
    with listening() as (server, connect, _listener):
        server.write_buffer_limit = 65536
        server._store.write(b"big", b"v" * size, keep_ttl=False)
        client = connect()
        pump(server)
        conn, = server._connections

        client.sendall(b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n")
        pump_until(server, lambda: conn.closed, "the oversized reply to close the connection")
        assert conn not in server._connections
        assert server.connected_clients == 0

        reply = b"$%d\r\n%s\r\n" % (size, b"v" * size)
        received = read_to_the_end(client)
        assert 0 < len(received) < len(reply), "the whole reply was delivered, so nothing exceeded the limit"
        assert received == reply[:len(received)], "what was delivered is a prefix of the reply"

        other = connect()
        pump(server)
        other.sendall(PING)
        pump(server)
        assert other.recv(len(PONG)) == PONG, "closing one connection took the server with it"


def test_a_value_at_the_size_cap_is_stored_and_not_readable_back_past_the_write_buffer_limit(caplog):
    # a value can be as large as --max-value-size and a reply can be no larger than
    # --write-buffer-limit once the kernel has stopped taking it, and nothing relates the two.
    # at 131072 against 65536 the client that sends GETs and never reads them stores the
    # value and is then closed by the first reply that does not fit. the numbers are above
    # loopback buffering on purpose: a send holds a reply this size whole whenever the peer
    # has room, so an idle client is never closed by one, and only a client whose kernel has
    # filled is
    cap = 131072
    with serving(max_value_size=cap, write_buffer_limit=65536) as (server, client, conn, _connect):
        value = bytes(range(256)) * (cap // 256)
        assert len(value) == cap
        store_value(server, client, value)
        assert server._store.lookup(b"k") == value, "the value was refused, so there is nothing to read back"

        replies = 100
        reply = b"$%d\r\n%s\r\n" % (cap, value)
        client.sendall(GET * replies)
        pump_until(server, lambda: conn.closed, "the connection to be closed by the limit")

        delivered = read_to_the_end(client)
        assert len(delivered) // len(reply) < replies, "every reply was delivered"
        assert delivered == (reply * replies)[:len(delivered)], "what was delivered is a prefix of the replies"
        assert server._store.lookup(b"k") == value, "closing the client does not unstore the value"
        assert any(
            "exceeds the 65536 byte limit" in record.getMessage() for record in caplog.records
        ), "the connection was not closed for exceeding the limit"


# --- many small elements are one reply -----------------------------------------------------
#
# the size cap holds each element of a list and each key, and nothing holds the number of them,
# so one LRANGE or one KEYS reply can be as long as the keyspace has grown and is judged whole,
# after the send, like any other. these two tests pin that for the shapes the string case above
# does not reach. every element is a small fraction of the cap and of the limit, so none of them
# can be what closes the connection: the sum is

# 4096 of 4 KiB is 16 MiB, past what a loopback pair takes from a peer that reads nothing, which
# is the premise the limit is judged on. the cap and the limit are the string case's
ELEMENT_SIZE = 4096
ELEMENT_COUNT = 4096
SMALL_CAP = 131072
SMALL_LIMIT = 65536
# what one RPUSH or one batch of SETs carries, kept well inside what the kernel takes from a
# client the server is not running for, since one thread plays both ends
BATCH = 8


def bulk(value):
    return b"$%d\r\n%s\r\n" % (len(value), value)


def array(items):
    return b"*%d\r\n" % len(items) + b"".join(bulk(item) for item in items)


def command(*parts):
    return array(parts)


def exchange(server, client, request, wanted):
    """Send a request and return the first `wanted` bytes of what comes back.

    The server runs once, then the client waits on its own socket for the reply, and the
    server runs again only if the reply is not there. `receive` does it the other way round and
    pumps after every look, so a reply that is still in flight on loopback costs a pass that
    waits out the loop's whole select timeout with nothing to do, once per round trip, and a
    few hundred of those are seconds
    """
    client.sendall(request)
    received = bytearray()
    deadline = time.monotonic() + PATIENCE_SECONDS
    while len(received) < wanted:
        if time.monotonic() > deadline:
            raise AssertionError("received %d of %d bytes" % (len(received), wanted))
        pump(server, times=1)
        ready, _, _ = select.select([client], [], [], 0.005)
        if ready:
            received.extend(take(client, wanted - len(received)))
    return bytes(received)


def small_elements(prefix):
    # distinct, so that a reply that arrives out of order or short is not mistaken for a whole one
    return [prefix + b"%08d" % i + b"e" * (ELEMENT_SIZE - 8 - len(prefix)) for i in range(ELEMENT_COUNT)]


def assert_each_is_small(items, what):
    # an eighth of the limit and a sixteenth of the cap, framing included, so that no one of
    # them can be what closes the connection and nothing but their number can
    assert all(len(bulk(item)) * 8 <= SMALL_LIMIT and len(item) * 16 <= SMALL_CAP for item in items), (
        "%s is big enough to be what closes the connection, so this proves nothing about sums" % what)


def test_a_list_of_small_elements_is_stored_and_not_readable_back_past_the_write_buffer_limit(caplog):
    # --max-value-size holds each element and nothing holds the list, so LRANGE 0 -1 is one
    # reply as long as the list is. README says a list far under the cap can be stored and not
    # read back at any finite limit, and 0 reads it back
    elements = small_elements(b"")
    assert_each_is_small(elements, "an element")
    with serving(max_value_size=SMALL_CAP, write_buffer_limit=SMALL_LIMIT) as (server, client, conn, connect):
        for first in range(0, ELEMENT_COUNT, BATCH):
            pushed = elements[first:first + BATCH]
            length = b":%d\r\n" % (first + len(pushed))
            assert exchange(server, client, command(b"RPUSH", b"lst", *pushed), len(length)) == length
        assert len(server._store.lookup(b"lst")) == ELEMENT_COUNT, "the list was refused, so there is nothing to read back"

        # the control: the same list, a slice of it that fits under the limit, on the same connection
        head = elements[:4]
        assert len(array(head)) < SMALL_LIMIT
        client.sendall(command(b"LRANGE", b"lst", b"0", b"3"))
        assert receive(server, client, len(array(head))) == array(head)
        assert not conn.closed, "a reply under the limit closed the connection"

        whole = array(elements)
        assert len(whole) > SMALL_LIMIT * 100, "the reply is not far enough over the limit to be past the kernel"
        client.sendall(command(b"LRANGE", b"lst", b"0", b"-1"))
        pump_until(server, lambda: conn.closed, "the connection to be closed by the limit")

        delivered = read_to_the_end(client)
        assert 0 < len(delivered) < len(whole), "every element was delivered"
        assert delivered == whole[:len(delivered)], "what was delivered is a prefix of the reply"
        assert any(
            "exceeds the 65536 byte limit" in record.getMessage() for record in caplog.records
        ), "the connection was not closed for exceeding the limit"

        # closing the client does not unstore the list, and the server goes on serving others
        other = connect()
        pump(server)
        other.sendall(command(b"LLEN", b"lst"))
        assert receive(server, other, len(b":%d\r\n" % ELEMENT_COUNT)) == b":%d\r\n" % ELEMENT_COUNT

        # and 0 is what reads every element back: the limit is the only thing that changed
        server.write_buffer_limit = 0
        other.sendall(command(b"LRANGE", b"lst", b"0", b"-1"))
        assert_same_stream(receive(server, other, len(whole)), whole)


def test_a_keys_reply_of_small_keys_is_judged_whole_against_the_write_buffer_limit(caplog):
    # the same sum, reached through the keyspace: no key is larger than a fraction of the cap
    # and KEYS * lists every one of them as one array. its order is not part of the contract,
    # so what is compared is the set, and the closed connection is checked by its length
    names = small_elements(b"key-")
    assert_each_is_small(names, "a key")
    head = b"*%d\r\n" % ELEMENT_COUNT
    size = len(head) + sum(len(bulk(name)) for name in names)
    with serving(max_value_size=SMALL_CAP, write_buffer_limit=SMALL_LIMIT) as (server, client, conn, connect):
        for first in range(0, ELEMENT_COUNT, BATCH):
            stored = names[first:first + BATCH]
            request = b"".join(command(b"SET", name, b"v") for name in stored)
            assert exchange(server, client, request, 5 * len(stored)) == b"+OK\r\n" * len(stored)
        assert server._store.live_count() == ELEMENT_COUNT, "a key was refused, so KEYS has fewer to list"

        # the control: the same keyspace, a reply from it that fits under the limit
        client.sendall(command(b"KEYS", b"key-00000000*"))
        assert receive(server, client, len(array(names[:1]))) == array(names[:1])
        assert not conn.closed, "a reply under the limit closed the connection"

        assert size > SMALL_LIMIT * 100, "the reply is not far enough over the limit to be past the kernel"
        client.sendall(command(b"KEYS", b"*"))
        pump_until(server, lambda: conn.closed, "the connection to be closed by the limit")

        delivered = read_to_the_end(client)
        assert delivered.startswith(head), delivered[:32]
        assert 0 < len(delivered) < size, "every key was delivered"
        assert any(
            "exceeds the 65536 byte limit" in record.getMessage() for record in caplog.records
        ), "the connection was not closed for exceeding the limit"

        other = connect()
        pump(server)
        other.sendall(command(b"DBSIZE"))
        assert receive(server, other, len(b":%d\r\n" % ELEMENT_COUNT)) == b":%d\r\n" % ELEMENT_COUNT

        server.write_buffer_limit = 0
        other.sendall(command(b"KEYS", b"*"))
        reply = receive(server, other, size)
        assert len(reply) == size and reply.startswith(head), (len(reply), size)
        step = len(bulk(names[0]))
        listed = {reply[at:at + step] for at in range(len(head), size, step)}
        assert listed == {bulk(name) for name in names}, "KEYS read back a different set of keys"


# --- a paused connection is still a connection --------------------------------------------


def test_every_reply_of_a_paused_pipeline_arrives_once_and_in_order():
    keys = [b"k%d" % i for i in range(8)]
    values = [bytes([65 + i]) * 40000 + b"%08d" % i + bytes([97 + i]) * 25000 for i in range(8)]
    with serving() as (server, client, conn, _connect):
        for key, value in zip(keys, values):
            store_value(server, client, value, key)
        order = [(i * 5) % 8 for i in range(120)]
        client.sendall(b"".join(b"*2\r\n$3\r\nGET\r\n$2\r\n%s\r\n" % keys[i] for i in order))
        pump(server)
        assert mask_of(server, conn) == WRITE, "the pipeline never paused the connection"

        expected = b"".join(b"$%d\r\n%s\r\n" % (len(values[i]), values[i]) for i in order)
        assert_same_stream(receive(server, client, len(expected)), expected)
        assert mask_of(server, conn) == READ


def test_a_paused_client_that_disconnects_is_still_reaped():
    with serving() as (server, client, conn, _connect):
        store_value(server, client, VALUE)
        client.sendall(GET * BURST)
        pump(server)
        assert mask_of(server, conn) == WRITE

        # not reading, so the only way the server can learn it has gone is through the write
        # it still has registered
        client.close()
        pump_until(server, lambda: conn.closed, "the paused connection to the vanished client to be closed")

        assert conn not in server._connections
        assert server.connected_clients == 0
        assert all(key.fileobj is not conn for key in server._loop._selector.get_map().values())


def test_a_paused_connection_does_not_stop_the_server_answering_the_others():
    with serving() as (server, client, conn, connect):
        store_value(server, client, VALUE)
        client.sendall(GET * BURST)
        pump(server)
        assert mask_of(server, conn) == WRITE

        other = connect()
        other.settimeout(PATIENCE_SECONDS)
        pump(server)
        assert server.connected_clients == 2
        other.sendall(PING)
        assert receive(server, other, len(PONG)) == PONG
        other.sendall(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        assert receive(server, other, len(REPLY)) == REPLY

        assert mask_of(server, conn) == WRITE, "serving another client resumed the paused one"
        assert not conn.closed


# --- the pair, from outside ---------------------------------------------------------------

PAYLOAD_SIZE = 16 * 1024
PAYLOAD_COUNT = 1500
STALL_SECONDS = 0.1
DEADLINE_SECONDS = 120


class SenderProgress:
    # two plain attributes the reader polls: it only ever needs the latest values, never a
    # consistent pair
    def __init__(self):
        self.sent = 0
        self.last_send = time.monotonic()


def test_the_water_marks_pause_and_resume_over_a_real_socket(tmp_path):
    # a server in its own process, one client that sends from one thread and reads from
    # another, and a reader that deliberately lags. the lag is what builds a backlog: a reader
    # that kept pace would pass against a server with no pause at all. what makes the run
    # prove the pair and not merely survive it is the limit, set to a sixth of the traffic:
    # without the pause the server reads the whole pipeline, queues every reply past the
    # kernel's share and closes the connection, and with it the server stops reading the
    # sender, which stalls, which is the only moment this reader reads
    high_water = build_arg_parser().parse_args([]).write_buffer_high_water
    limit = 4 * 1024 * 1024
    payloads = [(b"%08d" % i) * (PAYLOAD_SIZE // 8) for i in range(PAYLOAD_COUNT)]
    requests = [b"*2\r\n$4\r\nECHO\r\n$%d\r\n%s\r\n" % (len(p), p) for p in payloads]
    reply_size = len(b"$%d\r\n" % PAYLOAD_SIZE) + PAYLOAD_SIZE + 2
    expected = b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in payloads)
    total = PAYLOAD_COUNT * reply_size
    assert len(expected) == total
    assert total > 5 * limit
    # past anything the pause lets pile up and past what a socket buffer and the limit can
    # hold between them, so this reader never reads because of its backlog alone: it reads
    # when the sender is held up, or when the sender is done
    lag_ceiling = 5 * limit

    progress = SenderProgress()
    outcome = {}

    def send_everything(sock):
        try:
            for request in requests:
                sock.sendall(request)
                progress.sent += 1
                progress.last_send = time.monotonic()
        except OSError as exc:
            outcome["sender_error"] = repr(exc)

    def read_lagging(sock):
        received = bytearray()
        peak = 0
        deadline = time.monotonic() + DEADLINE_SECONDS
        try:
            while len(received) < len(expected) and time.monotonic() < deadline:
                outstanding = progress.sent * reply_size - len(received)
                peak = max(peak, outstanding)
                held_up = time.monotonic() - progress.last_send >= STALL_SECONDS
                if outstanding >= lag_ceiling or held_up:
                    ready, _, _ = select.select([sock], [], [], 1.0)
                    if not ready:
                        continue
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    received.extend(chunk)
                else:
                    time.sleep(0.005)
        except OSError as exc:
            outcome["reader_error"] = repr(exc)
        outcome["received"] = bytes(received)
        outcome["peak"] = peak

    proc, port = launch_server(
        tmp_path / "dump.mrdb", extra_args=("--write-buffer-limit", str(limit)))
    try:
        with socket.create_connection(("127.0.0.1", port)) as sock:
            reader = threading.Thread(target=read_lagging, args=(sock,), daemon=True)
            sender = threading.Thread(target=send_everything, args=(sock,), daemon=True)
            reader.start()
            sender.start()
            sender.join(DEADLINE_SECONDS + 5)
            reader.join(DEADLINE_SECONDS + 5)
            assert not sender.is_alive() and not reader.is_alive(), "a thread was still running at the deadline"
    finally:
        stop_server(proc)

    peak = outcome.get("peak", 0)
    print("peak outstanding: %d bytes (high-water mark %d, write buffer limit %d, traffic %d)"
          % (peak, high_water, limit, total))
    received = outcome.get("received", b"")
    errors = [outcome[name] for name in ("sender_error", "reader_error") if name in outcome]
    assert not errors, "the connection failed after %d of %d reply bytes: %s" % (len(received), total, errors)
    assert_same_stream(received, expected)
    assert peak > high_water, (
        "peak outstanding was %d bytes and never passed the %d byte high-water mark, so the "
        "run never had a backlog the pause could act on" % (peak, high_water))
