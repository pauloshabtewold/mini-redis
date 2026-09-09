"""Pipelining and the large-value round trip.

Two shapes of pressure reach the same two buffers: a thousand small commands
arriving in one batch exercise the read-side drain that reassembles a pipeline
without dropping or reordering a reply, and one command whose reply does not fit a
single send() exercises the write-side drain that queues the remainder and empties
it across later writable events. Both live in this module because both are guards
against code this feature has not touched yet -- proven here, once, under the
caps' own defaults (64 MiB, 1,048,576) with every payload here comfortably inside
both, so the witness is that neither drain broke, not that no cap was watching.
"""

import os
import selectors
import socket
import time

from tests.test_server_lifecycle import listening, pump

# a self-imposed budget per read phase, never pytest.mark.timeout -- consistent with
# tests/test_incr_atomicity.py, the only other module in this suite driving a real
# socket to a deadline
DEADLINE_SECONDS = 60

# the count each test's name commits to
PIPELINE_COUNT = 1000

# one 8 MiB payload, allocated once at module level and shared by the round-trip test
# and the drain witness -- neither keeps a second copy of it beyond the bytes a reply
# comparison needs
LARGE_VALUE = os.urandom(8 << 20)


def expected_bulk_stream(values):
    return b"".join(b"$%d\r\n%s\r\n" % (len(v), v) for v in values)


def assert_stream_matches(assembled, values):
    expected = expected_bulk_stream(values)
    if assembled == expected:
        return
    shared = min(len(assembled), len(expected))
    for i in range(shared):
        if assembled[i] != expected[i]:
            raise AssertionError(
                "reply stream differs from the request order at byte %d: got %r, want %r"
                % (i, assembled[i:i + 8], expected[i:i + 8])
            )
    raise AssertionError(
        "reply stream length %d differs from the request order's expected length %d"
        % (len(assembled), len(expected))
    )


# --- module-local helpers: a real socket against the subprocess fixture ------------------


def _encode_command(*args):
    parts = [b"*%d\r\n" % len(args)]
    for arg in args:
        parts.append(b"$%d\r\n%s\r\n" % (len(arg), arg))
    return b"".join(parts)


def _pipeline_keys_and_values():
    keys = [b"pipeline-key-%d" % i for i in range(PIPELINE_COUNT)]
    values = [b"pipeline-value-%d" % i for i in range(PIPELINE_COUNT)]
    return keys, values


def _recv_exactly(sock, want_len, deadline):
    data = bytearray()
    while len(data) < want_len and time.monotonic() < deadline:
        try:
            chunk = sock.recv(65536)
        except TimeoutError:
            continue
        if not chunk:
            break
        data.extend(chunk)
    return bytes(data)


# --- module-local helpers: an in-process Server driven a pump() at a time ----------------


def _inproc_write_interest(server, conn):
    return bool(server._loop._selector.get_key(conn).events & selectors.EVENT_WRITE)


def _inproc_feed(server, client, payload, seconds=DEADLINE_SECONDS):
    # non-blocking and interleaved with pump(): a plain sendall() of a payload this
    # size blocks on the client's own kernel buffer once it fills, and nothing drains
    # that buffer but the server's own read, which only a pump() drives here
    client.setblocking(False)
    pos, deadline = 0, time.monotonic() + seconds
    while pos < len(payload) and time.monotonic() < deadline:
        try:
            pos += client.send(payload[pos:pos + (1 << 16)])
        except BlockingIOError:
            pass
        pump(server, times=4)
    assert pos == len(payload), (pos, len(payload))


def _inproc_read_exactly(server, client, count, seconds=DEADLINE_SECONDS):
    got, deadline = bytearray(), time.monotonic() + seconds
    while len(got) < count and time.monotonic() < deadline:
        pump(server, times=1)
        try:
            got += client.recv(1 << 16)
        except BlockingIOError:
            pass
    return bytes(got)


# --- the four tests ------------------------------------------------------------------


def test_a_thousand_pipelined_sets_are_all_answered_with_none_dropped(mini_redis_server):
    keys, values = _pipeline_keys_and_values()
    batch = b"".join(_encode_command(b"SET", k, v) for k, v in zip(keys, values))
    expected = b"+OK\r\n" * PIPELINE_COUNT

    with socket.create_connection(("127.0.0.1", mini_redis_server)) as sock:
        sock.sendall(batch)
        sock.settimeout(1.0)
        reply = _recv_exactly(sock, len(expected), time.monotonic() + DEADLINE_SECONDS)

    assert reply == expected, (len(reply), len(expected))


def test_a_thousand_pipelined_gets_come_back_byte_exact_in_request_order(mini_redis_server):
    keys, values = _pipeline_keys_and_values()
    set_batch = b"".join(_encode_command(b"SET", k, v) for k, v in zip(keys, values))
    get_batch = b"".join(_encode_command(b"GET", k) for k in keys)
    set_expected = b"+OK\r\n" * PIPELINE_COUNT

    with socket.create_connection(("127.0.0.1", mini_redis_server)) as sock:
        sock.sendall(set_batch)
        sock.settimeout(1.0)
        set_reply = _recv_exactly(sock, len(set_expected), time.monotonic() + DEADLINE_SECONDS)
        assert set_reply == set_expected, "pipelined SET setup failed"

        sock.settimeout(None)
        sock.sendall(get_batch)
        sock.settimeout(1.0)
        want_len = len(expected_bulk_stream(values))
        get_reply = _recv_exactly(sock, want_len, time.monotonic() + DEADLINE_SECONDS)

    assert_stream_matches(get_reply, values)


def test_an_eight_mebibyte_value_round_trips_byte_exact_in_both_directions(mini_redis_server):
    with socket.create_connection(("127.0.0.1", mini_redis_server)) as sock:
        sock.sendall(_encode_command(b"SET", b"big", LARGE_VALUE))
        sock.settimeout(1.0)
        set_reply = _recv_exactly(sock, len(b"+OK\r\n"), time.monotonic() + DEADLINE_SECONDS)
        assert set_reply == b"+OK\r\n", set_reply

        sock.settimeout(None)
        sock.sendall(_encode_command(b"GET", b"big"))
        sock.settimeout(1.0)
        want_len = len(expected_bulk_stream([LARGE_VALUE]))
        get_reply = _recv_exactly(sock, want_len, time.monotonic() + DEADLINE_SECONDS)

    assert_stream_matches(get_reply, [LARGE_VALUE])


def test_a_reply_too_large_for_one_send_is_queued_and_drained_across_writable_events():
    with listening() as (server, connect, _listener):
        client = connect()
        pump(server)
        conn = next(iter(server._connections))

        _inproc_feed(server, client, _encode_command(b"SET", b"big", LARGE_VALUE))
        assert _inproc_read_exactly(server, client, 5) == b"+OK\r\n", (
            "the large SET was never answered"
        )

        # the near miss: a reply that fits one send() leaves nothing behind, which is
        # what makes the assertions below a witness rather than a formality
        client.setblocking(True)
        client.sendall(_encode_command(b"SET", b"small", b"ab"))
        pump(server, times=4)
        assert _inproc_read_exactly(server, client, 5) == b"+OK\r\n"

        client.sendall(_encode_command(b"GET", b"small"))
        pump(server, times=1)
        assert len(conn.write_buffer) == 0, len(conn.write_buffer)
        assert not _inproc_write_interest(server, conn)
        assert _inproc_read_exactly(server, client, 8) == b"$2\r\nab\r\n"

        client.sendall(_encode_command(b"GET", b"big"))
        pump(server, times=4)
        queued = len(conn.write_buffer)
        assert queued > 0, "the whole 8 MiB reply left in one send() -- no drain to witness"
        assert _inproc_write_interest(server, conn), (
            "a partially sent reply left write interest cleared"
        )

        want = expected_bulk_stream([LARGE_VALUE])
        assert _inproc_read_exactly(server, client, len(want)) == want
        assert conn.write_buffer == bytearray(), len(conn.write_buffer)
        assert not _inproc_write_interest(server, conn), (
            "write interest survived an emptied buffer"
        )
        print(
            "drain witness: %d of %d bytes were still queued after the first flush"
            % (queued, len(want))
        )
