"""Many clients writing at once: no command lost, no value mixed up, every command answered.

CLIENTS connections open, wait for each other, then each pipelines KEYS_PER_CLIENT SETs on
keys no other client writes. Three observables, because a server that silently drops half
its commands is not caught by asking whether it is still running: a dropped command is a
short DBSIZE, a value crossed between connections is a read-back mismatch, and a dropped
reply is a client holding fewer replies than it sent commands.

Replies are read to a total wall-clock deadline and a shortfall is reported by count. An idle
timeout would read "nothing arrived for N seconds" as "the server is done", which is false
whenever the server is merely busy.
"""

import random
import socket
import threading
import time

import pytest

from tests.conftest import launch_server, stop_server

# far past the handful a hand-written test uses, so one select() pass routinely finds many connections readable at once
CLIENTS = 64
# about 90 KiB of SETs per client, past one 64 KiB recv, so commands split across reads while other connections are served
KEYS_PER_CLIENT = 500
# an order of magnitude over CLIENTS and passed explicitly, so this stays a concurrency test and never becomes a cap test
MAX_CONNECTIONS = 512
# bounds a failure and not a pass: a healthy run finishes well inside a second, and a shortfall is reported the moment this passes
DEADLINE_SECONDS = 15

RECV_SIZE = 65536
OK = b"+OK\r\n"


@pytest.fixture
def server_port(tmp_path):
    # a snapshot path under tmp_path, because teardown's SIGTERM now saves one
    proc, port = launch_server(
        tmp_path / "dump.mrdb", extra_args=("--max-connections", str(MAX_CONNECTIONS))
    )
    try:
        yield port
    finally:
        stop_server(proc)


def _command(*parts):
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts)


def _key(client, index):
    return b"client%03d:key%05d" % (client, index)


def _value(client, index):
    # seeded per key so the expected bytes are recomputed rather than stored, and at least 16 random bytes so one client's value is never mistaken for another's
    length = 16 + (index * 37 + client * 11) % 240
    return random.Random(client * 1_000_003 + index).randbytes(length)


def _bulk(value):
    return b"$%d\r\n%s\r\n" % (len(value), value)


def _frame_end(buffer, start):
    # the index just past the reply that begins at `start`, or None while it is still incomplete
    line_end = buffer.find(b"\r\n", start)
    if line_end == -1:
        return None
    kind = bytes(buffer[start:start + 1])
    if kind in (b"+", b"-", b":"):
        return line_end + 2
    if kind == b"$":
        length = int(buffer[start + 1:line_end])
        if length < 0:
            return line_end + 2
        end = line_end + 2 + length + 2
        return end if len(buffer) >= end else None
    # arrays are never asked for here, and guessing at one would hide a wrong reply behind a wrong count
    raise AssertionError("unexpected reply type %r at offset %d" % (kind, start))


def read_replies_until_deadline(sock, expected, deadline_seconds):
    """Read RESP replies off `sock` until `expected` have arrived or the deadline passes.

    Returns every complete reply read, each as its raw frame, and does not trim to `expected`:
    a surplus reply is something the caller should see. On a shortfall it raises
    AssertionError naming how many arrived out of how many were expected, whether the deadline
    passed or the peer closed first. The deadline is one span of wall-clock time for the whole
    read, never a wait for the next byte.
    """
    started = time.monotonic()
    deadline = started + deadline_seconds
    previous_timeout = sock.gettimeout()
    buffer = bytearray()
    replies = []
    position = 0
    cause = None
    try:
        while len(replies) < expected:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                cause = "the %gs deadline passed" % deadline_seconds
                break
            sock.settimeout(remaining)
            try:
                chunk = sock.recv(RECV_SIZE)
            except TimeoutError:
                cause = "the %gs deadline passed" % deadline_seconds
                break
            except ConnectionResetError:
                cause = "the server reset the connection after %.1fs" % (time.monotonic() - started)
                break
            if not chunk:
                cause = "the server closed the connection after %.1fs" % (time.monotonic() - started)
                break
            buffer.extend(chunk)
            while (end := _frame_end(buffer, position)) is not None:
                replies.append(bytes(buffer[position:end]))
                position = end
    finally:
        # the caller sends on this socket next, and a timeout left at the last sliver of the deadline would fail that send
        sock.settimeout(previous_timeout)
    if len(replies) < expected:
        raise AssertionError("got %d of %d expected replies: %s" % (len(replies), expected, cause))
    return replies


def _client(port, index, start, read_back):
    # the payloads are built before the barrier so the release is followed straight away by sends and not by every thread building its own
    sets = b"".join(
        _command(b"SET", _key(index, i), _value(index, i)) for i in range(KEYS_PER_CLIENT)
    )
    gets = b"".join(_command(b"GET", _key(index, i)) for i in range(KEYS_PER_CLIENT))
    with socket.create_connection(("127.0.0.1", port), timeout=DEADLINE_SECONDS) as sock:
        start.wait(DEADLINE_SECONDS)
        sock.sendall(sets)
        set_replies = read_replies_until_deadline(sock, KEYS_PER_CLIENT, DEADLINE_SECONDS)
        get_replies = []
        if read_back:
            sock.sendall(gets)
            get_replies = read_replies_until_deadline(sock, KEYS_PER_CLIENT, DEADLINE_SECONDS)
    return set_replies, get_replies


def _run_clients(port, read_back):
    """Run every client at once and return one (set_replies, get_replies) pair per client."""
    start = threading.Barrier(CLIENTS)
    outcomes = [None] * CLIENTS

    def run(index):
        try:
            outcomes[index] = _client(port, index, start, read_back)
        except Exception as exc:
            # a client that dies before the barrier would otherwise leave the rest waiting out its whole timeout
            start.abort()
            outcomes[index] = exc

    threads = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(CLIENTS)]
    for thread in threads:
        thread.start()
    # two reads at most per client, each bounded by its own deadline, plus slack for connecting
    give_up = time.monotonic() + 2 * DEADLINE_SECONDS + 10
    for thread in threads:
        thread.join(max(0.0, give_up - time.monotonic()))
    stuck = sum(thread.is_alive() for thread in threads)
    assert not stuck, "%d of %d clients had not finished when the deadline passed" % (stuck, CLIENTS)

    failures = [(i, o) for i, o in enumerate(outcomes) if isinstance(o, Exception)]
    if failures:
        # an abort makes every other client raise BrokenBarrierError, which names nobody; the first real failure does
        real = [f for f in failures if not isinstance(f[1], threading.BrokenBarrierError)]
        index, exc = (real or failures)[0]
        raise AssertionError("client %d failed (%d clients did): %r" % (index, len(failures), exc)) from exc
    return outcomes


def test_many_clients_writing_distinct_keys_lose_none_of_them(server_port):
    _run_clients(server_port, read_back=False)
    with socket.create_connection(("127.0.0.1", server_port), timeout=DEADLINE_SECONDS) as sock:
        sock.sendall(_command(b"DBSIZE"))
        (reply,) = read_replies_until_deadline(sock, 1, DEADLINE_SECONDS)
    expected = b":%d\r\n" % (CLIENTS * KEYS_PER_CLIENT)
    assert reply == expected, (
        "DBSIZE answered %r after %d clients wrote %d distinct keys each, expected %r"
        % (reply, CLIENTS, KEYS_PER_CLIENT, expected)
    )


def test_every_value_written_concurrently_reads_back_byte_exact(server_port):
    outcomes = _run_clients(server_port, read_back=True)
    wrong = []
    for client, (_, get_replies) in enumerate(outcomes):
        for index in range(KEYS_PER_CLIENT):
            expected = _bulk(_value(client, index))
            if index >= len(get_replies) or get_replies[index] != expected:
                wrong.append((client, index))
    assert not wrong, (
        "%d of %d values did not read back byte-exact; the first is client %d key %d"
        % (len(wrong), CLIENTS * KEYS_PER_CLIENT, *wrong[0])
    )


def test_every_client_receives_a_reply_for_every_command_it_sent(server_port):
    outcomes = _run_clients(server_port, read_back=False)
    counts = {client: len(set_replies) for client, (set_replies, _) in enumerate(outcomes)}
    off = {client: count for client, count in counts.items() if count != KEYS_PER_CLIENT}
    assert not off, "clients whose reply count was not %d: %r" % (KEYS_PER_CLIENT, off)
    refused = [
        (client, index)
        for client, (set_replies, _) in enumerate(outcomes)
        for index, reply in enumerate(set_replies)
        if reply != OK
    ]
    assert not refused, "%d SETs were not answered +OK; the first is client %d key %d" % (
        len(refused), *refused[0])
