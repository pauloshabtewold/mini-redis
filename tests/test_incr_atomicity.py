"""INCR under a real backlog.

One connection bursts N INCRs against a fresh counter while a deliberately lagging
reader proves the backlog actually grew past a real high-water mark before it drains.
A second scenario runs eight connections against the same counter to prove nothing is
lost or reordered when several clients increment it at once.

Run directly (`python -m tests.test_incr_atomicity [N]`) to repeat the burst at
whatever size is given on the command line, printing the byte counts and the final
counter value rather than asserting on them.
"""

import pathlib
import socket
import subprocess
import sys
import threading
import time

from tests.conftest import free_port, wait_until_listening

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

N = 500_000  # clears a 1 MiB backlog with margin while staying near 1.5s of runtime -- measured
TARGET_OUTSTANDING = 2 * 1024 * 1024
STALL_SECONDS = 0.1
DEADLINE_SECONDS = 120

HIGH_WATER_MARK = 1024 * 1024  # the inbound-backpressure threshold this test proves it clears

_INCR_COUNTER = b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"
_GET_COUNTER = b"*2\r\n$3\r\nGET\r\n$7\r\ncounter\r\n"


class _SenderProgress:
    # a shared mutable object rather than a lock: the reader only ever needs the
    # sender's latest snapshot, never a consistent pair of fields, and plain attribute
    # assignment is already atomic under the GIL
    def __init__(self):
        self.sent = 0
        self.last_send = time.monotonic()


def _expected_replies(n):
    # the byte-exact expected stream and the exact prefix-sum table it's built from, in
    # one pass -- reply lengths are not uniform (":9\r\n" is 4 bytes, ":10\r\n" is 5), so
    # "outstanding bytes so far" has to come from this table rather than from a reply
    # count times an assumed size
    chunks = [b":%d\r\n" % i for i in range(1, n + 1)]
    prefix = [0]
    total = 0
    for chunk in chunks:
        total += len(chunk)
        prefix.append(total)
    return b"".join(chunks), prefix


def _send_incr_burst(sock, n, progress):
    for i in range(1, n + 1):
        sock.sendall(_INCR_COUNTER)
        progress.sent = i
        progress.last_send = time.monotonic()


def _read_lagging(sock, prefix, target_outstanding, stall_seconds, deadline_seconds, progress):
    # lags rather than paces: a reader that drains as fast as it sends never has more
    # than a few KiB outstanding, and a server with no drain at all would pass right
    # alongside it. reading also resumes on a stalled sender, not only on a full
    # backlog -- pacing the backlog alone would deadlock once inbound flow control
    # exists and stalls a sender this reader would otherwise wait on forever
    expected_total = prefix[-1]
    received = bytearray()
    peak_outstanding = 0
    deadline = time.monotonic() + deadline_seconds
    sock.settimeout(1.0)
    while len(received) < expected_total and time.monotonic() < deadline:
        outstanding = prefix[progress.sent] - len(received)
        peak_outstanding = max(peak_outstanding, outstanding)
        stalled = (time.monotonic() - progress.last_send) >= stall_seconds
        if outstanding >= target_outstanding or stalled:
            try:
                chunk = sock.recv(65536)
            except TimeoutError:
                continue
            if not chunk:
                break
            received.extend(chunk)
        else:
            time.sleep(0.005)
    return bytes(received), peak_outstanding


def _run_burst(sock, n):
    """Send n INCRs on a fresh counter over sock and read every reply back.

    Returns (received_bytes, expected_bytes, peak_outstanding_bytes).
    """
    expected, prefix = _expected_replies(n)
    progress = _SenderProgress()
    outcome = {}

    def read_target():
        outcome["received"], outcome["peak"] = _read_lagging(
            sock, prefix, TARGET_OUTSTANDING, STALL_SECONDS, DEADLINE_SECONDS, progress
        )

    reader = threading.Thread(target=read_target, daemon=True)
    sender = threading.Thread(target=_send_incr_burst, args=(sock, n, progress), daemon=True)
    reader.start()
    sender.start()
    sender.join(DEADLINE_SECONDS + 5)
    reader.join(DEADLINE_SECONDS + 5)
    return outcome.get("received", b""), expected, outcome.get("peak", 0)


def _read_bulk_string_reply(sock, deadline):
    # parses the $<len> header before reading the body, rather than assuming a length
    # up front -- a wrong count then shows up as a mismatched value instead of a hang
    sock.settimeout(1.0)
    buf = bytearray()
    while b"\r\n" not in buf:
        if time.monotonic() > deadline:
            raise AssertionError("no bulk-string header received before the deadline")
        try:
            chunk = sock.recv(4096)
        except TimeoutError:
            continue
        if not chunk:
            raise AssertionError("connection closed before a bulk-string header arrived")
        buf.extend(chunk)
    header_end = buf.index(b"\r\n")
    length = int(buf[1:header_end])
    total_len = header_end + 2 + length + 2
    while len(buf) < total_len:
        if time.monotonic() > deadline:
            raise AssertionError("bulk-string body incomplete before the deadline")
        try:
            chunk = sock.recv(4096)
        except TimeoutError:
            continue
        if not chunk:
            raise AssertionError("connection closed before the bulk-string body arrived")
        buf.extend(chunk)
    return bytes(buf[header_end + 2 : header_end + 2 + length])


# --- the two suite tests -----------------------------------------------------------------


def test_a_bursting_client_gets_every_reply_in_order(mini_redis_server):
    with socket.create_connection(("127.0.0.1", mini_redis_server)) as sock:
        received, expected, peak = _run_burst(sock, N)
    print("peak outstanding: %d bytes (target %d, N=%d)" % (peak, TARGET_OUTSTANDING, N))
    assert received == expected, (
        "received %d bytes, expected %d bytes for %d INCR replies (stopped at the %ds deadline)"
        % (len(received), len(expected), N, DEADLINE_SECONDS)
    )
    assert peak > HIGH_WATER_MARK, (
        "peak outstanding was %d bytes, never exceeded the %d byte high-water mark"
        % (peak, HIGH_WATER_MARK)
    )


def test_many_connections_incrementing_one_counter_stay_exact(mini_redis_server):
    port = mini_redis_server
    n_connections = 8
    per_connection = 2_000
    deadline = time.monotonic() + DEADLINE_SECONDS
    results = {}

    def client(idx):
        with socket.create_connection(("127.0.0.1", port)) as sock:
            sock.sendall(_INCR_COUNTER * per_connection)
            sock.settimeout(1.0)
            data = bytearray()
            while data.count(b"\r\n") < per_connection and time.monotonic() < deadline:
                try:
                    chunk = sock.recv(65536)
                except TimeoutError:
                    continue
                if not chunk:
                    break
                data.extend(chunk)
            results[idx] = data.count(b"\r\n")

    threads = [
        threading.Thread(target=client, args=(idx,), daemon=True) for idx in range(n_connections)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(DEADLINE_SECONDS + 5)

    assert len(results) == n_connections, (
        "only %d of %d client threads finished before the deadline" % (len(results), n_connections)
    )
    for idx in range(n_connections):
        assert results[idx] == per_connection, (idx, results[idx])

    with socket.create_connection(("127.0.0.1", port)) as sock:
        sock.sendall(_GET_COUNTER)
        value = _read_bulk_string_reply(sock, time.monotonic() + 5)
    assert value == b"16000", value


# --- the standalone entry point, at whatever N is given on the command line --------------


def _run_burst_against_a_fresh_server(n):
    # not the mini_redis_server fixture: pytest refuses a fixture called outside a
    # test, so this builds its own server from the same two helpers the fixture uses
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, str(REPO_ROOT / "server.py"), "--port", str(port)],
        stdout=subprocess.DEVNULL,
    )
    try:
        assert wait_until_listening(port, time.monotonic() + 5), "server never started listening"
        with socket.create_connection(("127.0.0.1", port)) as sock:
            received, expected, peak = _run_burst(sock, n)
        with socket.create_connection(("127.0.0.1", port)) as sock:
            sock.sendall(_GET_COUNTER)
            counter_value = _read_bulk_string_reply(sock, time.monotonic() + 5)
        return received, expected, peak, counter_value
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else N
    received_bytes, expected_bytes, peak_bytes, counter_value = (
        _run_burst_against_a_fresh_server(count)
    )
    print("reply bytes received:", len(received_bytes))
    print("reply bytes expected:", len(expected_bytes))
    print("peak outstanding:", peak_bytes)
    print("GET counter:", counter_value.decode())
