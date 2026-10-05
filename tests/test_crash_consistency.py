"""What a restart recovers after `kill -9`, checked against a real `server.py` process.

These are manual procedures and not part of the default run: every one starts servers of
its own, waits on a real snapshot interval and ends one with a signal, which costs seconds
where the rest of the suite costs milliseconds. `pyproject.toml` deselects the `manual`
marker; select it with

    python -m pytest -m manual tests/test_crash_consistency.py -v -s

and `-s` shows the numbers each test compares: the snapshot's mtime, the count written
before it, and the `PTTL` that came back.

The data-loss window
--------------------
Persistence here is a periodic full snapshot and a snapshot on a clean stop, nothing
between. A crash therefore loses every write made since the last save that completed: at
most `--snapshot-interval` seconds of writes, plus the time a save takes to run and up to
one `select()` timeout (0.1 s) for the tick that starts it to notice its deadline. A
`SIGTERM` is not a crash and loses nothing, because the stop writes one more snapshot, so
every kill here is a `SIGKILL`, which cannot be caught and runs no code of the server's.

Why each test is shaped the way it is
-------------------------------------
Counting recovered keys cannot see a wrong deadline. A deadline stored with
`time.monotonic()` instead of an absolute Unix millisecond leaves every key present after
the restart and only its expiry wrong, so the key written with a TTL has its `PTTL` read
back in milliseconds and bounded on both sides by the time that passed.

"The recovered state matches the last snapshot" holds trivially if no snapshot was ever
written, because both sides are then empty. The snapshot is observed on disk, with its
mtime, before the kill; the recovered count has to be above zero and at least what had been
written before that save.

Nothing here sleeps for a guessed interval. A save is awaited by reading the snapshot until
it holds what was written, under a deadline, and the one bound measured from outside, how
long a write waits to reach disk, is that deadline.
"""

import collections
import contextlib
import os
import pathlib
import select
import signal
import subprocess
import sys
import threading
import time
import types

import pytest
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

import persistence
from store import Store

pytestmark = pytest.mark.manual

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# a one second interval keeps the module quick, and is the smallest `--snapshot-interval`
# that is not off
SNAPSHOT_INTERVAL = 1
# the bytes of a snapshot holding no keys: a file of this size says a save ran and found
# nothing, which is the empty file the second assertion of the snapshot test refuses
EMPTY_SNAPSHOT_BYTES = len(persistence.encode(Store()))
# long enough to outlive every wait in this module by a wide margin, short enough that a
# deadline read as seconds or as microseconds is wrong by orders of magnitude
TTL_MS = 300_000
# how far a wall-clock reading in this process may differ from the server's once it has
# been truncated to milliseconds at each end and carried across a socket
CLOCK_SLACK_MS = 50
BURST_KEYS = 500
LIST_KEY = b"jobs"
LIST_ITEMS = [b"first", b"second", b"third", b"\x00binary\xff"]
PLAIN_KEY = b"session:plain"
TTL_KEY = b"session:ttl"
# how many keys the writer thread has had acknowledged when the kill is sent, so the kill
# lands with a pipelined burst still arriving and not in a quiet moment
LATE_KEYS_BEFORE_THE_KILL = 100
WAIT_SECONDS = 20

WINDOW_INTERVAL = 2
# time allowed beyond the interval for the tick that starts a save to be reached, the save
# itself to run and this process's poll to see the file
WINDOW_MARGIN_SECONDS = 1.0
WINDOW_ATTEMPTS = 3
# a poll sees the file a little after the rename that made it, so a second save that landed
# before the kill is excused only if it had a full interval less this much to arrive
POLL_LATENCY_SECONDS = 0.25

Snapshot = collections.namedtuple("Snapshot", "ino mtime_ns size")


def _client(port):
    # protocol 2 as everywhere in this suite; no retries, so a killed server surfaces as an
    # error at once and not after a backoff this module did not ask for
    return redis.Redis(
        host="127.0.0.1", port=port, protocol=2, socket_timeout=10, socket_connect_timeout=10,
        retry=Retry(NoBackoff(), 0))


def _bound_port(proc, seconds=10):
    # the bind line names the port the kernel chose. read from the raw descriptor under a
    # deadline: a readline() entered on a partial line has no bound of its own
    end = time.monotonic() + seconds
    seen = b""
    while time.monotonic() < end:
        ready, _, _ = select.select([proc.stdout], [], [], 0.05)
        if ready:
            chunk = os.read(proc.stdout.fileno(), 4096)
            if not chunk:
                break
            seen += chunk
            for line in seen.split(b"\n")[:-1]:
                if line.startswith(b"listening on "):
                    return int(line.rsplit(b":", 1)[1])
        elif proc.poll() is not None:
            break
    raise AssertionError("the server never said where it listened: %r" % (seen,))


def _reap(proc):
    if proc.poll() is None:
        proc.kill()
    proc.wait(timeout=10)
    if proc.stdout is not None:
        proc.stdout.close()


@contextlib.contextmanager
def _server(directory, snapshot_interval):
    """A `server.py` on a port the kernel chose, its snapshot at `directory/dump.mrdb`.

    The process is owned here and not started through the shared fixture, whose teardown
    sends `SIGTERM` and ignores the exit status: a stop that way writes a snapshot, which
    would make a crash test prove the clean path. Whatever happens in the body, the process
    is dead when this exits.
    """
    proc = subprocess.Popen(
        [sys.executable, str(REPO_ROOT / "server.py"), "--port", "0",
         "--snapshot-path", str(directory / "dump.mrdb"),
         "--snapshot-interval", str(snapshot_interval)],
        stdout=subprocess.PIPE, cwd=directory)
    try:
        yield proc, _bound_port(proc)
    finally:
        _reap(proc)


def _stat(path):
    info = os.stat(path)
    return Snapshot(info.st_ino, info.st_mtime_ns, info.st_size)


def _contents(path):
    """The snapshot at `path` as `{key: (value, deadline)}`, a list value as a `list`."""
    items = persistence.load(str(path)).snapshot_items()
    return {key: (list(value) if kind == b"list" else value, deadline)
            for key, kind, value, deadline in items}


def _wait_for_snapshot(path, holds, seconds, what):
    """Read the snapshot until `holds(contents)` is true; return its identity and contents.

    A save writes a temporary file and renames it over the path, so what is read is always
    a whole snapshot, and a changed inode or mtime between the stat and the read means the
    file was replaced under the read and it is read again.
    """
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        try:
            identity = _stat(path)
            contents = _contents(path)
            unchanged = identity == _stat(path)
        except FileNotFoundError:
            unchanged = False
        if unchanged and holds(contents):
            return identity, contents
        time.sleep(0.01)
    raise AssertionError("no snapshot held %s within %.1f s" % (what, seconds))


def _kill(proc):
    """Send `SIGKILL`, wait for the process to be gone, and return when it was sent."""
    sent_ns = time.time_ns()
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=10)
    return sent_ns


class _Writer(threading.Thread):
    """Keeps writing pipelined batches of keys until the server it writes to dies."""

    def __init__(self, port):
        super().__init__(daemon=True)
        self._port = port
        self.acked = 0
        self.interrupted = False

    def run(self):
        client = _client(self._port)
        written = 0
        try:
            while True:
                pipe = client.pipeline(transaction=False)
                for _ in range(50):
                    pipe.set(b"late:%08d" % written, b"in flight")
                    written += 1
                pipe.execute()
                self.acked = written
        except (redis.exceptions.RedisError, OSError):
            self.interrupted = True
        finally:
            client.close()


def _recovered(client):
    """Every live key of the server behind `client` as `{key: value}`, a list as a `list`."""
    keys = client.keys(b"*")
    kinds = client.pipeline(transaction=False)
    for key in keys:
        kinds.type(key)
    reads = client.pipeline(transaction=False)
    for key, kind in zip(keys, kinds.execute()):
        if kind == b"list":
            reads.lrange(key, 0, -1)
        else:
            reads.get(key)
    return dict(zip(keys, reads.execute()))


@pytest.fixture(scope="module")
def crash(tmp_path_factory):
    home = tmp_path_factory.mktemp("crash")
    path = home / "dump.mrdb"
    # what one burst, one kill and one restart left, for the first three tests to read
    run = types.SimpleNamespace(path=path)

    strings = {b"burst:%04d" % i: b"value-%04d" % i for i in range(BURST_KEYS)}
    with _server(home, SNAPSHOT_INTERVAL) as (proc, port):
        client = _client(port)
        burst = client.pipeline(transaction=False)
        for key, value in strings.items():
            burst.set(key, value)
        burst.rpush(LIST_KEY, *LIST_ITEMS)
        burst.set(PLAIN_KEY, b"no deadline")
        burst.execute()
        # the key with a deadline goes in on its own so the wall clock on either side of it
        # brackets the moment the server computed that deadline and nothing else
        run.wall_before_ms = time.time_ns() // 1_000_000
        run.ttl_sent_at = time.monotonic()
        client.set(TTL_KEY, b"expires", px=TTL_MS)
        run.ttl_acked_at = time.monotonic()
        run.wall_after_ms = time.time_ns() // 1_000_000

        written = set(strings) | {LIST_KEY, PLAIN_KEY, TTL_KEY}
        run.written_before_the_tick = len(written)
        run.before_kill, _ = _wait_for_snapshot(
            path, lambda held: written <= set(held), WAIT_SECONDS,
            "all %d keys written before it" % len(written))

        # the kill lands mid-burst: a writer is still pipelining new keys when it is sent,
        # and everything it wrote after the save above is in no snapshot
        writer = _Writer(port)
        writer.start()
        end = time.monotonic() + WAIT_SECONDS
        while writer.acked < LATE_KEYS_BEFORE_THE_KILL:
            assert time.monotonic() < end, "the writer never got its first batches acknowledged"
            time.sleep(0.005)
        run.kill_sent_ns = _kill(proc)
        run.returncode = proc.returncode
        writer.join(10)
        assert not writer.is_alive(), "the writer outlived the server it was writing to"
        assert writer.interrupted, "the kill did not reach the writer mid-burst"
        run.late_keys_acked = writer.acked
        client.close()

    # read after the process is gone, so a save that landed between the stat above and the
    # signal is what is compared, and not a file that was about to be replaced
    run.after_kill = _stat(path)
    run.on_disk = _contents(path)

    with _server(home, 0) as (restarted, port):
        client = _client(port)
        run.recovered = _recovered(client)
        run.pttl_sent_at = time.monotonic()
        run.pttl = client.pttl(TTL_KEY)
        run.pttl_replied_at = time.monotonic()
        run.plain_pttl = client.pttl(PLAIN_KEY)
        client.close()
    return run


def test_a_killed_server_recovers_the_keys_its_last_snapshot_held(crash):
    on_disk = {key: value for key, (value, _) in crash.on_disk.items()}
    print("\nkeys written before the save the kill left behind: %d" % crash.written_before_the_tick)
    print("keys in the snapshot on disk after the kill: %d" % len(on_disk))
    print("keys recovered by the restart: %d" % len(crash.recovered))
    print("keys the writer had acknowledged when the kill was sent: %d" % crash.late_keys_acked)
    assert len(crash.recovered) > 0
    assert len(crash.recovered) >= crash.written_before_the_tick
    # the whole keyspace, values and a list's order included, and in both directions: a key
    # the restart invented, or one the snapshot held and it dropped, both fail here
    assert crash.recovered == on_disk
    for index in range(BURST_KEYS):
        assert crash.recovered[b"burst:%04d" % index] == b"value-%04d" % index
    assert crash.recovered[LIST_KEY] == LIST_ITEMS


def test_a_recovered_key_still_carries_a_ttl_in_the_right_units(crash):
    in_file = crash.on_disk[TTL_KEY][1]
    print("\ndeadline in the snapshot: %d ms since the epoch; wall clock around the SET: %d to %d"
          % (in_file, crash.wall_before_ms, crash.wall_after_ms))
    # the file holds an absolute Unix millisecond. a monotonic reading would be a small
    # number from a different epoch and a count of seconds would be a thousand times too
    # small, and either reads as a long-expired key after a restart
    assert (crash.wall_before_ms + TTL_MS - CLOCK_SLACK_MS
            <= in_file
            <= crash.wall_after_ms + TTL_MS + CLOCK_SLACK_MS)

    # the key survived the restart, so a deadline already in the past is ruled out too
    assert TTL_KEY in crash.recovered
    # time passed between the SET and this read, so the remainder lies between the TTL less
    # the longest that can have passed and the TTL less the shortest. a TTL restarted at
    # load, or one that did not shrink, falls above the upper bound
    longest_ms = (crash.pttl_replied_at - crash.ttl_sent_at) * 1000
    shortest_ms = (crash.pttl_sent_at - crash.ttl_acked_at) * 1000
    print("recovered PTTL: %d ms of %d ms set, %.0f to %.0f ms having passed"
          % (crash.pttl, TTL_MS, shortest_ms, longest_ms))
    assert 0 < crash.pttl <= TTL_MS
    assert TTL_MS - longest_ms - CLOCK_SLACK_MS <= crash.pttl <= TTL_MS - shortest_ms + CLOCK_SLACK_MS

    # and a key written without a deadline did not acquire one on the way through the file
    assert PLAIN_KEY in crash.recovered
    assert crash.plain_pttl == -1
    assert crash.on_disk[PLAIN_KEY][1] == -1


def test_the_snapshot_predates_the_kill_and_is_not_an_empty_file(crash):
    held = len(crash.on_disk)
    print("\nsnapshot mtime before the kill: %d ns; the kill was sent at %d ns"
          % (crash.before_kill.mtime_ns, crash.kill_sent_ns))
    print("snapshot size before the kill: %d bytes (an empty snapshot is %d); keys in it after: %d"
          % (crash.before_kill.size, EMPTY_SNAPSHOT_BYTES, held))
    # SIGKILL runs nothing in the server, so there was no stop-time save to supply the
    # snapshot; a SIGTERM would have exited 0 having written one
    assert crash.returncode == -signal.SIGKILL
    # a file existed, with a modification time, before the signal went out
    assert crash.before_kill.mtime_ns < crash.kill_sent_ns
    # and it held keys: an empty one is the same size whenever it was written
    assert crash.before_kill.size > EMPTY_SNAPSHOT_BYTES
    assert held >= crash.written_before_the_tick > 0


def _lose_the_tail(directory):
    """One run of the loss window on a fresh directory; `None` if the run proved nothing.

    The write that is timed goes in just after a save has been seen to land, so it waits
    for the longest interval it can. The write after it, made at the same moment and
    followed at once by the kill, has no save before it and is lost.
    """
    path = directory / "dump.mrdb"
    with _server(directory, WINDOW_INTERVAL) as (proc, port):
        client = _client(port)
        client.set(b"seed", b"1")
        reach = WINDOW_INTERVAL + WINDOW_MARGIN_SECONDS
        _wait_for_snapshot(path, lambda held: b"seed" in held, reach, "the first write")

        client.set(b"aged", b"1")
        acked_at = time.monotonic()
        landed, _ = _wait_for_snapshot(path, lambda held: b"aged" in held, reach, "a write")
        landed_at = time.monotonic()
        delay = landed_at - acked_at
        print("\na write reached disk %.2f s after it was acknowledged, with --snapshot-interval %d"
              % (delay, WINDOW_INTERVAL))

        client.set(b"late", b"1")
        killed_at = time.monotonic()
        _kill(proc)
        client.close()
    # the poll that saw a save lags behind it by a little, so a further save is early only if
    # the gap since the one before is shorter than an interval by more than that
    if _stat(path) != landed:
        assert killed_at - landed_at >= WINDOW_INTERVAL - POLL_LATENCY_SECONDS, (
            "a snapshot landed %.2f s after the previous one, under a %d s interval"
            % (killed_at - landed_at, WINDOW_INTERVAL))
        return None

    with _server(directory, 0) as (restarted, port):
        client = _client(port)
        recovered = (client.get(b"aged"), client.get(b"late"))
        client.close()
    return delay, recovered


def test_the_documented_data_loss_window_is_bounded_by_the_snapshot_interval(tmp_path_factory):
    for _ in range(WINDOW_ATTEMPTS):
        outcome = _lose_the_tail(tmp_path_factory.mktemp("window"))
        if outcome is not None:
            break
    else:
        pytest.fail("a second save landed before the kill in each of %d runs, so none of them "
                    "measured the window" % WINDOW_ATTEMPTS)
    delay, (aged, late) = outcome
    print("after the kill: the write a save had covered is %r, the one it had not is %r"
          % (aged, late))
    # the interval, from both sides. the write was made just after a save landed, so the
    # next one is a whole interval away: on disk within it, with a margin for the tick that
    # starts the save and for the poll that saw it (the wait above already failed if not),
    # and not much sooner, because a save that comes early narrows the window below the one
    # configured and the bound would then be measuring some other number
    assert WINDOW_INTERVAL - POLL_LATENCY_SECONDS <= delay <= WINDOW_INTERVAL + WINDOW_MARGIN_SECONDS
    # what the bound protects: a write that a save covered survives the crash
    assert aged == b"1"
    # and what it admits: a write after the last save is gone. a window that was always zero
    # would pass the bound above without ever having been a window
    assert late is None
