"""Entry point: CLI flags, signal handling, the event loop's three callback bodies, and the periodic tick that runs after every select() return."""

import argparse
import contextlib
import logging
import os
import signal
import socket
import sys
import time
from collections.abc import Callable

import commands
import persistence
import resp
from connection import BatchProtocolError, Connection, Role
from event_loop import EventLoop
from store import Store

DEFAULT_PORT = 6379
# the replication sync command is unauthenticated and is safe only bound to loopback.
LISTEN_HOST = "127.0.0.1"
# bounds how long a stop signal waits to be noticed, and on an idle loop floors both --expiry-sweep-interval and --snapshot-interval: either deadline is checked only when run_once() returns -- see Server._tick -- so with no traffic a deadline can be noticed up to one timeout late, and the default sweep interval is equal to it. under traffic run_once() returns as soon as a socket is ready, so a shorter interval is honoured.
SELECT_TIMEOUT_SECONDS = 0.1
# 0 is unlimited, which is what the reference defaults to for an ordinary client. see
# Server._flush for why exceeding this closes the connection instead of slowing it down.
DEFAULT_OUTPUT_BUFFER_LIMIT = 0
# a local choice and not the reference's: proto-max-bulk-len defaults to 512 MiB there,
# eight times this. 64 MiB is well above the largest value the suite round-trips and below
# anything that risks OOMing this machine. --max-value-size bounds every inbound bulk
# element, including the command name and any key, not only what a human would call
# "the value"
DEFAULT_MAX_VALUE_SIZE = 64 * 1024 * 1024
# this project's own number as well, and with less to borrow: the reference has no
# multibulk default at all, and refuses a count only above INT_MAX -- which is
# MAX_MULTIBULK_COUNT in resp.py, checked separately and unconditionally
DEFAULT_MAX_MULTIBULK = 1024 * 1024
# relative to the process's current working directory, not to this file or the
# repository root: a server launched from a different directory reads and writes its
# snapshot there
DEFAULT_SNAPSHOT_PATH = "./dump.mrdb"
# a local choice and not the reference's: its own save policy is a three-tier
# changes-based 3600 1 300 100 60 10000, where this is one flat interval that runs
# unconditionally, never skipped for having nothing new to write
DEFAULT_SNAPSHOT_INTERVAL_SECONDS = 60
# refusing to start over a corrupt file is the default; --ignore-snapshot is the
# explicit escape hatch for bringing a server back up past one anyway
DEFAULT_IGNORE_SNAPSHOT = False
# matches the cadence an idle redis-server 7.2.7 runs its background cycle at: hz is 10
# there by default, so the cycle runs every 100 ms
DEFAULT_EXPIRY_SWEEP_INTERVAL_MS = 100
# the sweep's own three constants, read off the tagged sources rather than assumed:
# 7.2.7's expire.c:109-111 and 5.0.14's server.h:172-174 compile in 20 keys a pass, a
# 1000-microsecond budget and 25 per cent, and the 20 and the 1 ms here are those. the
# 25 per cent is not: it bounds a share of a tick there, where SWEEP_RELOOP_THRESHOLD
# below is a fraction of the sample a pass actually drew, and the two are different
# quantities that happen to be written with the same digits.
#
# what is deliberately not written here is how the reference reaches those numbers --
# which of its two cycles each belongs to, how it draws its sample, when it re-loops.
# every attempt to set that down here has been wrong about one clause or another, each
# correction introducing the next, and none of it was load-bearing: nothing in this file
# behaves differently for any of it. the numbers above are checkable against the sources
# named; the machinery around them is the reference's and not this file's to describe
SWEEP_SAMPLE_SIZE = 20
SWEEP_RELOOP_THRESHOLD = 0.25
SWEEP_BUDGET_SECONDS = 0.001
# the one site --expiry-sweep-interval's milliseconds are turned into the seconds
# time.monotonic() deals in
_MILLISECONDS_PER_SECOND = 1000
# the ceiling _check_schedulable refuses past: above this, the arithmetic that schedules
# an interval -- now + value for the snapshot arm, value / 1000 for the sweep -- raises
# OverflowError from float's own limit (around 10**308) rather than refusing cleanly.
# 2**63 - 1 is this project's own convention for the largest integer any value here is
# ever let hold (see commands/registry.py's INT64_MAX), reused as a ceiling that is
# still far below where the float conversion actually breaks
MAX_SCHEDULABLE_INTERVAL = 2**63 - 1

# logging.lastResort sends an ERROR record to stderr with no configuration
logger = logging.getLogger(__name__)


class ListenFailed(OSError):
    """The listening socket could not be bound, so this server never started.

    An `OSError` subclass, because a failed bind has always raised `OSError` and a caller
    that catches one still catches this. A named subclass, because `main()` catches it
    around `run()` and `run()` is the whole event loop: catching plain `OSError` there
    would report a socket error from anywhere inside that loop as a failure to start.
    """


def _port(value: str) -> int:
    # argparse's own type=int lets -1 and 99999 through to bind(), which answers them with
    # an OverflowError traceback where a mistyped port answers with a usage message. the
    # range is checked here so all three shapes of bad port fail the same clean way
    # ValueError is caught rather than left to argparse, which builds its message from
    # type='s own __name__ and would tell the user "invalid _port value"
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("port must be a number, not %r" % value) from None
    if not 0 <= number <= 65535:
        raise argparse.ArgumentTypeError("port must be between 0 and 65535, not %d" % number)
    return number


def _check_not_negative(value: int, label: str) -> None:
    # the "0 disables, a negative number is refused" rule, stated once and used by every
    # CLI validator below and by Server.__init__. `limit and len(buf) >
    # limit` reads any non-zero value as "enabled", and every buffer length is greater
    # than a negative number -- including zero -- so a negative limit reaching that
    # check would close every connection after its first reply while the server still
    # logs a healthy startup line. -1 is a conventional spelling of "unlimited"
    # elsewhere, which makes it the likeliest value to be typed here by someone
    # reaching for the opposite of what it does
    if value < 0:
        raise ValueError(
            "%s cannot be negative; 0 disables the check, not %d" % (label, value))


def _check_schedulable(value: int, label: str, unit: str) -> None:
    # shared by the CLI validators for --snapshot-interval and --expiry-sweep-interval
    # and by Server.__init__ for the same two, the only numeric settings the periodic
    # tick's own arithmetic adds to a clock reading or divides by a constant. a value
    # past this ceiling reaches that arithmetic as an OverflowError instead of a clean
    # refusal -- the same failure mode _check_not_negative exists to prevent at the
    # other end of the range, and the reason this check runs beside it rather than
    # replacing it
    if value > MAX_SCHEDULABLE_INTERVAL:
        raise ValueError(
            "%s cannot exceed %d %s; a larger value cannot be scheduled, not %d"
            % (label, MAX_SCHEDULABLE_INTERVAL, unit, value))


def _load_initial_store(
    snapshot_path: str | None, ignore_snapshot: bool, snapshot_interval: int
) -> Store:
    if snapshot_path is None:
        return Store()
    # a process killed mid-save strands its temporary file where no later save looks
    # for it, and one killed after the fsync may have left a complete snapshot newer than
    # the one about to be loaded, so each is named here and left alone. first, ahead of
    # every refusal below -- a path no save could write, a corrupt snapshot -- so the
    # name is already out when the start is refused, which is when that file is likeliest
    # to be the way back. whether or not saving is on: such files belong to the path and
    # not to the interval, and a directory this process cannot list reports none
    for name in persistence.stale_temporaries(snapshot_path):
        # two sentences, because the two shapes say different things. A name built from
        # the snapshot's own basename can only have come from a save to this path. The
        # bare tmpXXXXXXXX an earlier build left is tempfile's own default and carries
        # nothing about which snapshot it belongs to -- anything on the machine that
        # called mkstemp in this directory leaves the same name -- so telling an operator
        # it may hold their snapshot is a guess presented as a fact
        if persistence.is_legacy_temporary_name(name):
            logger.warning(
                "found %s beside %s: it has the bare temporary-file name an earlier build "
                "of this server left behind, which any program that makes a temporary "
                "file in that directory also produces, so it may be an interrupted save's "
                "snapshot or may have nothing to do with this server; nothing reads or "
                "removes it", name, snapshot_path,
            )
        else:
            logger.warning(
                "found %s beside %s: it has the name a save there gives its temporary "
                "file, so a save was likely interrupted, and it may hold that save's "
                "snapshot; nothing reads or removes it", name, snapshot_path,
            )
    if snapshot_interval:
        # ahead of both the load and --ignore-snapshot: a path no save could write as
        # things stand would otherwise start a server that answers every write and loses
        # them at the next restart, and --ignore-snapshot's warning would promise a
        # replacement no save can make. with saving off nothing is written there, so this
        # check has nothing to refuse -- a directory at the path is still refused, by the
        # load, unless --ignore-snapshot skips the load as well
        persistence.check_writable(snapshot_path)
    if ignore_snapshot:
        if os.path.exists(snapshot_path):
            if snapshot_interval:
                logger.warning(
                    "ignoring the snapshot at %s; it will be replaced at the next "
                    "%d-second interval", snapshot_path, snapshot_interval,
                )
            else:
                logger.warning(
                    "ignoring the snapshot at %s; periodic saving is off, so the file "
                    "is left in place", snapshot_path,
                )
        return Store()
    try:
        return persistence.load(snapshot_path)
    # a missing file is a first run, not a failure, and is treated the same as
    # snapshot_path=None above. anything else persistence.load raises -- a
    # corrupt file -- is left uncaught here, so it propagates out of __init__ and
    # refuses construction rather than starting empty over a snapshot that failed
    # to load and then overwriting it at the next save
    except FileNotFoundError:
        return Store()


def _numeric_limit(value: str, label: str, unit: str) -> int:
    # shared by every CLI validator below, so a value typed on the command line
    # and a value passed straight to Server.__init__ are refused by the same rule
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "%s must be a number of %s, not %r" % (label, unit, value)) from None
    try:
        _check_not_negative(number, label)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None
    return number


def _output_buffer_limit(value: str) -> int:
    return _numeric_limit(value, "output buffer limit", "bytes")


def _max_value_size(value: str) -> int:
    return _numeric_limit(value, "max value size", "bytes")


def _max_multibulk(value: str) -> int:
    return _numeric_limit(value, "max multibulk count", "elements")


def _snapshot_interval(value: str) -> int:
    number = _numeric_limit(value, "snapshot interval", "seconds")
    try:
        _check_schedulable(number, "snapshot interval", "seconds")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None
    return number


def _expiry_sweep_interval(value: str) -> int:
    number = _numeric_limit(value, "expiry sweep interval", "milliseconds")
    try:
        _check_schedulable(number, "expiry sweep interval", "milliseconds")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None
    return number


def build_arg_parser() -> argparse.ArgumentParser:
    # separate from main() so the parser can be inspected without running the server.
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=_port, default=DEFAULT_PORT)
    parser.add_argument(
        "--output-buffer-limit",
        type=_output_buffer_limit,
        default=DEFAULT_OUTPUT_BUFFER_LIMIT,
        metavar="BYTES",
        help="close a connection whose queued replies exceed BYTES; 0 disables the "
             "check, which is the reference's own default for an ordinary client",
    )
    parser.add_argument(
        "--max-value-size",
        type=_max_value_size,
        default=DEFAULT_MAX_VALUE_SIZE,
        metavar="BYTES",
        help="refuse a single inbound bulk element -- including the command name and "
             "any key -- declaring more than BYTES; 0 disables the check",
    )
    parser.add_argument(
        "--max-multibulk",
        type=_max_multibulk,
        default=DEFAULT_MAX_MULTIBULK,
        metavar="COUNT",
        help="refuse a command declaring more than COUNT elements; 0 disables the check",
    )
    parser.add_argument(
        "--snapshot-path",
        default=DEFAULT_SNAPSHOT_PATH,
        metavar="PATH",
        help="load a snapshot from PATH on startup and save to it every "
             "--snapshot-interval; refuses to start if what is at PATH cannot be loaded, "
             "unless --ignore-snapshot is given, and, whenever saving is on, if a save "
             "could not write PATH as things stand at startup -- a missing or read-only "
             "directory, or a directory standing at PATH",
    )
    parser.add_argument(
        "--snapshot-interval",
        type=_snapshot_interval,
        default=DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
        metavar="SECONDS",
        help="save a snapshot to --snapshot-path every SECONDS; 0 disables saving",
    )
    parser.add_argument(
        "--expiry-sweep-interval",
        type=_expiry_sweep_interval,
        default=DEFAULT_EXPIRY_SWEEP_INTERVAL_MS,
        metavar="MILLISECONDS",
        help="run the expiry sweep no sooner than every MILLISECONDS; on an otherwise idle "
             f"loop, no later than that plus one {SELECT_TIMEOUT_SECONDS} second select() "
             "timeout -- a snapshot save or a long command holding the loop delays it "
             "further; not a promise about any one key: a key nobody looks up is reclaimed "
             "on some later pass rather than at its deadline; 0 disables the sweep",
    )
    parser.add_argument(
        "--ignore-snapshot",
        # no default= naming DEFAULT_IGNORE_SNAPSHOT, unlike every flag above: store_true
        # already defaults to False, which is that constant's own value, so nothing here
        # reads it -- Server.__init__ is what actually consults DEFAULT_IGNORE_SNAPSHOT,
        # for a caller that builds a Server directly instead of going through this parser
        action="store_true",
        help="start with an empty keyspace instead of loading --snapshot-path: the "
             "snapshot is ignored whether or not it is readable, so a good one is "
             "discarded too, and the next --snapshot-interval overwrites it -- unless "
             "--snapshot-interval is 0, which leaves the file in place instead. the "
             "escape hatch for a file that refuses to load",
    )
    return parser


class Server:
    def __init__(
        self,
        port: int,
        output_buffer_limit: int = DEFAULT_OUTPUT_BUFFER_LIMIT,
        max_value_size: int = DEFAULT_MAX_VALUE_SIZE,
        max_multibulk: int = DEFAULT_MAX_MULTIBULK,
        # None, not DEFAULT_SNAPSHOT_PATH: the three defaults below mirror what the CLI
        # already defaults to, but a constructor default of ./dump.mrdb would write into
        # whatever directory happens to be current when something builds a Server
        # directly, which is every test in this suite and anything embedding it
        snapshot_path: str | None = None,
        snapshot_interval: int = DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
        expiry_sweep_interval: int = DEFAULT_EXPIRY_SWEEP_INTERVAL_MS,
        ignore_snapshot: bool = DEFAULT_IGNORE_SNAPSHOT,
    ) -> None:
        # both checked here as well as in the parser, for the same reason: Server is
        # constructed directly by tests and will be by anything embedding this, so a
        # validator that lives only on the CLI is a validator with a door beside it. a
        # negative limit reaching _flush closes every connection after its first reply
        # rather than failing at startup, and a port outside the range reaches bind()
        # and answers with an OverflowError traceback
        if not 0 <= port <= 65535:
            raise ValueError("port must be between 0 and 65535, not %d" % port)
        self.port = port
        _check_not_negative(output_buffer_limit, "output_buffer_limit")
        # a per-connection ceiling on queued replies, not a process-wide one: what this
        # bounds is one client's ability to make the server hold bytes it has not managed
        # to send, and connections do not share a write buffer to divide between them
        self.output_buffer_limit = output_buffer_limit
        _check_not_negative(max_value_size, "max_value_size")
        self.max_value_size = max_value_size
        _check_not_negative(max_multibulk, "max_multibulk")
        self.max_multibulk = max_multibulk
        _check_not_negative(snapshot_interval, "snapshot_interval")
        _check_schedulable(snapshot_interval, "snapshot_interval", "seconds")
        self.snapshot_interval = snapshot_interval
        _check_not_negative(expiry_sweep_interval, "expiry_sweep_interval")
        _check_schedulable(expiry_sweep_interval, "expiry_sweep_interval", "milliseconds")
        self.expiry_sweep_interval = expiry_sweep_interval
        self._sweep_interval_seconds = expiry_sweep_interval / _MILLISECONDS_PER_SECOND
        self.snapshot_path = snapshot_path
        self.ignore_snapshot = ignore_snapshot
        # the last validation before self._loop below: every refusal in this constructor
        # lands before the selector opens, so a refused construction -- here, a corrupt
        # snapshot or a path no save could write -- leaks no descriptor for the caller
        # to close
        self._store = _load_initial_store(snapshot_path, ignore_snapshot, snapshot_interval)
        self._connections: set[Connection] = set()
        self._loop = EventLoop(
            self._on_accept, self._on_readable, self._on_writable, SELECT_TIMEOUT_SECONDS
        )
        self._running = False
        self._ran = False
        # None means "not scheduled": a Server constructed and left unrun fires neither
        # arm, and an arm whose interval is 0 -- or, for the snapshot, with no path --
        # never gets a deadline in the first place. _arm_periodic_tasks() sets these
        # from time.monotonic() once run() actually starts it, and run() sets both back
        # to None on its way out
        self._next_sweep_at: float | None = None
        self._next_snapshot_at: float | None = None

    @property
    def connected_clients(self) -> int:
        # public and read-only, not a plain attribute a handler could read as
        # conn.server._connections: a private reach from commands/ into this module's own
        # state is the same category of defect the store's own internals check exists to
        # prevent, sitting just outside the set that check scans. A property rather than a
        # method because the connection's own slot for this object grants attribute reads
        # and nothing else -- no calls -- so what it exposes has to already be shaped as one
        return len(self._connections)

    @property
    def armed_snapshot_interval(self) -> int:
        # the interval saves are scheduled at: snapshot_interval while run() has the save
        # armed, and 0 before run() arms it, after run() returns, and throughout when
        # _arm_periodic_tasks() arms nothing -- an interval of 0, or no snapshot path.
        # CONFIG GET answers `save` from this rather than from the two settings, so a Server
        # whose loop is driven without run(), as the tests drive one, or one that has
        # stopped, says it saves nothing. A property for the reason connected_clients is
        # one: the slot grants attribute reads
        return self.snapshot_interval if self._next_snapshot_at is not None else 0

    def _open_listener(self) -> socket.socket:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind((LISTEN_HOST, self.port))
        except OSError as exc:
            # the one startup step that fails after construction, and a port already in
            # use is the ordinary way it does: starting a second instance by accident.
            # Every other startup refusal this server has -- a corrupt snapshot, an
            # unwritable path, a negative interval -- exits with one line, and this one
            # alone unwound as a traceback out of main(), which reads as a crash rather
            # than as the operator's own mistake. the socket is closed here because
            # nothing else will: run() has not reached the assignment that would
            listener.close()
            raise ListenFailed(
                "cannot listen on %s:%d: %s" % (LISTEN_HOST, self.port, exc)) from exc
        listener.setblocking(False)
        listener.listen(socket.SOMAXCONN)
        return listener

    def _on_accept(self, listener: socket.socket) -> None:
        try:
            sock, addr = listener.accept()
        except OSError:
            # a peer aborting between readiness and accept is normal, and no per-connection boundary can cover this because no connection exists yet; EMFILE arrives here too and looks identical
            return
        # one accept per readable event, so the level-triggered readiness re-reports a remaining backlog on the next select() return.
        sock.setblocking(False)
        conn = Connection(sock, addr, role=Role.CLIENT)
        try:
            # register before tracking, so a connection is never in the set while unregistered, which would raise from the selector partway through shutdown
            self._loop.register(conn)
        except (KeyError, ValueError, OSError):
            # measured: the selector raises KeyError for a descriptor already registered and ValueError for a closed one, and the kqueue backend's own control call raises OSError. this callback is outside _guard, which takes a connection and closes it -- here nothing tracks this socket yet, so it is closed on the spot or never, and letting the raise out of run_once costs every other client
            logger.exception("dropping %s, which could not be registered", addr)
            conn.close()
            return
        self._connections.add(conn)
        # filled only once the connection is both registered and tracked, so this slot
        # never points at a connection that _on_accept is about to close instead of keep
        conn.server = self

    def _on_readable(self, conn: Connection) -> None:
        self._guard(conn, self._read_and_dispatch)

    def _on_writable(self, conn: Connection) -> None:
        # the drain has two entry points and one body, so the write-interest decision is taken in exactly one place
        self._guard(conn, self._flush)

    def _guard(self, conn: Connection, step: Callable[[Connection], None]) -> None:
        # a single-threaded loop turns every uncaught exception into a total outage for every client, which is why this boundary is mandatory rather than defensive -- BlockingIOError and InterruptedError are named first because both are OSError subclasses
        try:
            step(conn)
        except (BlockingIOError, InterruptedError):
            return
        except Exception:
            # not wrapped: measured, handleError swallows the OSError family, so a full disk or a gone pipe cannot raise here. a closed stream raises ValueError straight through it, which nothing here can produce -- no handler is configured and nothing closes stderr
            logger.exception("closing %s after an unhandled exception", conn.addr)
            self._abandon(conn)

    def _guard_task(self, what: str, step: Callable[[], None]) -> None:
        # the periodic tasks' own boundary: _guard above takes a Connection and
        # _abandons it on failure, which a periodic task has none of. one OSError from
        # the snapshot path -- a full disk -- would otherwise reach the top of the only
        # thread this server has
        # no BlockingIOError/InterruptedError carve-out to match _guard's: those name a
        # non-blocking socket operation that just needs retrying on the next readiness
        # event, and neither periodic task touches a socket -- a blocked write here is a
        # slow disk, not a signal to come back later
        try:
            step()
        except Exception:
            logger.exception("%s failed", what)

    def _abandon(self, conn: Connection) -> None:
        # the one place a close that itself fails is handled, reached from the boundary above and from the shutdown sweep, so a failing close ends the same way wherever it is noticed. every statement in _close can raise, and a raise from the boundary's own recovery reaches the top of the only thread this server has
        try:
            self._close(conn)
        except Exception:
            logger.exception("closing %s failed; abandoning it", conn.addr)
            # the connection cannot be tracked any more, so drop it and take the descriptor back
            self._connections.discard(conn)
            # _close clears this on its own happy path, right after the same discard;
            # reaching here means it raised before getting there, so the slot is still
            # pointing at this Server unless this recovery clears it too
            conn.server = None
            try:
                # _close does this before the socket close; reaching here means it raised
                # first, so the registration is still in the selector's map keyed by a
                # descriptor about to be freed. the kernel hands that number to the next
                # accept, and registering it raises "already registered" -- which
                # _on_accept catches and turns into a refused, entirely unrelated client
                self._loop.unregister(conn)
            except Exception:
                # broad, unlike _on_accept's named three, because this is the last resort:
                # _close has already failed once and anything escaping here reaches the top
                # of the only thread this server has. the registration stays leaked in that
                # case, which is the lesser of the two outcomes
                logger.exception("deregistering %s failed", conn.addr)
            try:
                conn.close()
            except OSError:
                # close() reaches nothing but the socket, so this is the only family it can raise
                logger.exception("releasing %s's descriptor failed", conn.addr)

    def _read_and_dispatch(self, conn: Connection) -> None:
        if not conn.receive():
            self._close(conn)
            return
        try:
            parsed_commands = conn.take_commands(
                max_value_size=self.max_value_size, max_multibulk=self.max_multibulk
            )
        except BatchProtocolError as exc:
            # everything take_commands() had already parsed off this batch is answered
            # first, then the error, then the close -- exc.commands is what makes that
            # possible, since the list take_commands() built is otherwise a local this
            # raise would discard along with its frame
            self._dispatch_batch(conn, exc.commands)
            if conn.closed:
                # dispatching those commands can itself trip --output-buffer-limit and
                # close the connection, and an error queued onto it after that is a
                # reply nobody reads. _flush and _close both already guard on
                # this same flag, so nothing today makes this branch observable --
                # kept because it states the intent at the line, and because it is
                # what keeps this arm correct if anything reaching the wire is ever
                # added after it
                return
            # exc.message is already the RESP error body; delivery is best-effort since the connection closes right after, so a short send() drops the tail of a message the peer was about to lose anyway
            conn.queue(resp.encode_error(exc.message))
            self._flush(conn)
            self._close(conn)
            return
        self._dispatch_batch(conn, parsed_commands)

    def _dispatch_batch(self, conn: Connection, parsed_commands: list[list[bytes]]) -> None:
        # every command take_commands() returns is dispatched: level-triggered readiness re-reports unread socket bytes, not commands already taken out of the buffer, so a leftover here is never revisited and the client waits forever
        for argv in parsed_commands:
            response, effects = commands.dispatch(self._store, conn, argv)
            # drained once per command and before the reply is queued: the queue holds
            # effects the lookups inside THIS command produced, so they precede the
            # command's own -- an INCR that lazily expired its key must be preceded by the
            # DEL, or a follower ends with the key absent while this server holds the new
            # value. both lists are discarded because nothing propagates yet -- that lands
            # in a later feature -- and the drain still runs because lazy expiry fills the
            # queue from here on and nothing else would ever empty it
            self._store.take_effects()
            conn.queue(response)
            # one recv can carry thousands of commands, and queueing every reply before
            # the first send is what lets a few kilobytes of request commit gigabytes.
            # the limit has to be consulted here as well as after the batch, or it bounds
            # only what survives the flush and not the peak that got there
            if (self.output_buffer_limit
                    and len(conn.write_buffer) > self.output_buffer_limit):
                # drained before it is judged: a client reading normally can outrun any
                # limit on a long enough pipeline, and closing it for the depth of its
                # batch rather than for failing to read is not what the limit is for.
                # _flush closes it if the buffer is still over once the kernel is done --
                # which makes this comparison a trigger and not the decision, so its exact
                # boundary is not observable. mutating it to >= changes only how early the
                # flush happens, and _flush's own > still decides. mutation testing reports
                # that as a surviving mutant; it is an equivalent one, noted so the next
                # reader spends no time on it
                self._flush(conn)
                if conn.closed:
                    return
        # one flush for the whole batch, not one per command: N replies concatenate into one buffer and N syscalls buy nothing
        self._flush(conn)

    def _flush(self, conn: Connection) -> None:
        if conn.closed:
            return
        if not conn.flush():
            self._close(conn)
            return
        # checked after the send, so what it measures is what the kernel would not take
        # rather than what was queued a moment ago. closing rather than throttling is the
        # whole design: refusing to read a client that has stopped reading cannot slow it
        # down, because a client that writes its requests before reading any reply then
        # blocks in send() waiting for room only its own reading would create, and both
        # sides wait forever. the reference closes here too. replication will need its own
        # links exempted from this, on the same reasoning that keeps them off the rate
        # limiter -- a follower that falls behind is not a client that has stopped reading
        if self.output_buffer_limit and len(conn.write_buffer) > self.output_buffer_limit:
            logger.warning(
                "closing %s: %d bytes of queued replies exceeds the %d byte limit",
                conn.addr, len(conn.write_buffer), self.output_buffer_limit,
            )
            self._close(conn)
            return
        # write interest tracks the buffer's emptiness exactly: a connection left permanently writable spins the loop at 100% CPU without dropping a single reply
        self._loop.set_write_interest(conn, bool(conn.write_buffer))

    def _close(self, conn: Connection) -> None:
        # idempotent, because the protocol-error path closes twice: _flush closes on a failed send and the caller closes again, and a second unregister raises from inside _guard's own recovery -- the one exception that escapes the boundary and takes the process with it
        if conn.closed:
            return
        # a queued reply is owed to the client and conn.close() discards it, so take whatever the kernel will still accept. not wrapped: measured, flush() catches BlockingIOError and OSError below this point and returns False rather than raising, even on a socket whose peer is gone
        conn.flush()
        # unregister before closing: fileno() is -1 once the socket is closed, and the selector then finds the registration only by scanning its whole map for a matching object.
        self._loop.unregister(conn)
        # discarded before the close rather than after it: close() sets the flag first, so a raise from the socket would leave this connection in the set with every later _close returning at the guard above
        self._connections.discard(conn)
        # cleared before close() for the same reason: a raise from the socket would
        # otherwise leave this connection pointing at a Server it is no longer part of,
        # with every later _close returning at the guard above before reaching this line
        conn.server = None
        conn.close()

    def _tick(self) -> None:
        # each deadline is re-taken from the clock rather than advanced from the one it missed:
        # a save blocks this thread until the whole keyspace is serialized, which on a large one
        # outlasts several sweep intervals, and firing every skipped sweep back to back the
        # moment it returns would land the whole backlog in the p99 the sampling algorithm
        # exists to protect
        #
        # the two arms re-arm at different points around their own task for the same reason they
        # can miss an interval at all: the sweep is bounded by its own millisecond budget, so
        # re-arming it from this tick's own reading and then running it inside that budget keeps
        # the two readings within a millisecond of each other either way. a save has no such
        # bound -- on a keyspace large enough, it blocks this thread for longer than the interval
        # itself -- so re-arming it from a reading taken before it runs would count the save's own
        # duration against the next interval, running saves back to back exactly as the reference
        # does not: redis-server counts a save's interval from its completion (server.c's cron
        # compares unixtime against lastsave, and rdb.c sets lastsave when the save finishes), and
        # CONFIG GET save publishes that same schedule. re-arming the snapshot deadline from a
        # fresh reading taken after the save returns is what keeps this server's schedule honest
        # against what it claims
        now = time.monotonic()
        if self._next_sweep_at is not None and now >= self._next_sweep_at:
            self._next_sweep_at = now + self._sweep_interval_seconds
            self._guard_task("expiry sweep", self._sweep_expired)
        if self._next_snapshot_at is not None and now >= self._next_snapshot_at:
            self._guard_task("snapshot save", self._save_snapshot)
            self._next_snapshot_at = time.monotonic() + self.snapshot_interval

    def _sweep_expired(self) -> None:
        # bounded by elapsed time and nothing else -- no iteration cap -- because a
        # sample's own cost is not the quantity the 1 ms budget protects; an iteration
        # cap bounds dict lookups, and if one sampling step ever turns expensive, only a
        # time budget still bounds the stall this puts in the p99
        deadline = time.monotonic() + SWEEP_BUDGET_SECONDS
        try:
            while time.monotonic() < deadline:
                sampled, expired = self._store.sample_and_expire(SWEEP_SAMPLE_SIZE)
                # sampled == 0 is checked before the division to its right, not only to stop
                # on an empty expiry index: without it, an empty index divides zero by zero
                # on that clause instead of breaking out of the loop
                if sampled == 0 or expired / sampled <= SWEEP_RELOOP_THRESHOLD:
                    break
        finally:
            # in a finally, and not the loop's last line: a later pass raising -- the
            # sampler itself, or a value the sweep does not otherwise touch -- would
            # otherwise leave an earlier pass's DELs on the queue for whichever command
            # dispatches next to be blamed for, exactly the misattribution this drain
            # running once per tick rather than once per dispatched command exists to
            # prevent. server.py's other drain, inside _dispatch_batch, runs once per
            # dispatched command
            self._store.take_effects()

    def _save_snapshot(self) -> None:
        # runs whether or not the keyspace changed since the last save -- no dirty-key
        # counter, so a save is a flat cost paid on the interval rather than a decision
        # self.snapshot_path is never None here: _tick() only reaches this call once
        # _next_snapshot_at holds a deadline, and _arm_periodic_tasks() only ever sets
        # that deadline once snapshot_path is confirmed not None
        persistence.save(self._store, self.snapshot_path)

    def _arm_periodic_tasks(self) -> None:
        # its own method for the same reason _tick() already is one: callable on its
        # own, so a test can exercise one arming guard without driving run()'s loop
        # taken once, in this method, rather than inside _tick: both arms' first
        # deadlines are relative to when the server actually starts running, not to
        # when it was built
        now = time.monotonic()
        if self.expiry_sweep_interval:
            self._next_sweep_at = now + self._sweep_interval_seconds
        # snapshot_interval alone is not enough in this method: its default is a
        # nonzero 60 while snapshot_path defaults to None, so a Server built with no
        # path and otherwise left at its defaults still has a nonzero interval, and
        # only this second check keeps that combination from arming a save with
        # nowhere to write it
        if self.snapshot_interval and self.snapshot_path is not None:
            self._next_snapshot_at = now + self.snapshot_interval

    def _request_stop(self, signum, frame) -> None:
        self._running = False

    def _shutdown(self, listener: socket.socket) -> None:
        try:
            for conn in list(self._connections):
                # _abandon rather than _close: this sweep runs after the loop has exited, where _guard no longer applies, so one connection that cannot be closed would otherwise strand every connection after it in iteration order with its descriptor still open
                self._abandon(conn)
        finally:
            # the sweep is still nested, so anything escaping it that _abandon does not answer for cannot cost the port and the selector
            try:
                listener.close()
            finally:
                # nested so neither close can be skipped by the other one failing
                self._loop.close()

    def run(self) -> None:
        # the selector is built in __init__ and closed on the way out, so a second run fails inside selectors with an error naming kqueue rather than the reuse
        if self._ran:
            raise RuntimeError("this Server has already run; construct a new one")
        # raised before the handlers exist, because a signal delivered between installing them and this line would be cleared by the handler and then overwritten here.
        self._running = True
        # restored on the way out: run() is also called in-process, and a handler left pointing at a discarded Server swallows every later signal in that process
        restore = [
            (signal.SIGINT, signal.signal(signal.SIGINT, self._request_stop)),
            (signal.SIGTERM, signal.signal(signal.SIGTERM, self._request_stop)),
        ]
        try:
            # inside the try, so every way out of run() -- a stop, a failed bind, a raise --
            # passes the finally below that disarms both again; and below the if self._ran:
            # check above, so a second run() on an already-ran server hits that raise before
            # arming anything, rather than announcing a schedule this call will never honour.
            # ahead of the listener is not itself load-bearing: nothing accepts a connection
            # until the loop runs, so moving this call after _open_listener() would not be
            # observable
            self._arm_periodic_tasks()
            listener = self._open_listener()
            # set once the listener is open, because what a second run must not reuse is the selector, and nothing has touched it yet -- a failed bind leaves this instance usable
            self._ran = True
            self._loop.register_listener(listener)
            # the bound address, not self.port: --port 0 asks the kernel to choose, and this is the only way to learn what it chose. printed rather than logged because it is not a diagnostic -- a foreground server that says nothing on success is indistinguishable from one that died, and the documented way to check this one is alive is a round trip on a port another Redis may already own, which answers either way
            host, port = listener.getsockname()[:2]
            print(f"listening on {host}:{port}", flush=True)
            try:
                while self._running:
                    self._loop.run_once()
                    # every iteration, not only when a socket was ready: run_once()
                    # itself returns at least every SELECT_TIMEOUT_SECONDS even with
                    # nothing to read or write, which is what lets an idle server with no
                    # connections still sweep and save on schedule
                    self._tick()
            finally:
                self._shutdown(listener)
        finally:
            # nothing is scheduled once run() is on its way out, however it got here, and
            # armed_snapshot_interval reads the save deadline: left set, CONFIG GET save
            # would go on answering a schedule for a server that will never save again. a
            # run() retried after a failed bind arms both afresh
            self._next_sweep_at = None
            self._next_snapshot_at = None
            # unwound rather than looped: a signal delivered during one restore raises out of a flat loop's body, and every handler after it stays bound to a discarded Server -- the exact leak the restore exists to prevent. ExitStack runs all of them and still re-raises the first, with no except clause of its own
            with contextlib.ExitStack() as stack:
                for signum, handler in restore:
                    stack.callback(signal.signal, signum, handler)


def main(argv=None) -> None:
    args = build_arg_parser().parse_args(argv)
    try:
        server = Server(
            args.port, args.output_buffer_limit, args.max_value_size, args.max_multibulk,
            snapshot_path=args.snapshot_path,
            snapshot_interval=args.snapshot_interval,
            expiry_sweep_interval=args.expiry_sweep_interval,
            ignore_snapshot=args.ignore_snapshot,
        )
    except persistence.SnapshotError as exc:
        # this specific exception, not a bare Exception: anything else raised while
        # constructing a Server -- a bug, an unrelated OSError -- is left to crash with
        # its own traceback rather than being folded into a one-line CLI message that
        # was never meant to describe it
        # exit 1 rather than argparse's 2: a corrupt snapshot is not a usage error
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
    try:
        server.run()
    except ListenFailed as exc:
        # its own exception type, not OSError: run() is the whole event loop, and an
        # OSError raised anywhere inside it -- a selector, a socket, a bug -- would be
        # folded into a one-line CLI message that was never meant to describe it. Only
        # the bind raises this
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
