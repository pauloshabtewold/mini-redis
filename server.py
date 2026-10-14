"""Entry point: CLI flags, signal handling, the event loop's three callback bodies, and the periodic tick that runs after every select() return."""

import argparse
import contextlib
import errno
import ipaddress
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
import ratelimit
from connection import BatchProtocolError, Connection, Role, unread_in_kernel
from event_loop import EventLoop
from store import Store

# set by main()'s handler, consumed by run() after its own are armed: the parse and the snapshot
# load run first, and PID 1 gets no default-disposition SIGTERM. SIGTERM only, so Ctrl-C still aborts a load
_stop_requested_during_startup = False


def _note_stop_requested_during_startup(signum, frame) -> None:
    global _stop_requested_during_startup
    _stop_requested_during_startup = True


def _consume_stop_requested_during_startup() -> bool:
    # read and cleared in one step. main() calls it on the way in and on every way out, so a request
    # from a startup that never reached run() is not left set for the next Server in the process
    global _stop_requested_during_startup
    requested = _stop_requested_during_startup
    _stop_requested_during_startup = False
    return requested


DEFAULT_PORT = 6379
# loopback by default: there is no authentication, so whatever reaches the port owns every key
LISTEN_HOST = "127.0.0.1"
# bounds how long a stop takes to be noticed, and on an idle loop floors both periodic intervals:
# a deadline is checked only when run_once() returns, and the default sweep interval equals it
SELECT_TIMEOUT_SECONDS = 0.1
# the most one connection may hold queued once the kernel has taken what it will, then it is closed
# a divergence: the reference leaves an ordinary client unlimited, and a stored value can exceed it
DEFAULT_WRITE_BUFFER_LIMIT = 32 * 1024 * 1024
# above this a connection stops being read, and is read again at the low-water mark; 0 disables it
# two marks because equal ones would pause and resume on every event; it bounds across reads, not one
DEFAULT_WRITE_BUFFER_HIGH_WATER = 1024 * 1024
DEFAULT_WRITE_BUFFER_LOW_WATER = 256 * 1024
# how long a connection may sit on a partly sent command, which no size cap bounds; 0 turns it off
# it bounds the time and not the sum -- inside the limit those read buffers still add up
DEFAULT_INCOMPLETE_COMMAND_TIMEOUT_SECONDS = 30
# applied by _parse_and_run() and nowhere else: a module that configures logging at import steals it
DEFAULT_LOG_LEVEL = "INFO"
# a divergence: proto-max-bulk-len is 512 MiB there. bounds every inbound bulk element, the command
# name and any key included, not only what a human would call the value
DEFAULT_MAX_VALUE_SIZE = 64 * 1024 * 1024
# this project's own: the reference has no multibulk default and refuses only above INT_MAX
DEFAULT_MAX_MULTIBULK = 1024 * 1024
# relative to the process's cwd, not to this file: a server started elsewhere reads its snapshot there
DEFAULT_SNAPSHOT_PATH = "./dump.mrdb"
# a divergence: the reference's policy is three changes-based tiers, this is one flat interval
DEFAULT_SNAPSHOT_INTERVAL_SECONDS = 60
# refusing to start over a corrupt file is the default; --ignore-snapshot is the
# explicit escape hatch for bringing a server back up past one anyway
DEFAULT_IGNORE_SNAPSHOT = False
# matches the cadence an idle redis-server 7.2.7 runs its background cycle at: hz is 10
# there by default, so the cycle runs every 100 ms
DEFAULT_EXPIRY_SWEEP_INTERVAL_MS = 100
# long enough for a reader that is only slow, short enough to stay inside docker stop's ten seconds
DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 5
# must not fall under 50: redis-benchmark -c 50 is standard, and a lower cap tests the cap instead
# a cap of exactly 50 admits 50, since a count equal to the cap is full
DEFAULT_MAX_CONNECTIONS = 1024
# off, and the one default here that is off rather than a value: with a limit on, redis-benchmark
# at -c 50 trips it on the first batch and the published benchmark would measure the rejection path
DEFAULT_RATE_LIMIT = 0
# the window the limit is counted over. a duration, so time.monotonic() deals in it and not the
# wall clock the expiry deadlines use
DEFAULT_RATE_LIMIT_WINDOW_SECONDS = 1
# the reference's own numbers, read off 7.2.7's expire.c:109-111: 20 keys a pass and a 1 ms budget
# its 25 per cent is not reused -- SWEEP_RELOOP_THRESHOLD below is a share of the sample, not of a tick
SWEEP_SAMPLE_SIZE = 20
SWEEP_RELOOP_THRESHOLD = 0.25
SWEEP_BUDGET_SECONDS = 0.001
# how many repeats of one task's failure pass before a line says how many there were, so a task
# failing on every tick writes one line and then a short line per this many, not one per interval
FAILURE_REPEATS_PER_LINE = 100
# the cap's counterpart, ten times larger because a retrier is refused as fast as it can connect
# where a periodic task fails at most once an interval
REFUSALS_PER_LINE = 1000
# how long refusals must stop before the next is reported in full. a close cannot end an episode:
# a freed slot is what a retrier waits for, so one comes between every pair of refusals
REFUSAL_EPISODE_GAP_SECONDS = 60
# the one site --expiry-sweep-interval's milliseconds are turned into the seconds
# time.monotonic() deals in
_MILLISECONDS_PER_SECOND = 1000
# the ceiling _check_schedulable refuses past, so an absurd interval is a startup refusal and not an
# OverflowError a tick later. 2**63 - 1 is this project's convention, far below where float breaks
MAX_SCHEDULABLE_INTERVAL = 2**63 - 1

# importing this module configures nothing: logging.lastResort writes WARNING and above to stderr,
# and _parse_and_run() is the one place that configures it
logger = logging.getLogger(__name__)


class ListenFailed(OSError):
    """The listening socket could not be bound, so this server never started.

    An `OSError` subclass, because a failed bind has always raised `OSError` and a caller
    that catches one still catches this. A named subclass, because `_parse_and_run()`, which
    `main()` reaches, catches it around `run()` and `run()` is the whole event loop: catching
    plain `OSError` there would report a socket error from anywhere inside that loop as a
    failure to start.
    """


def _port(value: str) -> int:
    # checked here because argparse's type=int lets -1 and 99999 reach bind(), which answers with an
    # OverflowError traceback; ValueError would make argparse say 'invalid _port value'
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("port must be a number, not %r" % value) from None
    if not 0 <= number <= 65535:
        raise argparse.ArgumentTypeError("port must be between 0 and 65535, not %d" % number)
    return number


# the ending of every negative-value refusal but three: for those flags 0 is what turns the check off
_ZERO_DISABLES = "0 disables the check, not %d"
# --shutdown-drain-timeout's own, because its 0 is restrictive: no read pass, so -1 meaning
# 'unlimited' would be steered to the opposite. the setup pass and both backlog sweeps still run
_ZERO_SKIPS_THE_READ_LOOP = "%d does not mean unlimited, and 0 means no drain pass"
# --write-buffer-low-water's own, because its 0 disables nothing: with the pause on it means resume
# only once the queue is empty, the most conservative resume
_ZERO_RESUMES_WHEN_EMPTY = "%d does not turn anything off, and 0 means resume only when the queue is empty"
# --snapshot-interval's own, because its 0 stops the periodic save alone: a clean stop still saves,
# unless --ignore-snapshot is set with it
_ZERO_STOPS_THE_PERIODIC_SAVE = "%d does not turn saving off, and 0 stops only the periodic save"
# the window takes no 0, where --rate-limit's 0 disables the limiter: a zero window puts every
# reading instantly outside it, so an operator meaning 'off' would get one that never fires
_WINDOW_TAKES_NO_ZERO = "%d is not a window, and --rate-limit 0 is what disables the limiter"
# terse and lowercase after the code, like every other error here, and it names no figure:
# the limit is the operator's configuration and not the client's business
_RATE_LIMITED = b"ERR rate limit exceeded"


# a value that is not an int gets past every check below, because they are all orderings:
# nan < 0 is False, inf is accepted, and True is 1 -- so a limiter at nan never refuses
def _check_whole_number(value, label: str) -> None:
    # bool before int, because isinstance(True, int) is True and the message has to name what was
    # actually passed rather than call it an integer
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            "%s must be a whole number, not %s" % (label, type(value).__name__))


def _check_not_negative(value: int, label: str, ending: str = _ZERO_DISABLES) -> None:
    # the '0 disables, a negative is refused' rule, stated once for the CLI and Server.__init__:
    # a negative limit reads as enabled and every buffer length exceeds it, so every connection closes
    _check_whole_number(value, label)
    if value < 0:
        raise ValueError(("%s cannot be negative; " + ending) % (label, value))


def _check_rate_limit_window(value: int, label: str) -> None:
    # not _check_not_negative with a different ending, because the rule itself differs:
    # every other numeric setting here admits 0 as "off", and this one admits no 0 at all
    _check_whole_number(value, label)
    if value <= 0:
        raise ValueError(("%s must be positive; " + _WINDOW_TAKES_NO_ZERO) % (label, value))


def _check_schedulable(value: int, label: str, unit: str) -> None:
    # the ceiling for the four durations, so an absurd value is refused instead of reaching the clock
    # arithmetic as an OverflowError. the command timeout only compares, and takes it for one rule
    if value > MAX_SCHEDULABLE_INTERVAL:
        raise ValueError(
            "%s cannot exceed %d %s; a larger value cannot be scheduled, not %d"
            % (label, MAX_SCHEDULABLE_INTERVAL, unit, value))


def _check_water_marks(high: int, low: int, high_label: str, low_label: str) -> None:
    # the rule spanning two settings, stated once for both doors: argparse has no hook for a pair
    # guarded on the high-water mark, whose 0 switches the pause off; >= because equal marks are a zero band
    if high and low >= high:
        raise ValueError(
            "%s (%d) must be below %s (%d); equal marks would pause and resume a "
            "connection on every event, and 0 for %s switches the pause off instead"
            % (low_label, low, high_label, high, high_label))


def _check_limit_above_high_water(
    limit: int, high: int, limit_label: str, high_label: str
) -> None:
    # the second such rule: a non-zero limit at or below a non-zero mark leaves both marks dead,
    # since _flush tests the limit first and returns on the close. guarded on both, each 0 its own
    if limit and high and limit <= high:
        raise ValueError(
            "%s (%d) must be above %s (%d); a limit at or below the mark closes a "
            "connection before the pause can ever fire, so both water marks would do "
            "nothing, and 0 for %s removes the limit instead"
            % (limit_label, limit, high_label, high, limit_label))


def _check_host(value: str, label: str) -> None:
    # stated once for both doors. an empty host is what `--host "$HOST"` becomes when HOST is unset, and
    # bind() reads it as every interface; 0.0.0.0 is accepted, since whoever types it has chosen it
    if not value:
        raise ValueError(
            "%s cannot be empty; an empty host listens on every interface, and 0.0.0.0 "
            "asks for that by name" % label)


def _load_initial_store(
    snapshot_path: str | None, ignore_snapshot: bool, snapshot_interval: int
) -> Store:
    if snapshot_path is None:
        return Store()
    # a save killed mid-write strands its temporary file, and one killed after the fsync may hold a
    # newer snapshot, so each is named and left alone -- first, so the name is out even on a refusal
    for name in persistence.stale_temporaries(snapshot_path):
        # two sentences, because a name built from the snapshot's basename can only be a save to this path,
        # where the bare tmpXXXXXXXX an earlier build left is tempfile's own and names no snapshot
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
        # ahead of the load and --ignore-snapshot: a path no save could write would otherwise answer every
        # write and lose it. at interval 0 nothing checks, so the stop's own save is where it is first found
        persistence.check_writable(snapshot_path)
    if ignore_snapshot:
        if os.path.exists(snapshot_path):
            if snapshot_interval:
                logger.warning(
                    "ignoring the snapshot at %s; it will be replaced at the next "
                    "%d-second interval, or on a clean stop if that comes first",
                    snapshot_path, snapshot_interval,
                )
            else:
                logger.warning(
                    "ignoring the snapshot at %s; periodic saving is off, so the file "
                    "is left in place", snapshot_path,
                )
        return Store()
    try:
        return persistence.load(snapshot_path)
    # a missing file is a first run, not a failure. anything else load raises propagates out of __init__,
    # so a corrupt snapshot refuses construction rather than being overwritten at the next save
    except FileNotFoundError:
        return Store()


def _numeric_limit(value: str, label: str, unit: str, ending: str = _ZERO_DISABLES) -> int:
    # shared by every CLI validator below, so a value typed on the command line
    # and a value passed straight to Server.__init__ are refused by the same rule
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "%s must be a number of %s, not %r" % (label, unit, value)) from None
    try:
        _check_not_negative(number, label, ending)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None
    return number


def _write_buffer_limit(value: str) -> int:
    return _numeric_limit(value, "write buffer limit", "bytes")


def _write_buffer_high_water(value: str) -> int:
    return _numeric_limit(value, "write buffer high water", "bytes")


def _write_buffer_low_water(value: str) -> int:
    return _numeric_limit(value, "write buffer low water", "bytes", _ZERO_RESUMES_WHEN_EMPTY)


LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


def _is_an_ipv6_literal(host: str) -> bool:
    # the zone id is cut first: ipaddress will not parse fe80::1%en0, which is still IPv6 for this
    try:
        return isinstance(ipaddress.ip_address(host.partition("%")[0]), ipaddress.IPv6Address)
    except ValueError:
        return False


def _notes_of(exc: BaseException) -> str:
    # str() renders args and an OSError's filename, never notes, and save() puts one there to say it
    # stranded a full-size temporary file -- without this nothing reports that copy at all
    return "".join("; " + str(note) for note in getattr(exc, "__notes__", ()))


def _log_level(value: str) -> str:
    # folded only when the folded form is a level, so argparse's 'invalid choice' quotes what was typed:
    # type=str.upper refused 'verbose' as 'VERBOSE', naming a value nobody wrote
    folded = value.upper()
    return folded if folded in LOG_LEVELS else value


def _max_value_size(value: str) -> int:
    return _numeric_limit(value, "max value size", "bytes")


def _max_multibulk(value: str) -> int:
    return _numeric_limit(value, "max multibulk count", "elements")


def _snapshot_interval(value: str) -> int:
    number = _numeric_limit(value, "snapshot interval", "seconds", _ZERO_STOPS_THE_PERIODIC_SAVE)
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


def _rate_limit(value: str) -> int:
    return _numeric_limit(value, "rate limit", "requests")


def _rate_limit_window(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("rate limit window must be an integer number of seconds") from None
    try:
        _check_rate_limit_window(number, "rate limit window")
        _check_schedulable(number, "rate limit window", "seconds")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None
    return number


def _shutdown_drain_timeout(value: str) -> int:
    number = _numeric_limit(value, "shutdown drain timeout", "seconds", _ZERO_SKIPS_THE_READ_LOOP)
    try:
        _check_schedulable(number, "shutdown drain timeout", "seconds")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None
    return number


def _max_connections(value: str) -> int:
    return _numeric_limit(value, "max connections", "connections")


def _incomplete_command_timeout(value: str) -> int:
    number = _numeric_limit(value, "incomplete command timeout", "seconds")
    try:
        _check_schedulable(number, "incomplete command timeout", "seconds")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None
    return number


def _host(value: str) -> str:
    try:
        _check_host(value, "host")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None
    return value


def build_arg_parser() -> argparse.ArgumentParser:
    # separate from _parse_and_run() so the parser can be inspected without running the server.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port",
        type=_port,
        default=DEFAULT_PORT,
        help=f"the TCP port to listen on, 0 to 65535; the default is {DEFAULT_PORT}. "
             "0 is not a way to switch anything off here: it asks the kernel to choose a "
             "free port, and the line the server prints once it is listening, listening "
             "on HOST:PORT, names the one it got",
    )
    parser.add_argument(
        "--write-buffer-limit",
        type=_write_buffer_limit,
        default=DEFAULT_WRITE_BUFFER_LIMIT,
        metavar="BYTES",
        help="close a connection whose queued replies still exceed BYTES once the kernel "
             f"has taken what it will; the default is {DEFAULT_WRITE_BUFFER_LIMIT} ({DEFAULT_WRITE_BUFFER_LIMIT // (1024 * 1024)} MiB). 0 means no limit, and nothing is closed for it. "
             "this is a hard limit and it closes a connection for holding too much, "
             "not for failing to read: a client that reads slower than this server "
             "produces can reach it too, because --write-buffer-high-water below stops reading "
             "such a connection but the resume then answers a whole read's worth of the "
             "requests that piled up. whether that batch exceeds BYTES depends on the "
             "size of the replies, so the pause prevents the close for small replies and "
             "delays it for large ones; 0 prevents it in either case. a value stored "
             "under --max-value-size can also be too large to read back under it",
    )
    parser.add_argument(
        "--max-value-size",
        type=_max_value_size,
        default=DEFAULT_MAX_VALUE_SIZE,
        metavar="BYTES",
        help="refuse a single inbound bulk element -- including the command name and "
             f"any key -- declaring more than BYTES; the default is {DEFAULT_MAX_VALUE_SIZE}"
             f" ({DEFAULT_MAX_VALUE_SIZE // (1024 * 1024)} MiB). 0 disables the check",
    )
    parser.add_argument(
        "--max-multibulk",
        type=_max_multibulk,
        default=DEFAULT_MAX_MULTIBULK,
        metavar="COUNT",
        help=f"refuse a command declaring more than COUNT elements; the default is"
             f" {DEFAULT_MAX_MULTIBULK}. 0 disables the check",
    )
    parser.add_argument(
        "--snapshot-path",
        default=DEFAULT_SNAPSHOT_PATH,
        metavar="PATH",
        help="load a snapshot from PATH on startup and save to it every "
             "--snapshot-interval and on a clean stop; refuses to start if what is at "
             "PATH cannot be loaded, unless --ignore-snapshot is given, and, with a "
             "non-zero --snapshot-interval, if a save could not write PATH as things "
             "stand at startup -- a missing or read-only directory, or a directory "
             f"standing at PATH. the default is {DEFAULT_SNAPSHOT_PATH}, resolved against the"
             " directory the server was started in, so leaving this off still writes a file",
    )
    parser.add_argument(
        "--snapshot-interval",
        type=_snapshot_interval,
        default=DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
        metavar="SECONDS",
        help="save a snapshot to --snapshot-path every SECONDS, and on a clean stop; 0 "
             "disables the periodic save only -- a clean stop still saves, except with "
             f"--ignore-snapshot, where 0 leaves the file at --snapshot-path alone. the default"
             f" is {DEFAULT_SNAPSHOT_INTERVAL_SECONDS}. CONFIG GET save reports the schedule in"
             " the reference's own syntax, where a rule fires once a clock is MORE than its"
             " seconds past the last save, so a default server answers 59 0 rather than 60 0",
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
             f"on some later pass rather than at its deadline; the default is"
             f" {DEFAULT_EXPIRY_SWEEP_INTERVAL_MS}. 0 disables the sweep",
    )
    parser.add_argument(
        "--ignore-snapshot",
        # no default= here, unlike every flag above: store_true already defaults to False, which is that
        # constant's own value, and Server.__init__ is what consults it for a direct caller
        action="store_true",
        help="start with an empty keyspace instead of loading --snapshot-path: the "
             "snapshot is ignored whether or not it is readable, so a good one is "
             "discarded too, and the next --snapshot-interval or a clean stop, whichever "
             "comes first, overwrites it -- unless --snapshot-interval is 0, which leaves "
             "the file in place instead: no save runs, not even on the way out. the "
             "escape hatch for a file that refuses to load",
    )
    parser.add_argument(
        "--shutdown-drain-timeout",
        type=_shutdown_drain_timeout,
        default=DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
        metavar="SECONDS",
        help="on SIGTERM, and on SIGINT unless this process inherited it ignored, save a snapshot and then keep sending the replies "
             "already queued for clients, for no longer than SECONDS plus one "
             f"{SELECT_TIMEOUT_SECONDS} second select() timeout, then exit whether "
             "or not the kernel took all of them. that bounds the WAIT and not the "
             "exit: the snapshot save ahead of it, the two sweeps of the accept "
             "backlog, the reads that empty the receive queues and the teardown's "
             "final flush all sit outside it and have no deadline of their own, so "
             "signal-to-exit runs over SECONDS by however much those cost. 0 is not "
             "unlimited here, it is the "
             "opposite: no drain pass, so whatever the kernel will not take in one "
             "pass is discarded, and a request still unread at the close can take the "
             f"kernel's unsent tail with it. the default is"
             f" {DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SECONDS}",
    )
    parser.add_argument(
        "--max-connections",
        type=_max_connections,
        default=DEFAULT_MAX_CONNECTIONS,
        metavar="COUNT",
        help="close, without a reply, a connection that arrives while COUNT clients are "
             f"already connected; the default is {DEFAULT_MAX_CONNECTIONS}. 0 means no limit,"
             " and nothing is refused",
    )
    parser.add_argument(
        "--rate-limit",
        type=_rate_limit,
        default=DEFAULT_RATE_LIMIT,
        metavar="REQUESTS",
        help="answer an error, without dispatching, once a connection has been permitted "
             f"REQUESTS commands inside --rate-limit-window; the default is "
             f"{DEFAULT_RATE_LIMIT}, which disables the limiter. permitted and not sent: a refusal records nothing, so the "
             "budget returns one window after the permits and a refused command never "
             "delays it. the budget is per connection, so a client gets a fresh one by "
             "reconnecting and nothing here bounds how often it may -- --max-connections "
             "bounds how many budgets exist at once, which is not the same thing",
    )
    parser.add_argument(
        "--rate-limit-window",
        type=_rate_limit_window,
        default=DEFAULT_RATE_LIMIT_WINDOW_SECONDS,
        metavar="SECONDS",
        help="the span --rate-limit counts over, sliding rather than fixed, so no burst "
             "straddles a boundary; it takes no 0, because 100 requests per 0 seconds is a "
             f"limiter that permits everything. the default is"
             f" {DEFAULT_RATE_LIMIT_WINDOW_SECONDS}",
    )
    parser.add_argument(
        "--write-buffer-high-water",
        type=_write_buffer_high_water,
        default=DEFAULT_WRITE_BUFFER_HIGH_WATER,
        metavar="BYTES",
        help="stop reading from a connection while more than BYTES of replies are queued "
             "for it, and resume once the queue falls to --write-buffer-low-water; 0 "
             "disables the pause, so reading goes on however much is queued, and "
             f"--write-buffer-low-water is then unused. the default is"
             f" {DEFAULT_WRITE_BUFFER_HIGH_WATER} ({DEFAULT_WRITE_BUFFER_HIGH_WATER // 1024} KiB). a non-zero"
             " --write-buffer-limit must be above BYTES: at or below it the limit closes a"
             " connection before the pause can fire, so both marks would do nothing, and that"
             " pairing is refused",
    )
    parser.add_argument(
        "--write-buffer-low-water",
        type=_write_buffer_low_water,
        default=DEFAULT_WRITE_BUFFER_LOW_WATER,
        metavar="BYTES",
        help="resume reading from a paused connection once its queued replies fall to "
             "BYTES or fewer; must be below --write-buffer-high-water unless that is 0. "
             "0 means resume only when the queue is empty, which is the most "
             f"conservative resume and not a way to switch anything off. the default is"
             f" {DEFAULT_WRITE_BUFFER_LOW_WATER} ({DEFAULT_WRITE_BUFFER_LOW_WATER // 1024} KiB)",
    )
    parser.add_argument(
        "--incomplete-command-timeout",
        type=_incomplete_command_timeout,
        default=DEFAULT_INCOMPLETE_COMMAND_TIMEOUT_SECONDS,
        metavar="SECONDS",
        help="close a connection that has held a partly sent command for more than "
             "SECONDS, counted from when the command began and not from its latest byte. "
             "the count stops while --write-buffer-high-water has the connection paused "
             "and starts over from zero when reading resumes, so time held before a "
             f"pause is not carried across it. the default is"
             f" {DEFAULT_INCOMPLETE_COMMAND_TIMEOUT_SECONDS}. 0 means no limit, and nothing is"
             " closed for it",
    )
    parser.add_argument(
        "--host",
        type=_host,
        default=LISTEN_HOST,
        metavar="HOST",
        help=f"the IPv4 address to listen on, or a name that resolves to one. the default "
             f"is {LISTEN_HOST}, which is loopback, so a server is local-only unless asked "
             "otherwise: it has no "
             "authentication, and whatever can reach the port can read and change every "
             "key. an empty value is refused, since it would listen on every interface; "
             "0.0.0.0 asks for that by name",
    )
    parser.add_argument(
        "--log-level",
        # folded for the comparison, quoted as typed in the refusal: type=str.upper refused 'verbose' as
        # invalid choice: 'VERBOSE', naming a value the operator never wrote
        type=_log_level,
        choices=LOG_LEVELS,
        default=DEFAULT_LOG_LEVEL,
        help="how much to log, in any case. INFO reports each connection opened and "
             "closed; DEBUG adds a line per dispatched command naming the command and "
             "its argument count, never a key or a value. the shutdown drain writes one "
             "line per stop, INFO when no reply was lost and WARNING when one was -- "
             "replies alone decide that, so a stop that threw away requests nobody will "
             "answer is INFO, with the count of their bytes on the same line. WARNING "
             "therefore hides a clean stop's confirmation, the per-connection lines and "
             "the only report there is of discarded requests; ERROR hides the stop line "
             f"either way. the default is {DEFAULT_LOG_LEVEL}",
    )
    return parser


class Server:
    def __init__(
        self,
        port: int,
        write_buffer_limit: int = DEFAULT_WRITE_BUFFER_LIMIT,
        max_value_size: int = DEFAULT_MAX_VALUE_SIZE,
        max_multibulk: int = DEFAULT_MAX_MULTIBULK,
        # None, not DEFAULT_SNAPSHOT_PATH: a constructor default of ./dump.mrdb would write into whatever
        # directory happens to be current, which is every test here and anything embedding this
        snapshot_path: str | None = None,
        snapshot_interval: int = DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
        expiry_sweep_interval: int = DEFAULT_EXPIRY_SWEEP_INTERVAL_MS,
        ignore_snapshot: bool = DEFAULT_IGNORE_SNAPSHOT,
        shutdown_drain_timeout: int = DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        # appended and not placed beside what they resemble: _parse_and_run() binds the first four
        # positionally, so a parameter inserted ahead of one rebinds every positional call site silently
        write_buffer_high_water: int = DEFAULT_WRITE_BUFFER_HIGH_WATER,
        write_buffer_low_water: int = DEFAULT_WRITE_BUFFER_LOW_WATER,
        incomplete_command_timeout: int = DEFAULT_INCOMPLETE_COMMAND_TIMEOUT_SECONDS,
        host: str = LISTEN_HOST,
        # after host for that reason: these two once went ahead of it, moving host from positional 14 to
        # 16, and a fourteen-positional call raised TypeError. no caller here passes that many
        rate_limit: int = DEFAULT_RATE_LIMIT,
        rate_limit_window: int = DEFAULT_RATE_LIMIT_WINDOW_SECONDS,
    ) -> None:
        # checked here as well as in the parser, because Server is built directly by tests and by anything
        # embedding it: a negative limit would close every connection after its first reply instead
        if not 0 <= port <= 65535:
            raise ValueError("port must be between 0 and 65535, not %d" % port)
        self.port = port
        _check_not_negative(write_buffer_limit, "write_buffer_limit")
        # per connection and not process-wide: connections do not share a write buffer to divide
        self.write_buffer_limit = write_buffer_limit
        _check_not_negative(max_value_size, "max_value_size")
        self.max_value_size = max_value_size
        _check_not_negative(max_multibulk, "max_multibulk")
        self.max_multibulk = max_multibulk
        _check_not_negative(snapshot_interval, "snapshot_interval", _ZERO_STOPS_THE_PERIODIC_SAVE)
        _check_schedulable(snapshot_interval, "snapshot_interval", "seconds")
        self.snapshot_interval = snapshot_interval
        _check_not_negative(expiry_sweep_interval, "expiry_sweep_interval")
        _check_schedulable(expiry_sweep_interval, "expiry_sweep_interval", "milliseconds")
        self.expiry_sweep_interval = expiry_sweep_interval
        self._sweep_interval_seconds = expiry_sweep_interval / _MILLISECONDS_PER_SECOND
        self.snapshot_path = snapshot_path
        self.ignore_snapshot = ignore_snapshot
        _check_not_negative(shutdown_drain_timeout, "shutdown_drain_timeout", _ZERO_SKIPS_THE_READ_LOOP)
        _check_schedulable(shutdown_drain_timeout, "shutdown_drain_timeout", "seconds")
        self.shutdown_drain_timeout = shutdown_drain_timeout
        # the limit takes the shared zero rule and the window takes its own: 0 requests is off, and
        # 100 requests per 0 seconds is a limiter that permits everything in silence
        _check_not_negative(rate_limit, "rate_limit")
        self.rate_limit = rate_limit
        _check_rate_limit_window(rate_limit_window, "rate_limit_window")
        _check_schedulable(rate_limit_window, "rate_limit_window", "seconds")
        self.rate_limit_window = rate_limit_window
        _check_not_negative(max_connections, "max_connections")
        self.max_connections = max_connections
        _check_not_negative(write_buffer_high_water, "write_buffer_high_water")
        _check_not_negative(write_buffer_low_water, "write_buffer_low_water", _ZERO_RESUMES_WHEN_EMPTY)
        _check_water_marks(
            write_buffer_high_water, write_buffer_low_water,
            "write_buffer_high_water", "write_buffer_low_water")
        _check_limit_above_high_water(
            write_buffer_limit, write_buffer_high_water,
            "write_buffer_limit", "write_buffer_high_water")
        self.write_buffer_high_water = write_buffer_high_water
        self.write_buffer_low_water = write_buffer_low_water
        _check_not_negative(incomplete_command_timeout, "incomplete_command_timeout")
        _check_schedulable(incomplete_command_timeout, "incomplete_command_timeout", "seconds")
        self.incomplete_command_timeout = incomplete_command_timeout
        _check_host(host, "host")
        self.host = host
        # the last validation before self._loop, so every refusal here lands before the selector opens
        # and leaks no descriptor for the caller to close
        self._store = _load_initial_store(snapshot_path, ignore_snapshot, snapshot_interval)
        self._connections: set[Connection] = set()
        self._loop = EventLoop(
            self._on_accept, self._on_readable, self._on_writable, SELECT_TIMEOUT_SECONDS
        )
        self._running = False
        self._ran = False
        # None means not scheduled: an unrun Server fires neither arm, and an interval of 0 never gets a
        # deadline. _arm_periodic_tasks() sets them once run() starts, and run() clears them on the way out
        self._next_sweep_at: float | None = None
        self._next_snapshot_at: float | None = None
        # per task name: failures in a row, counting the one reported. a success drops the entry, so the
        # next failure after a recovery is reported in full again
        self._failures_in_a_row: dict[str, int] = {}
        # set when the drain begins and never cleared: it turns a read into a discard, and lets end of input
        # on a connection that owes bytes wait rather than close into a reset
        self._draining = False
        # the two cumulative counters INFO publishes and redis-cli --stat reads: the connection set's length
        # is how many there are now, and these are how many there have been
        self._total_connections_received = 0
        self._total_commands_processed = 0
        # refusals in this episode, counting the one reported, and when the last came: a gap longer than
        # REFUSAL_EPISODE_GAP_SECONDS starts the count again
        self._cap_refusals = 0
        self._last_refusal_at: float | None = None
        # inbound bytes a stop threw away undispatched, in five shapes: a read buffer, what a half-received
        # command parsed off it, the drain's reads, an unread receive queue, and the accept backlog
        self._discarded_request_bytes = 0
        # set only while _drain_for is accounting, so a closing connection's unread queue is counted once
        # and the closes _shutdown makes after the line contribute nothing
        self._counting_unread_at_close = False
        # one descriptor held in reserve for the backlog sweeps, released just before the shutdown save, and
        # None otherwise. not opened in __init__: an unrun Server would hold one nothing closes
        self._spare_fd: int | None = None

    def _hold_spare_descriptor(self) -> None:
        if self._spare_fd is not None:
            return
        try:
            self._spare_fd = os.open(os.devnull, os.O_RDONLY)
        except OSError as exc:
            # a full table has nothing to spare and that is no reason to refuse to start: the sweep then runs
            # without a reserve and says what it could not count. no traceback -- rendering one opens files
            logger.warning(
                "could not reserve a descriptor for the shutdown's sweep of the accept backlog: %r", exc)

    def _release_spare_descriptor(self) -> None:
        # idempotent: run() calls it on every way out as well as the sweep calling it before its first accept
        spare, self._spare_fd = self._spare_fd, None
        if spare is not None:
            os.close(spare)

    @property
    def connected_clients(self) -> int:
        # public and read-only, so a handler cannot reach conn.server._connections: the slot grants
        # attribute reads and no calls, so what it exposes has to already be shaped as one
        return len(self._connections)

    @property
    def total_connections_received(self) -> int:
        # every socket accept() returned, the cap's refusals included, which is what 'received' means and
        # what --stat wants: a cap turning a retrier away is traffic, and hiding it reads as an idle server
        return self._total_connections_received

    @property
    def total_commands_processed(self) -> int:
        # every command the loop answered, a rate-limit refusal included: counting only what reached a
        # handler would make a limited client look idle. not the reference's rule, and DESIGN.md says so
        return self._total_commands_processed

    @property
    def armed_snapshot_interval(self) -> int:
        # the interval saves are actually scheduled at: the setting while run() has the arm, 0 before it,
        # after it, and when nothing arms it. CONFIG GET answers `save` from this and not from the settings
        return self.snapshot_interval if self._next_snapshot_at is not None else 0

    @property
    def _ignored_snapshot_is_left_in_place(self) -> bool:
        # the one pairing that skips the stop's own save: with the snapshot ignored and saving off, the file
        # was protected only because nothing wrote it, and the operator rescuing it must not lose it here
        return self.ignore_snapshot and not self.snapshot_interval

    def _open_listener(self) -> socket.socket:
        # the construction is inside the try with the bind: socket() itself fails EMFILE on a full table,
        # and left outside it that raise escaped as the traceback this clause exists to prevent
        listener = None
        try:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.host, self.port))
        except OSError as exc:
            # the one startup step that fails after construction, and a port in use is the ordinary way:
            # every other refusal exits on one line, where this alone unwound as a traceback out of main()
            if listener is not None:
                listener.close()
            # an IPv6 literal gets EAI_NONAME from an AF_INET bind, the same as a misspelled hostname, so it
            # is named. keyed on parsing as one, not on a colon: --host 127.0.0.1:7000 is the likely typo
            if isinstance(exc, socket.gaierror) and _is_an_ipv6_literal(self.host):
                raise ListenFailed(
                    "cannot listen on [%s]:%d: this server listens on IPv4 only, and %s is "
                    "an IPv6 address; pass an IPv4 address or a name that resolves to one"
                    % (self.host, self.port, self.host)) from exc
            raise ListenFailed(
                "cannot listen on %s:%d: %s" % (self.host, self.port, exc)) from exc
        listener.setblocking(False)
        listener.listen(socket.SOMAXCONN)
        return listener

    def _on_accept(self, listener: socket.socket) -> None:
        try:
            sock, addr = listener.accept()
        except OSError as exc:
            # a peer aborting between readiness and accept is normal and silent, and no boundary covers it yet
            # EMFILE is not that: it arrives here too, and is reported through the cap's own bounded path
            if exc.errno in (errno.EMFILE, errno.ENFILE):
                self._log_exhaustion(exc)
            return
        # counted before the cap can refuse it: a refused connection still reached accept() and cost a
        # descriptor, and the traffic a cap turns away is what an operator needs to see
        self._total_connections_received += 1
        # one accept per readable event; level-triggered readiness re-reports any remaining backlog
        # >= where byte limits are >: this asks if there is room for one more, so a count at the cap is full
        if self.max_connections and len(self._connections) >= self.max_connections:
            self._log_refusal(addr)
            # closed with nothing written: a send() here is on a socket nothing tracks, for a client not being
            # kept. before Connection(), so nothing owns the socket but this frame
            sock.close()
            return
        sock.setblocking(False)
        conn = Connection(sock, addr, role=Role.CLIENT)
        try:
            # register before tracking, so a connection is never in the set while unregistered -- which would
            # raise from the selector partway through shutdown
            self._loop.register(conn)
        except (KeyError, ValueError, OSError):
            # measured: KeyError for a descriptor already registered, ValueError for a closed one, OSError
            # from kqueue's control call. nothing tracks this socket yet, so it closes here or never
            logger.exception("dropping %s, which could not be registered", addr)
            conn.close()
            return
        self._connections.add(conn)
        # filled only once the connection is both registered and tracked, so this slot
        # never points at a connection that _on_accept is about to close instead of keep
        conn.server = self
        logger.info("accepted connection %d from %s; %d connected", conn.id, addr, len(self._connections))

    def _log_exhaustion(self, exc: OSError) -> None:
        # bounded exactly as _log_refusal is, through the same counter and gap: a full table refuses every
        # arrival. one episode covers both causes, since the message says which and the count how many
        self._log_capacity_refusal(
            first="could not accept a connection: the descriptor table is full (%r); the "
                  "accept() that failed took the pending connection with it, so the client saw "
                  "a reset and the shutdown's sweep of the accept backlog cannot see it. "
                  "further refusals are logged at DEBUG, with a count every %d, until none has "
                  "come for %d seconds"
                  % (exc, REFUSALS_PER_LINE, REFUSAL_EPISODE_GAP_SECONDS),
            repeat="%d connections refused so far, the most recent because the descriptor "
                   "table is full" % (self._cap_refusals + 1,),
            quiet="could not accept a connection: the descriptor table is full")

    def _log_refusal(self, addr: tuple[str, int]) -> None:
        # bounded by count: a full report for the first, one short line per REFUSALS_PER_LINE, DEBUG for the
        # rest. cleared by a gap and not by a close -- a close is what the retrier is waiting for
        self._log_capacity_refusal(
            first="refusing %s: %d connections is the --max-connections limit; further "
                  "refusals are logged at DEBUG, with a count every %d, until none has come "
                  "for %d seconds"
                  % (addr, self.max_connections, REFUSALS_PER_LINE,
                     REFUSAL_EPISODE_GAP_SECONDS),
            # no address: this line says the refusals are still going, and the first one named who
            repeat="%d connections refused so far at the --max-connections limit of %d"
                   % (self._cap_refusals + 1, self.max_connections),
            quiet="refusing %s: at the --max-connections limit" % (addr,))

    def _log_capacity_refusal(self, first: str, repeat: str, quiet: str) -> None:
        # the counting half, shared with _log_exhaustion so two kinds of refusal cannot warn once per arrival
        # by taking turns: one episode, one counter, one gap, and each caller's own message
        now = time.monotonic()
        if (self._last_refusal_at is not None
                and now - self._last_refusal_at > REFUSAL_EPISODE_GAP_SECONDS):
            self._cap_refusals = 0
        self._last_refusal_at = now
        self._cap_refusals += 1
        if self._cap_refusals == 1:
            logger.warning("%s", first)
        elif self._cap_refusals % REFUSALS_PER_LINE == 0:
            logger.warning("%s", repeat)
        else:
            logger.debug("%s", quiet)

    def _on_readable(self, conn: Connection) -> None:
        self._guard(conn, self._read_and_dispatch)

    def _on_writable(self, conn: Connection) -> None:
        # the drain has two entry points and one body, so write interest is decided in exactly one place
        self._guard(conn, self._flush)

    def _guard(self, conn: Connection, step: Callable[[Connection], None]) -> None:
        # one uncaught exception on a single-threaded loop is a total outage, so this boundary is mandatory
        # BlockingIOError and InterruptedError are named first because both are OSError subclasses
        try:
            step(conn)
        except (BlockingIOError, InterruptedError):
            return
        except Exception:
            # not wrapped: measured, handleError swallows the OSError family, so a full
            # disk cannot raise here, and a closed stream's ValueError is unreachable
            logger.exception("closing %s after an unhandled exception", conn.addr)
            self._abandon(conn)

    def _guard_task(self, what: str, step: Callable[[], None]) -> None:
        # the periodic tasks' boundary: _guard takes a Connection and a task has none, so one OSError from
        # the save would reach the top of the only thread. bounded, because a task fails on a timer
        try:
            step()
        except Exception as exc:
            failures = self._failures_in_a_row.get(what, 0) + 1
            self._failures_in_a_row[what] = failures
            if failures == 1:
                # the error in the message and no traceback: rendering one opens files, and a
                # full table is how these fail. the class, str(exc), and save()'s own notes
                logger.error("%s failed: %s: %s%s", what, type(exc).__name__, exc, _notes_of(exc))
            elif failures % FAILURE_REPEATS_PER_LINE == 0:
                # no traceback and no exc_info: this line exists to say the failure is
                # still going, and the one that named it is already in the log above
                logger.error(
                    "%s has now failed %d times in a row; the first failure above is "
                    "still the one to read", what, failures)
        else:
            # cleared on success rather than decayed: the next failure after a recovery is a new episode, and
            # reporting it in full is why this is kept per task
            self._failures_in_a_row.pop(what, None)

    def _abandon(self, conn: Connection) -> None:
        # the one place a close that itself fails is handled, reached from the boundary and from the sweep:
        # every statement in _close can raise, and a raise in the recovery reaches the top of the thread
        try:
            self._close(conn)
        except Exception:
            logger.exception("closing %s failed; abandoning it", conn.addr)
            # the connection cannot be tracked any more, so drop it and take the descriptor back
            self._connections.discard(conn)
            # _close clears this on its happy path, so reaching here means it raised first and the slot still
            # points at this Server
            conn.server = None
            try:
                # _close unregisters before the socket close, so reaching here leaves the map
                # keyed by a descriptor the kernel is about to reuse
                self._loop.unregister(conn)
            except Exception:
                # broad, unlike _on_accept's named three: _close has already failed and
                # anything escaping reaches the top of the thread
                logger.exception("deregistering %s failed", conn.addr)
            try:
                conn.close()
            except OSError:
                # close() reaches nothing but the socket, so this is the only family it can raise
                logger.exception("releasing %s's descriptor failed", conn.addr)

    def _read_and_dispatch(self, conn: Connection) -> None:
        if not conn.receive():
            if self._draining and conn.write_buffer:
                # end of input is not a dead peer: a half-closed client is still reading,
                # and closing it discards the replies the drain exists to deliver
                self._loop.set_read_interest(conn, False)
                return
            # a connection owing nothing is closed as everywhere: nothing queued for the close to discard, and
            # nothing unread to turn it into a reset
            self._close(conn)
            return
        if self._draining:
            # read and thrown away: dispatching would queue a reply the snapshot missed,
            # and leaving it unread makes the close a reset. counted before the clear
            self._discarded_request_bytes += len(conn.read_buffer)
            conn.read_buffer.clear()
            return
        try:
            parsed_commands = conn.take_commands(
                max_value_size=self.max_value_size, max_multibulk=self.max_multibulk
            )
        except BatchProtocolError as exc:
            # everything already parsed off this batch is answered first, then the error, then the close:
            # exc.commands carries that list, which the raise would otherwise discard with its frame
            self._dispatch_batch(conn, exc.commands)
            if conn.closed:
                # dispatching can trip --write-buffer-limit and close the connection, and an
                # error queued after that is a reply nobody reads
                return
            # exc.message is already the RESP body, and delivery is best-effort: the
            # connection closes next, so a short send drops a tail already lost
            conn.queue(resp.encode_error(exc.message))
            self._flush(conn)
            self._close(conn)
            return
        # armed on the transition into holding a command, cleared on the way out, and
        # before the dispatch -- which can pause this connection and clear it again
        if conn.has_incomplete_command:
            if conn.incomplete_since is None or parsed_commands:
                conn.incomplete_since = time.monotonic()
        else:
            conn.incomplete_since = None
        self._dispatch_batch(conn, parsed_commands)

    def _dispatch_batch(self, conn: Connection, parsed_commands: list[list[bytes]]) -> None:
        # every command take_commands() returns is dispatched: readiness re-reports
        # unread socket bytes, not parsed commands, so a leftover is never revisited
        for argv in parsed_commands:
            # at the top of the loop, so a rate-limit refusal counts: this is commands answered,
            # which is what INFO publishes it as
            self._total_commands_processed += 1
            # guarded because the arguments are built before the call and this is the hot loop; the name
            # is cut and arguments counted. above the guard, never between it and the call: a test cuts both
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("connection %d: %r with %d argument%s", conn.id, argv[0][:32], len(argv) - 1, "" if len(argv) == 2 else "s")
            # inside the per-command loop, like --write-buffer-limit: one recv carries a batch, so a
            # per-event check would charge thousands of pipelined commands one unit
            if not self._rate_limit_permits(conn):
                # queued and fallen THROUGH to the size check, never continued past it: a refused flood is
                # bytes too, and with a continue the peak went 283,953 past a 100-byte limit
                conn.queue(resp.encode_error(_RATE_LIMITED))
            else:
                response, effects = commands.dispatch(self._store, conn, argv)
                # drained once per command and before the reply, so an INCR that lazily expired its key is
                # preceded by the DEL. both lists are discarded -- nothing consumes an effect
                self._store.take_effects()
                conn.queue(response)
            # here as well as after the batch, or it bounds only what survives the flush and not the peak:
            # one recv of pipelined GETs would queue the whole batch before anything throttled
            queued = len(conn.write_buffer)
            if ((self.write_buffer_limit and queued > self.write_buffer_limit)
                    or (self.write_buffer_high_water and queued > self.write_buffer_high_water)):
                # drained before it is judged: a reader can outrun any limit on a long enough pipeline, and
                # both comparisons only trigger the flush -- _flush's own > decides
                self._flush(conn)
                if conn.closed:
                    return
        # one flush for the batch, not one per command: N replies concatenate and N syscalls buy nothing
        self._flush(conn)

    def _flush(self, conn: Connection) -> None:
        if conn.closed:
            return
        if not conn.flush():
            self._close(conn)
            return
        # checked after the send, so it measures what the kernel would not take. it closes rather than
        # throttles: refusing to read a client that has stopped reading cannot slow it down
        if self.write_buffer_limit and len(conn.write_buffer) > self.write_buffer_limit:
            logger.warning(
                "closing %s: %d bytes of queued replies exceeds the %d byte limit",
                conn.addr, len(conn.write_buffer), self.write_buffer_limit,
            )
            self._close(conn)
            return
        # read interest is decided before write: a paused connection whose buffer just emptied wants
        # neither, which is a mask the selector cannot hold. left alone while the drain runs
        if not self._draining:
            queued = len(conn.write_buffer)
            if self.write_buffer_high_water and queued > self.write_buffer_high_water:
                # a paused connection cannot finish its command, so the deadline stops with the reading, and
                # set_read_interest's return is what makes this an edge rather than a level
                if self._loop.set_read_interest(conn, False):
                    conn.incomplete_since = None
            elif queued <= self.write_buffer_low_water:
                # at or below the mark, not only at it: one send can carry the queue past. a resume re-arms
                # the deadline only if the registration changed and a command is outstanding
                if self._loop.set_read_interest(conn, True) and conn.has_incomplete_command:
                    conn.incomplete_since = time.monotonic()
        # write interest tracks the buffer's emptiness exactly: left writable, a connection spins the
        # loop at full CPU without dropping a reply
        self._loop.set_write_interest(conn, bool(conn.write_buffer))

    def _close(self, conn: Connection) -> None:
        # idempotent, because the protocol-error path closes twice: a second unregister would raise
        # inside _guard's own recovery, which is the one escape that takes the process with it
        if conn.closed:
            return
        # taken before anything touches the socket and only while the drain accounts: a close over a
        # non-empty queue is a reset. read off, not asked of the kernel, whose answer is not portable
        if self._counting_unread_at_close:
            self._discarded_request_bytes += conn.discard_unread_from_kernel()
        # a queued reply is owed and close() discards it, so take what the kernel will still accept
        # not wrapped: flush() catches BlockingIOError and OSError and returns False instead
        conn.flush()
        # unregister before closing: fileno() is -1 afterwards, and the selector then has to scan its
        # whole map for a matching object
        self._loop.unregister(conn)
        # discarded before the close: close() sets the flag first, so a raise from the socket would
        # leave this connection in the set with every later _close returning at the guard
        self._connections.discard(conn)
        # cleared before close() for the same reason: a raise would leave it pointing at a Server it
        # is no longer part of
        conn.server = None
        conn.close()
        logger.info("closed connection %d from %s; %d connected", conn.id, conn.addr, len(self._connections))

    def _tick(self) -> None:
        # each deadline is re-taken from the clock, not advanced: a save can outlast several sweep
        # intervals, and the save re-arms after it returns so the interval is the gap CONFIG GET publishes
        now = time.monotonic()
        if self._next_sweep_at is not None and now >= self._next_sweep_at:
            self._next_sweep_at = now + self._sweep_interval_seconds
            self._guard_task("expiry sweep", self._sweep_expired)
        if self._next_snapshot_at is not None and now >= self._next_snapshot_at:
            self._guard_task("snapshot save", self._save_snapshot)
            self._next_snapshot_at = time.monotonic() + self.snapshot_interval
        if self.incomplete_command_timeout:
            self._guard_task("incomplete command sweep", self._close_stalled_commands)

    def _close_stalled_commands(self) -> None:
        now = time.monotonic()
        # a copy, because closing discards from the set: iterating it raises RuntimeError on the first close
        for conn in list(self._connections):
            armed_at = conn.incomplete_since
            if armed_at is not None and now - armed_at > self.incomplete_command_timeout:
                logger.warning(
                    "closing %s: a command has been incomplete for %d seconds, past the %d second limit",
                    conn.addr, now - armed_at, self.incomplete_command_timeout)
                # _abandon and not _close: one close that raises would end the sweep with every connection
                # behind it still holding its half-sent command
                self._abandon(conn)

    def _sweep_expired(self) -> None:
        # bounded by elapsed time and no iteration cap: an iteration cap bounds dict lookups, where only
        # a time budget bounds the stall this puts in the p99
        deadline = time.monotonic() + SWEEP_BUDGET_SECONDS
        try:
            while time.monotonic() < deadline:
                sampled, expired = self._store.sample_and_expire(SWEEP_SAMPLE_SIZE)
                # sampled == 0 is tested before the division beside it: an empty index divides zero by zero
                if sampled == 0 or expired / sampled <= SWEEP_RELOOP_THRESHOLD:
                    break
        finally:
            # in a finally, not the loop's last line: a pass that raises would leave an earlier pass's DELs on
            # the queue for the next command to be blamed for
            self._store.take_effects()

    def _save_snapshot(self) -> None:
        # runs whether or not the keyspace changed -- no dirty counter, so a save is a flat interval cost
        # the path guard lives at each call site: the stop's save runs with the interval at 0
        persistence.save(self._store, self.snapshot_path)

    def _arm_periodic_tasks(self) -> None:
        # its own method so a test can exercise one arming guard without driving run()'s loop, and the
        # reading is taken once here: both first deadlines are relative to when the server starts
        now = time.monotonic()
        if self.expiry_sweep_interval:
            self._next_sweep_at = now + self._sweep_interval_seconds
        # snapshot_interval alone is not enough: its default is 60 while the path defaults to None, so
        # only this second check keeps that pairing from arming a save with nowhere to write
        if self.snapshot_interval and self.snapshot_path is not None:
            self._next_snapshot_at = now + self.snapshot_interval

    def _count_unaccepted_backlog(self, listener: socket.socket) -> int:
        # the listener leaves the select set but stays open, so the kernel keeps completing handshakes
        # through the save and the drain: accepted only to be asked what each holds and closed
        total = 0
        while True:
            try:
                sock, _addr = listener.accept()
            except BlockingIOError:
                # nothing further pending, and the listener is non-blocking, so this is the ordinary way out
                # InterruptedError cannot surface: since PEP 475 accept() retries inside the call
                break
            except OSError as exc:
                # a listener this cannot accept from is not worth a traceback in the one line a stop writes,
                # and EMFILE is the likeliest reason to be here -- rendering one fails the same way
                logger.error(
                    "could not sweep the accept backlog: %r; the request bytes discarded figure "
                    "does not count what it still held", exc)
                break
            try:
                # the ioctl is the answer here and not a ceiling: nothing has ever read this socket, so every
                # buffer on its queue is whole and the two kernels cannot disagree
                total += unread_in_kernel(sock)
            finally:
                try:
                    sock.close()
                except OSError:
                    logger.exception("could not close a swept backlog socket")
        return total

    def _rate_limit_permits(self, conn: Connection) -> bool:
        """True when this command may be dispatched; spends one permit when it is."""
        # off by default and off by this line: with no limit there is no window, no clock reading and no
        # deque, so a default server pays one attribute test per command
        if not self.rate_limit:
            return True
        # a three-value test and not `is not Role.FOLLOWER`, because the roles point opposite ways: a
        # two-value check would rate-limit this process's own link to its leader, silently
        if conn.role is not Role.CLIENT:
            return True
        window = conn.rate_limit_state
        if window is None:
            # built on first use rather than at accept, so a connection that never sends a
            # command costs nothing, and so the slot stays None for the roles above
            window = conn.rate_limit_state = ratelimit.SlidingWindow(
                self.rate_limit, self.rate_limit_window)
        return window.allow(time.monotonic())

    def _request_stop(self, signum, frame) -> None:
        self._running = False

    def _drain_for(self, timeout: int, listener: socket.socket | None = None) -> None:
        deadline = time.monotonic() + timeout
        # what bounds the remaining work is that nothing is dispatched, not that nothing is read: a socket
        # closed with unread bytes sends a reset, which discards its own send queue
        owing = []
        # zeroed here and not in __init__ alone, so the figure below belongs to this stop
        self._discarded_request_bytes = 0
        # from here until the finally clears it, every close adds its unread receive queue to that figure
        self._counting_unread_at_close = True
        try:
            # swept here as well as in the finally, and not for the figure: this one resets a client already
            # waiting when the stop landed, where the later sweep leaves it until the drain is over
            if listener is not None:
                self._discarded_request_bytes += self._count_unaccepted_backlog(listener)
            # a copy, because _abandon discards from the set being walked
            for conn in list(self._connections):
                # counted and cleared for every connection, owing or not
                # never dispatched. a parsed multibulk's own count goes too -- 1,400,019 bytes once read as 0
                self._discarded_request_bytes += (
                    len(conn.read_buffer) + conn.consumed_for_incomplete_command)
                conn.read_buffer.clear()
                # both zeroed for the same reason: from here on this connection holds only what
                # arrives after this pass, so the read path's own count cannot overlap this one
                conn.consumed_for_incomplete_command = 0
                if not conn.write_buffer:
                    # _abandon and not _close: this walk runs after the loop, where _guard no longer applies
                    self._abandon(conn)
                    continue
                owing.append(conn)
                # reading back on for every connection that owes bytes: left off, its unread requests make the
                # close a reset. incomplete_since is deliberately not re-armed -- the drain never ticks
                self._loop.set_read_interest(conn, True)
            # write interest is left alone: _flush already keeps it equal to a non-empty buffer, and every
            # queue site ends in a flush. run_once() alone, since a tick would sweep and save after the save
            while any(conn.write_buffer for conn in self._connections) and time.monotonic() < deadline:
                self._loop.run_once()
        finally:
            # cleared before the survivors are counted, so _shutdown's later closes add nothing to a figure
            # that has already been reported
            self._counting_unread_at_close = False
            # the backlog once more, because the kernel kept completing handshakes through the drain: this is
            # the last look, so what goes unaccounted is only what arrives before _shutdown's own close
            if listener is not None:
                self._discarded_request_bytes += self._count_unaccepted_backlog(listener)
            # the connections the drain did not dispose of: still open, so _close has not asked them yet, and
            # holding whatever arrived that no pass reached
            for conn in owing:
                if not conn.closed:
                    self._discarded_request_bytes += conn.discard_unread_from_kernel()
            # a close that discarded a reply leaves it in the buffer, so closed with bytes queued was lost and
            # closed with an empty one was handed over
            closed_owing = sum(1 for conn in owing if conn.closed and conn.write_buffer)
            still_owing = sum(1 for conn in owing if not conn.closed and conn.write_buffer)
            incomplete = bool(closed_owing or still_owing)
            # in a finally, so this is the only line a stop writes however it ends: WARNING when a connection
            # was owed bytes it did not get, INFO when none was. the request figure does not move it
            logger.log(
                logging.WARNING if incomplete else logging.INFO,
                "shutdown drain %s; connections closed while owed bytes: %d; connections still owed bytes: %d; request bytes discarded undispatched: %d",
                "incomplete" if incomplete else "complete", closed_owing, still_owing,
                self._discarded_request_bytes)

    def _shutdown(self, listener: socket.socket) -> None:
        try:
            for conn in list(self._connections):
                # _abandon and not _close: after the loop _guard no longer applies
                # would strand every connection after it with its descriptor open
                self._abandon(conn)
        finally:
            # the sweep is still nested, so anything _abandon does not answer for cannot cost the port
            try:
                listener.close()
            finally:
                # nested so neither close can be skipped by the other one failing
                self._loop.close()

    def run(self) -> None:
        # the selector is built in __init__ and closed on the way out, so a second run would fail inside
        # selectors naming kqueue rather than the reuse
        if self._ran:
            raise RuntimeError("this Server has already run; construct a new one")
        # raised before the handlers exist: a signal between installing them and this line would be
        # cleared by the handler and then overwritten here
        self._running = True
        # restored on the way out, since run() is also called in-process. SIGTERM always, SIGINT only when
        # it is not already SIG_IGN -- a parent that ignored it decided that, and nohup and setsid do
        restore = [(signal.SIGTERM, signal.signal(signal.SIGTERM, self._request_stop))]
        if signal.getsignal(signal.SIGINT) is not signal.SIG_IGN:
            restore.append(
                (signal.SIGINT, signal.signal(signal.SIGINT, self._request_stop)))
        # after the handlers are armed: a stop that arrived during _parse_and_run(), the snapshot load
        # included, is honoured here, so the ordinary way out runs whole rather than being discarded
        if _consume_stop_requested_during_startup():
            self._running = False
        try:
            # inside the try, so every way out passes the finally that disarms both, and below the _ran check,
            # so a second run() raises before announcing a schedule it will never honour
            self._arm_periodic_tasks()
            listener = self._open_listener()
            # right behind the listener, so a table that fills afterwards still leaves the backlog sweep one
            self._hold_spare_descriptor()
            # set once the listener is open, because what a second run must not reuse is the selector
            # bind leaves this instance usable
            self._ran = True
            self._loop.register_listener(listener)
            # the bound address, not self.port: --port 0 asks the kernel to choose. printed and not logged --
            # it is not a diagnostic, and it is the line the suite's launcher reads
            host, port = listener.getsockname()[:2]
            print(f"listening on {host}:{port}", flush=True)
            try:
                while self._running:
                    self._loop.run_once()
                    # every iteration, not only when a socket was ready: run_once() returns at least every
                    # SELECT_TIMEOUT_SECONDS, which is what makes an idle server still sweep and save
                    self._tick()
                # leaving the select set stops accept dispatch while the socket stays open until _shutdown
                # replacement server cannot bind this port yet
                self._loop.unregister_listener(listener)
                # the reserve goes back before the save, not before the first sweep: at a full table the save
                # cannot open its temporary file, and a failed save costs the keyspace where a figure
                self._release_spare_descriptor()
                # saved before the drain: a client that never reads holds the drain to its whole timeout
                # SIGKILL must not find the snapshot waiting behind it
                if self.snapshot_path is not None and not self._ignored_snapshot_is_left_in_place:
                    self._guard_task("shutdown snapshot save", self._save_snapshot)
                self._draining = True
                self._drain_for(self.shutdown_drain_timeout, listener)
            finally:
                # reached from every way out, a drain that raised included
                self._shutdown(listener)
        finally:
            # nothing is scheduled once run() is on its way out, and armed_snapshot_interval reads the save
            # deadline: left set, CONFIG GET save would answer a schedule nothing will honour
            self._next_sweep_at = None
            self._next_snapshot_at = None
            # unwound rather than looped: a signal during one restore would leave every handler after it bound
            # to a discarded Server. ExitStack runs them all and still re-raises the first
            with contextlib.ExitStack() as stack:
                # a way out that never reached the sweep still has the reserve to give back
                # so neither it nor a handler's restore can skip the other
                stack.callback(self._release_spare_descriptor)
                for signum, handler in restore:
                    stack.callback(signal.signal, signum, handler)


def main(argv=None) -> None:
    # the startup signal handler and nothing else: everything that can take time or fail is in
    # _parse_and_run, so a SIGTERM after main() is recorded rather than lost. SIGTERM alone
    _consume_stop_requested_during_startup()
    with contextlib.ExitStack() as startup_signals:
        # registered before the handler is, so that it runs after the handler has been put back
        startup_signals.callback(_consume_stop_requested_during_startup)
        previous = signal.signal(signal.SIGTERM, _note_stop_requested_during_startup)
        startup_signals.callback(signal.signal, signal.SIGTERM, previous)
        _parse_and_run(argv)


def _parse_and_run(argv) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        _check_water_marks(
            args.write_buffer_high_water, args.write_buffer_low_water,
            "write buffer high water", "write buffer low water")
        _check_limit_above_high_water(
            args.write_buffer_limit, args.write_buffer_high_water,
            "write buffer limit", "write buffer high water")
    except ValueError as exc:
        # caught around this call alone: a contradictory pair of flags is a usage error and exits 2,
        # where a corrupt snapshot is not and exits 1
        parser.error(str(exc))
    # the one place logging is configured: importing this module must leave an embedding program's
    # own alone, and Server reads no level
    logging.basicConfig(level=args.log_level)
    try:
        server = Server(
            args.port, args.write_buffer_limit, args.max_value_size, args.max_multibulk,
            snapshot_path=args.snapshot_path,
            snapshot_interval=args.snapshot_interval,
            expiry_sweep_interval=args.expiry_sweep_interval,
            ignore_snapshot=args.ignore_snapshot,
            shutdown_drain_timeout=args.shutdown_drain_timeout,
            max_connections=args.max_connections,
            write_buffer_high_water=args.write_buffer_high_water,
            write_buffer_low_water=args.write_buffer_low_water,
            incomplete_command_timeout=args.incomplete_command_timeout,
            rate_limit=args.rate_limit,
            rate_limit_window=args.rate_limit_window,
            host=args.host,
        )
    except persistence.SnapshotError as exc:
        # this exception and not a bare Exception: anything else raised while constructing a Server is
        # left to crash with its own traceback. exit 1, since a corrupt snapshot is not a usage error
        print(f"error: {exc}", file=sys.stderr)
        # the remedy named here and not in the message, because persistence.py knows nothing about flags
        # only for a refused load, and asked by type: a path named 'cannot write' lost it to a substring
        if not isinstance(exc, persistence.UnwritablePath):
            print(
                "       --ignore-snapshot starts with an empty keyspace without reading that "
                "file; with --snapshot-interval 0 beside it the file is left untouched, which "
                "is what to use if you mean to keep it",
                file=sys.stderr)
        raise SystemExit(1)
    try:
        server.run()
    except ListenFailed as exc:
        # its own exception type and not OSError: run() is the whole loop, and any OSError inside it would
        # be folded into a one-line CLI message. only the bind raises this
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
