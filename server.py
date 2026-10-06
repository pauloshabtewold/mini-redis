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
from connection import BatchProtocolError, Connection, Role, unread_in_kernel
from event_loop import EventLoop
from store import Store

# set by the handler main() installs before it builds a Server, and consumed by run() once
# run() has armed its own handlers. it exists because the handlers run() installs are the first
# thing in this process that gives SIGTERM a disposition, and everything before them -- the
# argument parse, the Server construction, and the whole snapshot load inside it -- runs with
# SIGTERM at its default. In a container the server is PID 1, and the kernel does not deliver a
# default-disposition signal to PID 1 at all: `docker stop` was discarded outright for as long as
# the load took, the container served on for the rest of the stop timeout and then died on
# SIGKILL with no save, and a write acknowledged in that window was gone after a restart. The
# window scaled with the snapshot, because the load is inside it
_stop_requested_during_startup = False


def _note_stop_requested_during_startup(signum, frame) -> None:
    global _stop_requested_during_startup
    _stop_requested_during_startup = True


def _consume_stop_requested_during_startup() -> bool:
    # read once and cleared, so that an in-process run() cannot inherit a request made for an
    # earlier one. run() calls this after installing its own handlers, which is what closes the
    # gap: a signal arriving before that point set the flag and is honoured here, and one
    # arriving after it reaches _request_stop like any other
    global _stop_requested_during_startup
    requested = _stop_requested_during_startup
    _stop_requested_during_startup = False
    return requested


DEFAULT_PORT = 6379
# loopback, so that starting a server exposes it to nothing by accident: it has no authentication of any kind, and whatever can reach the port can read, overwrite or flush every key. --host is the explicit way to listen anywhere else
LISTEN_HOST = "127.0.0.1"
# bounds how long a stop signal waits to be noticed, and on an idle loop floors both --expiry-sweep-interval and --snapshot-interval: either deadline is checked only when run_once() returns -- see Server._tick -- so with no traffic a deadline can be noticed up to one timeout late, and the default sweep interval is equal to it. under traffic run_once() returns as soon as a socket is ready, so a shorter interval is honoured.
SELECT_TIMEOUT_SECONDS = 0.1
# the most one connection may have queued once the kernel has taken what it will, past which it is closed. with the water marks below in force this is not what slows a client down: one that stops reading is paused rather than throttled here, so what this bounds is a reply larger than it, or the replies to one read's batch of pipelined requests, which the pause cannot unqueue once they are parsed. what the pause does not do is hold such a client near the mark -- the batch that crossed the mark is still dispatched whole, and measured a paused connection holds almost the whole of this default, so the pause keeps it from being closed here rather than keeping it far away; README.md carries the figure. 0 turns the check off. see Server._flush for why exceeding it closes the connection instead of slowing it down
# a local choice and not the reference's, which leaves an ordinary client unlimited. it is below DEFAULT_MAX_VALUE_SIZE, so a value that was stored can be too large to read back: the reply to a GET of it exceeds this and the connection is closed. that is what a hard limit does, and neither default is moved to hide it
DEFAULT_WRITE_BUFFER_LIMIT = 32 * 1024 * 1024
# a connection with more than this queued for it stops being read, and is read again once the queue falls to the low-water mark below. the gap between the two is the reason there are two: equal marks would pause and resume the connection on every event. 0 disables the pause. the pause is what keeps a client that pipelines faster than it reads from being closed by the limit above, and not what keeps it away from it: a paused connection holds this mark plus the one batch that crossed it, which the limit in turn caps at the limit plus one reply, so at the two shipped defaults it holds almost the whole of that ceiling rather than anything near this mark -- README.md carries the measured figure and the aggregate it implies
DEFAULT_WRITE_BUFFER_HIGH_WATER = 1024 * 1024
DEFAULT_WRITE_BUFFER_LOW_WATER = 256 * 1024
# how long a connection may sit on a command it has only partly sent. the size caps bound one element, one command's element count and one connection's queued replies, and none bounds how long a connection may hold part of one, so a hundred connections can each sit on almost all of a large element for as long as they like; this closes a connection that has held one for longer than this. it bounds the time and not the sum: inside the limit those read buffers still add up. 0 turns the check off
DEFAULT_INCOMPLETE_COMMAND_TIMEOUT_SECONDS = 30
# applied by main() and nowhere else: a library module that configures logging takes the decision away from whatever imports it
DEFAULT_LOG_LEVEL = "INFO"
# a local choice and not the reference's: proto-max-bulk-len defaults to 512 MiB there,
# eight times this. 64 MiB is well above the largest value the suite round-trips and
# below anything that risks OOMing a development machine. --max-value-size bounds every
# inbound bulk element, including the command name and any key, not only what a human
# would call "the value"
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
# a local choice: long enough for a reader that is only slow to take what is already queued for it, and short enough that an operator's own kill timer -- docker stop waits ten seconds -- does not land inside the drain and skip the teardown behind it
DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 5
# a local choice, and it must not fall under 50: redis-benchmark -c 50 is the standard invocation, and a cap under it refuses some of the benchmark's own clients and turns it into a test of the cap. a cap of exactly 50 admits exactly 50, since a count equal to the cap is full
DEFAULT_MAX_CONNECTIONS = 1024
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
# how many repeats of one periodic task's failure pass in silence before a line says how
# many there were. see _guard_task: a task that fails on every tick -- a save over a path
# whose snapshot cannot be replaced, a full disk -- writes one traceback and then a short
# line per this many, instead of a traceback per interval for as long as the server runs
FAILURE_REPEATS_PER_LINE = 100
# the cap's counterpart to FAILURE_REPEATS_PER_LINE, ten times larger because the two rates differ: a periodic task fails at most once an interval, where a client retrying in a loop is refused as fast as it can connect, and a hundred of those would still write a line every few milliseconds. see Server._log_refusal
REFUSALS_PER_LINE = 1000
# how long refusals must stop for before the next one is reported in full again. a close cannot end the episode, however natural that looks: a freed slot is what a retrier is waiting for, so with slots turning over one comes between every pair of refusals
REFUSAL_EPISODE_GAP_SECONDS = 60
# the one site --expiry-sweep-interval's milliseconds are turned into the seconds
# time.monotonic() deals in
_MILLISECONDS_PER_SECOND = 1000
# the ceiling _check_schedulable refuses past. the arithmetic that schedules an
# interval -- now + value for the snapshot arm, value / 1000 for the sweep -- raises
# OverflowError from float's own limit (around 10**308), and the ceiling is there so that
# a value that large is refused cleanly at startup instead. 2**63 - 1 is this project's own
# convention for the largest integer any value here is ever let hold (see
# commands/registry.py's INT64_MAX), reused as a ceiling that is still far below where
# the float conversion actually breaks
MAX_SCHEDULABLE_INTERVAL = 2**63 - 1

# importing this module configures nothing: with nothing configured, logging.lastResort writes records at WARNING and above to stderr as bare messages, and main() is the one place that configures logging
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


# the ending of every negative-value refusal but three: for the flags this rule was written for, 0 is the value that turns the check off, and it is the one the operator is steered to
_ZERO_DISABLES = "0 disables the check, not %d"
# --shutdown-drain-timeout's own ending, because its 0 is the restrictive value: no drain at all, so whatever the kernel will not take in one pass is discarded. -1 is the likeliest spelling of "unlimited" and is exactly what is refused here, so the shared ending would send the operator from the value they typed to the one that does the opposite of what they meant
_ZERO_SKIPS_THE_DRAIN = "%d does not mean unlimited, and 0 means no drain at all"
# --write-buffer-low-water's own ending, because its 0 disables nothing: with the pause on it means resume only once the queue is empty, the most conservative resume, so the shared ending would send an operator who typed -1 to a value that does not switch anything off
_ZERO_RESUMES_WHEN_EMPTY = "%d does not turn anything off, and 0 means resume only when the queue is empty"
# --snapshot-interval's own ending, because its 0 turns off the periodic save and no more: a clean stop still saves, unless --ignore-snapshot is set as well, so the shared ending would send an operator who typed -1 to a value that does not stop saving
_ZERO_STOPS_THE_PERIODIC_SAVE = "%d does not turn saving off, and 0 stops only the periodic save"


def _check_not_negative(value: int, label: str, ending: str = _ZERO_DISABLES) -> None:
    # the "0 disables, a negative number is refused" rule, stated once and used by every
    # CLI validator below and by Server.__init__. `limit and len(buf) >
    # limit` reads any non-zero value as "enabled", and every buffer length is greater
    # than a negative number -- including zero -- so a negative limit reaching that
    # check would close every connection after its first reply while the server still
    # logs a healthy startup line. -1 is a conventional spelling of "unlimited"
    # elsewhere, which makes it the likeliest value to be typed here by someone
    # reaching for the opposite of what it does
    if value < 0:
        raise ValueError(("%s cannot be negative; " + ending) % (label, value))


def _check_schedulable(value: int, label: str, unit: str) -> None:
    # shared by the CLI validators for --snapshot-interval, --expiry-sweep-interval,
    # --shutdown-drain-timeout and --incomplete-command-timeout and by Server.__init__
    # for the same four, the only numeric settings that are durations. the first three are
    # added to a clock reading or divided by a constant -- the periodic tick for the first
    # two, the drain's deadline for the third -- and a value far enough past this ceiling
    # reaches that arithmetic as an OverflowError instead of a clean refusal; for the drain
    # timeout that is a traceback on SIGTERM, after the save. that is the same failure mode
    # _check_not_negative exists to prevent at the other end of the range, and the reason
    # this check runs beside it rather than replacing it. the fourth never meets arithmetic
    # that can overflow: the stalled-command sweep only compares an elapsed time against it,
    # and comparing an int with a float cannot overflow however large the int is. it takes
    # the same ceiling so that one rule covers every duration, and would not fail without it
    if value > MAX_SCHEDULABLE_INTERVAL:
        raise ValueError(
            "%s cannot exceed %d %s; a larger value cannot be scheduled, not %d"
            % (label, MAX_SCHEDULABLE_INTERVAL, unit, value))


def _check_water_marks(high: int, low: int, high_label: str, low_label: str) -> None:
    # the one rule that spans two settings, stated once for the two places that can see both: main(), which turns the refusal into a usage error because argparse has no hook for a pair, and Server.__init__, which is built directly by tests and by anything embedding it. the labels are the caller's, so each door names the settings the way its own messages do
    # guarded by the high-water mark, because 0 for it disables the pause, and a pause that is off has no band for a low-water mark to be inconsistent with: without the guard --write-buffer-high-water 0 alone would be refused against the default low-water mark, and so would the one way to switch the pause off
    # >= and not >: marks that are equal are the band of zero width the two settings exist to avoid
    if high and low >= high:
        raise ValueError(
            "%s (%d) must be below %s (%d); equal marks would pause and resume a "
            "connection on every event, and 0 for %s switches the pause off instead"
            % (low_label, low, high_label, high, high_label))


def _check_host(value: str, label: str) -> None:
    # stated once and used by the --host validator and by Server.__init__. an empty host is refused rather than read as a shorthand, because the one thing bind() does with it is listen on every interface, which is the exposure LISTEN_HOST is loopback to avoid, on a server with no authentication at all. it is not a spelling anyone means: it is what `--host "$HOST"` becomes when HOST is unset. 0.0.0.0 is not refused, since it names the same thing and whoever types it has chosen it
    if not value:
        raise ValueError(
            "%s cannot be empty; an empty host listens on every interface, and 0.0.0.0 "
            "asks for that by name" % label)


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
        # replacement no save can make. a save can happen unless the snapshot was ignored
        # and periodic saving is off, which is wider than the non-zero interval this runs
        # under: with --snapshot-interval 0 and no --ignore-snapshot the stop still saves
        # there and nothing checks the path first, so one a save could not write is first
        # found at the stop, where the failure is logged and the exit status stays 0. a
        # directory at the path is still refused at interval 0, by the load, unless
        # --ignore-snapshot skips the load as well
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
    # a missing file is a first run, not a failure, and is treated the same as
    # snapshot_path=None above. anything else persistence.load raises -- a
    # corrupt file -- is left uncaught here, so it propagates out of __init__ and
    # refuses construction rather than starting empty over a snapshot that failed
    # to load and then overwriting it at the next save
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


def _shutdown_drain_timeout(value: str) -> int:
    number = _numeric_limit(value, "shutdown drain timeout", "seconds", _ZERO_SKIPS_THE_DRAIN)
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
    # separate from main() so the parser can be inspected without running the server.
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
             "has taken what it will; 0 means no limit, and nothing is closed for it. "
             "with --write-buffer-high-water in force this bounds a reply larger than "
             "BYTES, or the replies to one read's batch of pipelined requests, rather "
             "than a client that reads slowly, and a value stored under "
             "--max-value-size can be too large to read back under it",
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
             "--snapshot-interval and on a clean stop; refuses to start if what is at "
             "PATH cannot be loaded, unless --ignore-snapshot is given, and, with a "
             "non-zero --snapshot-interval, if a save could not write PATH as things "
             "stand at startup -- a missing or read-only directory, or a directory "
             "standing at PATH",
    )
    parser.add_argument(
        "--snapshot-interval",
        type=_snapshot_interval,
        default=DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
        metavar="SECONDS",
        help="save a snapshot to --snapshot-path every SECONDS, and on a clean stop; 0 "
             "disables the periodic save only -- a clean stop still saves, except with "
             "--ignore-snapshot, where 0 leaves the file at --snapshot-path alone",
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
        help="on SIGTERM or SIGINT, save a snapshot and then keep sending the replies "
             "already queued for clients, for no longer than SECONDS plus one "
             f"{SELECT_TIMEOUT_SECONDS} second select() timeout, before exiting whether "
             "or not the kernel took all of them. 0 is not unlimited here, it is the "
             "opposite: no drain at all, so whatever the kernel will not take in one "
             "pass is discarded, and a request still unread at the close can take the "
             "kernel's unsent tail with it",
    )
    parser.add_argument(
        "--max-connections",
        type=_max_connections,
        default=DEFAULT_MAX_CONNECTIONS,
        metavar="COUNT",
        help="close, without a reply, a connection that arrives while COUNT clients are "
             "already connected; 0 means no limit, and nothing is refused",
    )
    parser.add_argument(
        "--write-buffer-high-water",
        type=_write_buffer_high_water,
        default=DEFAULT_WRITE_BUFFER_HIGH_WATER,
        metavar="BYTES",
        help="stop reading from a connection while more than BYTES of replies are queued "
             "for it, and resume once the queue falls to --write-buffer-low-water; 0 "
             "disables the pause, so reading goes on however much is queued, and "
             "--write-buffer-low-water is then unused",
    )
    parser.add_argument(
        "--write-buffer-low-water",
        type=_write_buffer_low_water,
        default=DEFAULT_WRITE_BUFFER_LOW_WATER,
        metavar="BYTES",
        help="resume reading from a paused connection once its queued replies fall to "
             "BYTES or fewer; must be below --write-buffer-high-water unless that is 0. "
             "0 means resume only when the queue is empty, which is the most "
             "conservative resume and not a way to switch anything off",
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
             "pause is not carried across it. 0 means no limit, and nothing is closed "
             "for it",
    )
    parser.add_argument(
        "--host",
        type=_host,
        default=LISTEN_HOST,
        metavar="HOST",
        help="the IPv4 address to listen on, or a name that resolves to one. the default "
             "is loopback, so a server is local-only unless asked otherwise: it has no "
             "authentication, and whatever can reach the port can read and change every "
             "key. an empty value is refused, since it would listen on every interface; "
             "0.0.0.0 asks for that by name",
    )
    parser.add_argument(
        "--log-level",
        type=str.upper,
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default=DEFAULT_LOG_LEVEL,
        help="how much to log, in any case. INFO reports each connection opened and "
             "closed; DEBUG adds a line per dispatched command naming the command and "
             "its argument count, never a key or a value. the shutdown drain writes one "
             "line per stop, INFO when it lost nothing and WARNING when it lost "
             "something, so WARNING hides the confirmation of a clean stop along with "
             "the per-connection lines and shows that line only when something was "
             "lost; ERROR hides it either way",
    )
    return parser


class Server:
    def __init__(
        self,
        port: int,
        write_buffer_limit: int = DEFAULT_WRITE_BUFFER_LIMIT,
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
        shutdown_drain_timeout: int = DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        # appended and not placed beside the settings they resemble: main() binds the first
        # four positionally, so a parameter inserted ahead of any of them would rebind every
        # positional call site without an error
        write_buffer_high_water: int = DEFAULT_WRITE_BUFFER_HIGH_WATER,
        write_buffer_low_water: int = DEFAULT_WRITE_BUFFER_LOW_WATER,
        incomplete_command_timeout: int = DEFAULT_INCOMPLETE_COMMAND_TIMEOUT_SECONDS,
        host: str = LISTEN_HOST,
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
        _check_not_negative(write_buffer_limit, "write_buffer_limit")
        # a per-connection ceiling on queued replies, not a process-wide one: what this
        # bounds is one client's ability to make the server hold bytes it has not managed
        # to send, and connections do not share a write buffer to divide between them
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
        _check_not_negative(shutdown_drain_timeout, "shutdown_drain_timeout", _ZERO_SKIPS_THE_DRAIN)
        _check_schedulable(shutdown_drain_timeout, "shutdown_drain_timeout", "seconds")
        self.shutdown_drain_timeout = shutdown_drain_timeout
        _check_not_negative(max_connections, "max_connections")
        self.max_connections = max_connections
        _check_not_negative(write_buffer_high_water, "write_buffer_high_water")
        _check_not_negative(write_buffer_low_water, "write_buffer_low_water", _ZERO_RESUMES_WHEN_EMPTY)
        _check_water_marks(
            write_buffer_high_water, write_buffer_low_water,
            "write_buffer_high_water", "write_buffer_low_water")
        self.write_buffer_high_water = write_buffer_high_water
        self.write_buffer_low_water = write_buffer_low_water
        _check_not_negative(incomplete_command_timeout, "incomplete_command_timeout")
        _check_schedulable(incomplete_command_timeout, "incomplete_command_timeout", "seconds")
        self.incomplete_command_timeout = incomplete_command_timeout
        _check_host(host, "host")
        self.host = host
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
        # per task name: how many failures in a row, counting the one already reported.
        # _guard_task keeps it; a task that succeeds drops its entry, so the next failure
        # after a recovery is a first failure again and is reported in full
        self._failures_in_a_row: dict[str, int] = {}
        # set when the shutdown drain begins and never cleared: it is what turns a read into a discard instead of a dispatch, and what lets end of input on a connection that still owes bytes wait for the drain instead of closing it, which is the reset _drain_for reads to avoid
        self._draining = False
        # refusals in the current episode, counting the one already reported, and when the last one came. _log_refusal keeps both: a refusal that follows the one before it by more than REFUSAL_EPISODE_GAP_SECONDS starts the count again
        self._cap_refusals = 0
        self._last_refusal_at: float | None = None
        # inbound bytes a stop threw away without dispatching them, in all five shapes they come in: what was sitting in a read buffer when the drain began, what a half-received command had already had parsed off that buffer and holds with the buffer empty, everything the drain read and discarded, whatever was still unread in a connection's kernel receive queue when that connection was closed or when the drain ran out of time, and whatever sat on a connection the kernel completed into the accept backlog after the stop, which nothing ever accepts. _drain_for zeroes it on the way in and reports it on its one line. it exists because the pause makes this quantity large and nothing else reports it: a connection the high-water mark stopped reading has its requests waiting in the kernel's receive queue for as long as it stays paused, which is until it disconnects -- measured, all 50,000 of 50,000 pipelined SETs whose send() had already returned, over thirteen runs in which the pause fired; README.md carries the figure and why the executed count moves. the two reply counts beside it on that line cannot show this, because a discarded request is not a reply anybody is owed
        # the third shape is why the count is not just the drain's own reads. the drain ends the moment no connection owes a reply, which a client that reads its replies reaches long before the drain has emptied its receive queue, and --shutdown-drain-timeout 0 ends it before a single read: measured, a figure built from the drain's own reads reports every byte it read, which in both of those cases is 0 of a pipeline of 50,000
        self._discarded_request_bytes = 0
        # set only while _drain_for is accounting, so that _close contributes a closing connection's unread receive queue to the figure above exactly once, and the closes _shutdown makes after the line is written contribute nothing
        self._counting_unread_at_close = False

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

    @property
    def _ignored_snapshot_is_left_in_place(self) -> bool:
        # the one pair of settings that skips the save on the way out: with the snapshot
        # ignored and periodic saving off, the file was protected only because nothing
        # wrote to it, and a save on the way out is a write. any other pairing saves: an
        # interval of 0 alone included, and with a non-zero interval the next interval
        # overwrites an ignored file anyway. the guarantee is kept rather than
        # the prose corrected, on the principle the startup path already follows by
        # removing no stale temporary file beside the snapshot, which may hold the only
        # copy of a newer save: an operator reaching for the escape hatch is rescuing a
        # file, and the stop must not be what destroys it
        return self.ignore_snapshot and not self.snapshot_interval

    def _open_listener(self) -> socket.socket:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind((self.host, self.port))
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
                "cannot listen on %s:%d: %s" % (self.host, self.port, exc)) from exc
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
        # >= where every byte limit is >: a byte limit compares a quantity already held against a ceiling it may legally reach, and this asks whether there is room for one more, so a count equal to the cap is full. the count is the set's own length because a second counter would disagree with the set silently in both directions
        if self.max_connections and len(self._connections) >= self.max_connections:
            self._log_refusal(addr)
            # closed with nothing written: a send() from this callback is on a socket nothing tracks, for a client that by construction is not being kept, and a close is a refusal the client can see. before Connection() so nothing owns the socket but this frame
            sock.close()
            return
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
        logger.info("accepted connection %d from %s; %d connected", conn.id, addr, len(self._connections))

    def _log_refusal(self, addr: tuple[str, int]) -> None:
        # bounded by count, the way _guard_task bounds a failure that repeats: a full report for the first, one short line per REFUSALS_PER_LINE after it, DEBUG for the rest. what refuses here refuses as fast as a client can connect, and logging writes to stderr with a blocking write on the only thread here, so a line per refusal is a line per attempt of whatever is retrying in a loop
        #
        # cleared by a gap with no refusals and not by a connection closing, which is where _guard_task clears its own, on a success. a close is what a saturated cap's retrier is waiting for, and with slots turning over one comes between every pair of refusals, so clearing on it writes a full report per refusal: measured, 300 refusals at --max-connections 1 with the slot turning over are 300 WARNINGs
        now = time.monotonic()
        if (self._last_refusal_at is not None
                and now - self._last_refusal_at > REFUSAL_EPISODE_GAP_SECONDS):
            self._cap_refusals = 0
        self._last_refusal_at = now
        self._cap_refusals += 1
        if self._cap_refusals == 1:
            logger.warning(
                "refusing %s: %d connections is the --max-connections limit; further "
                "refusals are logged at DEBUG, with a count every %d, until none has "
                "come for %d seconds",
                addr, self.max_connections, REFUSALS_PER_LINE, REFUSAL_EPISODE_GAP_SECONDS)
        elif self._cap_refusals % REFUSALS_PER_LINE == 0:
            # no address: this line says the refusals are still going, and the first one named who
            logger.warning(
                "%d connections refused so far at the --max-connections limit of %d",
                self._cap_refusals, self.max_connections)
        else:
            logger.debug("refusing %s: at the --max-connections limit", addr)

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
            # not wrapped: measured, handleError swallows the OSError family, so a full disk or a gone pipe cannot raise here. a closed stream raises ValueError straight through it, which nothing here can produce -- the handler main() configures writes to stderr and nothing here closes stderr
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
        #
        # what is reported is bounded, because what fails here fails on a timer. a save
        # over a snapshot whose directory entry cannot be removed, or onto a full disk,
        # fails identically once per interval for as long as the server runs, and
        # logging writes to stderr with a blocking write(): a reader that stops -- a
        # stalled collector, a pipeline whose far end died -- fills its buffer and then
        # parks this call, which is on the only thread here, inside the tick. from there
        # the loop never reaches select() again and every client is served nothing, with
        # no further line to say so. the first failure is reported in full because it is
        # the diagnostic; the repeats after it carry no information the first did not,
        # and a count says more than a thousand copies of one traceback
        #
        # this bounds what this server writes. it does not make the write non-blocking,
        # and nothing here can: O_NONBLOCK lives on the open file description, which
        # belongs to whoever handed this process its stderr, and a writer thread is a
        # second thread on a server whose single one is the point
        try:
            step()
        except Exception:
            failures = self._failures_in_a_row.get(what, 0) + 1
            self._failures_in_a_row[what] = failures
            if failures == 1:
                logger.exception("%s failed", what)
            elif failures % FAILURE_REPEATS_PER_LINE == 0:
                # no traceback and no exc_info: this line exists to say the failure is
                # still going, and the one that named it is already in the log above
                logger.error(
                    "%s has now failed %d times in a row; the first failure above is "
                    "still the one to read", what, failures)
        else:
            # cleared on success rather than decayed: the next failure after a recovery
            # is a different episode, and reporting it in full is the whole point of
            # keeping this per task
            self._failures_in_a_row.pop(what, None)

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
            if self._draining and conn.write_buffer:
                # end of input is not a dead peer: a client that has shut down its sending half is finished asking and still reading, and closing it here discards every reply it is owed, which is the one thing the drain exists not to do. waiting costs nothing the drain does not already bound -- a peer that has really gone makes the next send fail, which flush() reports as a dead peer and _flush answers with a close the drain then counts, and a peer that has stopped reading is held by the drain's own deadline. reading is switched off for this connection because end of input stays readable: left registered, every pass would return at once having read nothing, for as long as the drain runs. write interest is already registered, because _flush keeps it equal to whether the buffer is non-empty, and the buffer is non-empty here, so clearing the read bit leaves a mask that is not empty. it does not stay that way: when the buffer empties _flush clears the write bit too, the mask reaches 0, and the applier unregisters the connection, which is right for a connection that owes nothing -- it stays open and in the set until _shutdown closes it
                self._loop.set_read_interest(conn, False)
                return
            # a connection that owes nothing is closed, here as everywhere: there is nothing queued for the close to discard and nothing unread to turn it into a reset, and a half-close has nothing left to wait for when nothing is being sent
            self._close(conn)
            return
        if self._draining:
            # read and thrown away: a command dispatched now would queue a reply for a write the snapshot already missed, and a request left unread makes the close that follows a reset and not a FIN. measured, a reset does not take back what the peer has already received -- a client reads every byte and meets the error only on the read after the last -- but it discards what the closing socket itself has queued and the kernel has not yet put on the wire, and that is the tail of what the drain was waiting to deliver. the buffer is emptied so it cannot grow across passes, and nothing is parsed because this connection is only waiting to be closed
            # counted before the clear, and the clear is what keeps one byte from being counted twice as the drain reads more into a buffer the setup pass already accounted for. the figure's sites are disjoint by what they measure, not by ordering: the setup pass counts a read buffer's contents and whatever a half-parsed command had already taken out of it, zeroing both; this counts what a read just added to an emptied one; _close and the drain's own survivor walk count what the kernel still holds and nothing ever read; and the backlog sweep counts sockets that never became connections at all
            self._discarded_request_bytes += len(conn.read_buffer)
            conn.read_buffer.clear()
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
                # dispatching those commands can itself trip --write-buffer-limit and
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
        # the deadline for a half-sent command is armed here, on the transition into holding one, and cleared on the transition out. a command that is still outstanding keeps the reading it was armed with: restarting it on every recv would let a client sending a byte a second hold a buffer indefinitely, and that is the client the deadline is for. a command that completed in this batch is the other case -- whatever is held now is the start of the next one and gets a deadline of its own, or a client streaming a pipeline that every recv cuts in the middle of a command would never leave a clean buffer and would be closed at the timeout however quickly each command completed
        # before the batch is dispatched and not after it: dispatching can pause this connection, and _flush suspends the deadline by clearing it, which an arm that came later would overwrite with a running one
        if conn.has_incomplete_command:
            if conn.incomplete_since is None or parsed_commands:
                conn.incomplete_since = time.monotonic()
        else:
            conn.incomplete_since = None
        self._dispatch_batch(conn, parsed_commands)

    def _dispatch_batch(self, conn: Connection, parsed_commands: list[list[bytes]]) -> None:
        # every command take_commands() returns is dispatched: level-triggered readiness re-reports unread socket bytes, not commands already taken out of the buffer, so a leftover here is never revisited and the client waits forever
        for argv in parsed_commands:
            # guarded, because the arguments are built before logger.debug is called and this is the hot loop: at the default level an unguarded call would pay for them once per dispatched command and write nothing. the name is cut and the arguments are counted, never shown -- a command name is a bulk element and may be as large as --max-value-size, and a key or a value does not belong in a log
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("connection %d: %r with %d arguments", conn.id, argv[0][:32], len(argv) - 1)
            response, effects = commands.dispatch(self._store, conn, argv)
            # drained once per command and before the reply is queued: the queue holds
            # effects the lookups inside THIS command produced, so they precede the
            # command's own -- an INCR that lazily expired its key must be preceded by the
            # DEL, or a follower ends with the key absent while this server holds the new
            # value. both lists are discarded because nothing consumes them: the only
            # reader an effect was ever for is replication, which is specified in full and
            # deliberately unbuilt, and the drain still runs because lazy expiry fills the
            # queue from here on and nothing else would ever empty it
            self._store.take_effects()
            conn.queue(response)
            # one recv can carry thousands of commands, and queueing every reply before
            # the first send is what lets a few kilobytes of request commit gigabytes.
            # the limit has to be consulted here as well as after the batch, or it bounds
            # only what survives the flush and not the peak that got there. so does the
            # high-water mark, and it is guarded by itself and not by the limit: with the
            # limit at 0 this flush would never be reached, and a pause decided once per
            # batch would let one recv of pipelined GETs queue the whole batch before
            # anything throttled. the pause cannot unqueue what the batch has already
            # parsed -- every command in it is still dispatched -- but it stops the next
            # recv, and the flush gives the kernel the chance to take what is queued
            queued = len(conn.write_buffer)
            if ((self.write_buffer_limit and queued > self.write_buffer_limit)
                    or (self.write_buffer_high_water and queued > self.write_buffer_high_water)):
                # drained before it is judged: a client reading normally can outrun any
                # limit on a long enough pipeline, and closing it for the depth of its
                # batch rather than for failing to read is not what the limit is for.
                # _flush closes it if the buffer is still over once the kernel is done --
                # which makes both comparisons here a trigger and not the decision, so
                # their exact boundaries are not observable. mutating either to >= changes
                # only how early the flush happens, and _flush's own > still decides.
                # mutation testing reports that as a surviving mutant; it is an equivalent
                # one, noted so the next reader spends no time on it
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
        # rather than what was queued a moment ago. the limit closes rather than
        # throttles, and the measurement behind that stands: refusing to read a client
        # that has stopped reading cannot slow it down, because a client that writes its
        # requests before reading any reply then blocks in send() waiting for room only
        # its own reading would create, and both sides wait forever. the pause below
        # refuses to read anyway and accepts that cost, because it holds a slow reader
        # where the limit alone would close it; docs/DESIGN.md has why. the reference
        # closes here too. a follower link would have needed exempting from this, on the
        # same reasoning that would have kept it off the rate limiter -- a follower that
        # falls behind is not a client that has stopped reading -- but replication is
        # specified in full and deliberately unbuilt, so there is no such link to exempt
        if self.write_buffer_limit and len(conn.write_buffer) > self.write_buffer_limit:
            logger.warning(
                "closing %s: %d bytes of queued replies exceeds the %d byte limit",
                conn.addr, len(conn.write_buffer), self.write_buffer_limit,
            )
            self._close(conn)
            return
        # read interest is decided before write interest, and the order matters: a paused connection whose buffer has just emptied wants neither, which the selector cannot hold as a mask and the loop spells unregistered. deciding read first means a buffer that drained to empty has already passed the resume below, so the write decision never meets a connection that wants neither
        # none of it while the shutdown drain runs. what keeps the drain from dispatching is _read_and_dispatch discarding what it reads, so read interest is not this method's to decide there. the guard is for a connection whose peer has finished sending and is still reading: end of input switched its reading off, because end of input stays readable and would wake the loop on every pass, and the resume below fires as soon as its queue is under the low-water mark -- it would switch the reading back on, and the drain would spin on end of input until its deadline
        if not self._draining:
            queued = len(conn.write_buffer)
            if self.write_buffer_high_water and queued > self.write_buffer_high_water:
                # a paused connection cannot finish its command, because this server is what stopped reading it, so the deadline for a half-sent one stops with the reading. set_read_interest reports whether the registration changed, which is what makes the pause an edge and not a level: this runs on every flush that finds the queue above the mark, and only the one that did the pausing suspends anything
                if self._loop.set_read_interest(conn, False):
                    conn.incomplete_since = None
            elif queued <= self.write_buffer_low_water:
                # at or below the low-water mark and not only at it, since one send can carry the queue past it. between the marks neither branch runs and the registration is left as it is, which is the hysteresis. a resume re-arms the deadline only if the registration actually changed and a command is actually outstanding: re-arming on every flush would restart it on each byte of a trickle, and arming for a connection holding nothing would close an idle client
                if self._loop.set_read_interest(conn, True) and conn.has_incomplete_command:
                    conn.incomplete_since = time.monotonic()
        # write interest tracks the buffer's emptiness exactly: a connection left permanently writable spins the loop at 100% CPU without dropping a single reply
        self._loop.set_write_interest(conn, bool(conn.write_buffer))

    def _close(self, conn: Connection) -> None:
        # idempotent, because the protocol-error path closes twice: _flush closes on a failed send and the caller closes again, and a second unregister raises from inside _guard's own recovery -- the one exception that escapes the boundary and takes the process with it
        if conn.closed:
            return
        # asked before anything below touches the socket, and only while the drain is accounting: what is still in this connection's receive queue is about to go, because a close over a non-empty one is a reset and nothing dispatches it either way. counted here rather than in _drain_for's walk so that one figure covers every connection the stop disposes of -- the ones the setup pass abandons for owing nothing, and the ones _flush closes mid-drain for a dead peer or the write-buffer limit -- and not only the ones still open when the drain ends, which _drain_for adds itself
        if self._counting_unread_at_close:
            self._discarded_request_bytes += conn.unread_in_kernel()
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
        logger.info("closed connection %d from %s; %d connected", conn.id, conn.addr, len(self._connections))

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
        if self.incomplete_command_timeout:
            self._guard_task("incomplete command sweep", self._close_stalled_commands)

    def _close_stalled_commands(self) -> None:
        now = time.monotonic()
        # a copy, because closing discards from the set being walked: iterating the set itself raises RuntimeError the first time anything is closed
        for conn in list(self._connections):
            armed_at = conn.incomplete_since
            if armed_at is not None and now - armed_at > self.incomplete_command_timeout:
                logger.warning(
                    "closing %s: a command has been incomplete for %d seconds, past the %d second limit",
                    conn.addr, now - armed_at, self.incomplete_command_timeout)
                # _abandon and not _close: one close that raises would otherwise end the sweep with every connection behind it in iteration order still holding its half-sent command
                self._abandon(conn)

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
        #
        # the guard lives at each call site and not here: _tick() only reaches this call
        # once _next_snapshot_at holds a deadline, which _arm_periodic_tasks() sets only for
        # a path, and run()'s shutdown save checks the path itself because it runs with
        # --snapshot-interval 0, where nothing is ever armed
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

    def _count_unaccepted_backlog(self, listener: socket.socket) -> int:
        # the listener leaves the select set when the loop stops, but the socket itself stays open
        # until _shutdown, so the kernel goes on completing handshakes into its backlog for the
        # whole of the save and the drain. Those clients are never accepted, never read and never
        # answered, and the listener's close resets them -- measured, three of them sent 201,000
        # bytes of complete SETs that the figure reported as 0. accepted here only to ask what
        # each one holds and close it, which is what the listener's close was going to do anyway,
        # so the client sees what it saw before and the figure no longer misses them
        total = 0
        while True:
            try:
                sock, _addr = listener.accept()
            except (BlockingIOError, InterruptedError):
                # nothing further pending. the listener is non-blocking, so this is the ordinary
                # way out rather than an error
                break
            except OSError:
                # a listener this cannot accept from is not worth a traceback inside the one
                # report a stop writes; whatever is left in its backlog is reset by its close
                logger.exception("could not sweep the accept backlog")
                break
            try:
                total += unread_in_kernel(sock)
            finally:
                try:
                    sock.close()
                except OSError:
                    logger.exception("could not close a swept backlog socket")
        return total

    def _request_stop(self, signum, frame) -> None:
        self._running = False

    def _drain_for(self, timeout: int, listener: socket.socket | None = None) -> None:
        deadline = time.monotonic() + timeout
        # what keeps the remaining work a fixed number of bytes is that nothing is dispatched, not that nothing is read, and a connection has to be drained of inbound bytes before it is closed: closing a socket that still holds unread ones sends a reset instead of a FIN, and a reset discards the closing socket's own send queue -- what this process handed the kernel and the kernel has not yet put on the wire -- so the last replies of a slow reader would go with it. what the peer had already received is not taken back
        # every connection that owes bytes when the drain begins, kept because a close empties the set it was found in and the report in the finally clause has to count what the drain lost as well as what it is still waiting on. the set only shrinks from here on, because the listener is no longer registered, so nothing can arrive that this list does not hold
        owing = []
        # zeroed here rather than in __init__ alone, so the figure on the line below belongs to this stop and not to the life of the process
        self._discarded_request_bytes = 0
        # from here until the finally clears it, every close adds the closing connection's unread receive queue to that figure
        self._counting_unread_at_close = True
        try:
            # before the connections, because these sockets are not in the set and nothing else
            # will ever look at them. None when _drain_for is driven directly, as the tests do,
            # where there is no listener and nothing can be in a backlog
            if listener is not None:
                self._discarded_request_bytes += self._count_unaccepted_backlog(listener)
            # a copy, because _abandon discards from the set being walked
            for conn in list(self._connections):
                # counted and cleared for every connection, owing or not, before either arm below: these are bytes that arrived before the stop and will never be dispatched, since the drain dispatches nothing. clearing is what makes the count in _read_and_dispatch additive rather than overlapping -- from here on a buffer holds only what arrived after this pass
                # the buffer's length is not the whole of what this connection holds: a
                # multibulk whose header and early elements are parsed keeps them in the
                # connection's own count with an empty buffer, and those bytes are discarded here
                # exactly as the buffered ones are -- measured, 1,400,019 bytes of one MSET that
                # never completed were reported as 0 before this was added
                self._discarded_request_bytes += (
                    len(conn.read_buffer) + conn.consumed_for_incomplete_command)
                conn.read_buffer.clear()
                # both zeroed for the same reason: from here on this connection holds only what
                # arrives after this pass, so the read path's own count cannot overlap this one
                conn.consumed_for_incomplete_command = 0
                if not conn.write_buffer:
                    # _abandon rather than _close: this walk runs after the loop has exited, where _guard no longer applies, so one close that raises would strand every connection behind it in iteration order
                    self._abandon(conn)
                    continue
                owing.append(conn)
                # reading is switched back on for every connection that owes bytes, because one whose reading is off here was switched off by the high-water mark and by nothing else: outside the drain _flush is the only thing that clears it, and end of input outside the drain closes the connection instead of holding it. left off, the requests it had not read would stay in its receive queue for the whole drain, and the close at the end would be a reset, which discards what the kernel had not yet sent. switched on, _read_and_dispatch reads and discards them as it does for any other connection. the mask cannot reach 0 by this: the buffer is non-empty and _flush keeps write interest equal to that, so write interest is registered, and for a connection already being read nothing changes. unlike _flush's resume, this does NOT re-arm conn.incomplete_since, and the asymmetry is the point: _flush resumes a connection this server intends to keep serving, where a half-sent command still has to be finished or timed out, while the drain resumes one only to empty its receive queue before closing it. nothing reads the deadline here -- the drain drives run_once() and never _tick(), so the sweep cannot run -- and arming it would mean a sweep that ever did run during a drain could close a connection the drain is still trying to deliver bytes to, which is the loss the drain exists to prevent
                self._loop.set_read_interest(conn, True)
            # write interest is not set for these: _flush leaves it equal to whether the buffer is non-empty after every send, and the only two places a reply is queued -- _dispatch_batch, and the protocol error in _read_and_dispatch -- each end in a _flush, so a connection that owes bytes is already registered for EVENT_WRITE. read interest does not last the drain: end of input on a connection that owes bytes clears it, leaving that connection registered for writing only, and the _draining guard in _flush keeps the resume there from undoing that. when its buffer empties _flush clears the write bit too, which leaves mask 0 and the applier spells that unregistered. the connection is still open and still in the set, and a connection that owes nothing has nothing left to wake the loop for, so that is the intended end of its part in the drain and not a lost registration
            # run_once() alone, not the main loop's pairing with _tick(): a tick here would run the expiry sweep, whose DELs after the snapshot are writes no snapshot holds, and a periodic save inside a shutdown that has already saved. the deadline is read between passes, so the bound is the timeout plus one select timeout
            while any(conn.write_buffer for conn in self._connections) and time.monotonic() < deadline:
                self._loop.run_once()
        finally:
            # cleared before the survivors are counted, so that _shutdown's own closes -- which run after this line is written -- add nothing to a figure that has already been reported
            self._counting_unread_at_close = False
            # and the backlog once more, for the same reason the survivors below are asked again:
            # the sweep at entry caught what was pending then, and the kernel goes on completing
            # handshakes for the whole of the drain. this is the last look before the figure is
            # computed, so what is left unaccounted is only what arrives between here and the
            # listener's own close in _shutdown -- the same irreducible window the survivors have
            if listener is not None:
                self._discarded_request_bytes += self._count_unaccepted_backlog(listener)
            # the connections the drain did not dispose of: still open, so _close has not asked them yet, and holding whatever arrived that no pass of the drain reached. the set at this point holds exactly the open members of this list, since the setup pass abandoned every connection that was not added to it and nothing has been added since. what is left unaccounted is what the peer's own kernel still holds unsent, which FIONREAD cannot reach for anyone: it answers for this socket's receive queue alone. measured, that costs nothing at the shipped drain timeout -- 0 bytes uncounted in 23 of 23, even where 360,246 were still in the client's kernel at the signal -- because the drain re-arms read interest and the peer delivers inside those five seconds; at --shutdown-drain-timeout 0 it is exactly what the peer still held, median 0 and at most 360,246 bytes of a 1,588,890-byte pipeline
            for conn in owing:
                if not conn.closed:
                    self._discarded_request_bytes += conn.unread_in_kernel()
            # a close that discarded a reply leaves it in the buffer, so a connection that is closed and still has bytes queued was lost, and one that is closed with an empty buffer had everything handed to the kernel first
            closed_owing = sum(1 for conn in owing if conn.closed and conn.write_buffer)
            still_owing = sum(1 for conn in owing if not conn.closed and conn.write_buffer)
            incomplete = bool(closed_owing or still_owing)
            # in a finally so that it is the only line this writes however it ends, including a pass that raises. it is a WARNING whenever any connection was owed bytes it did not get -- closed while owed them (a peer that went away, the write-buffer limit, an unhandled error) or still owed them at the deadline -- and INFO when none was, so the level says whether anything was lost
            # the third figure does not move the level, deliberately -- and not because the two kinds of byte cannot be told apart here. they can: has_incomplete_command is that question and this file asks it in two other places. what makes a threshold wrong is that the magnitudes invert. measured, one client caught mid-upload of a declared 8 MiB value reported 4,194,314 bytes against 1,588,890 for a whole discarded pipeline of fifty thousand SETs, and the first is bounded only by --max-value-size, so a threshold above the pipeline misses the pipeline and one below it warns on an ordinary upload. the word on this line is therefore about replies alone -- complete means every connection that was owed bytes got them, not that nothing inbound was thrown away -- and the figure is what has to be read for the other half
            # what the figure is for is that the loss is visible at all: before it, a stop that discarded a paused client's entire pipeline reported nothing but the replies it still owed
            logger.log(
                logging.WARNING if incomplete else logging.INFO,
                "shutdown drain %s; connections closed while owed bytes: %d; connections still owed bytes: %d; request bytes discarded undispatched: %d",
                "incomplete" if incomplete else "complete", closed_owing, still_owing,
                self._discarded_request_bytes)

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
        # after the handlers are armed and not before: a stop that arrived while main() was
        # building this server -- which includes the whole snapshot load -- is honoured here, and
        # one that arrives from this line on reaches _request_stop. the loop body then never runs
        # and the ordinary way out is taken whole, so the listener is still opened and closed, the
        # snapshot is still saved and the drain still writes its line: a stop during startup ends
        # exactly like a stop one millisecond into the loop, rather than being discarded
        if _consume_stop_requested_during_startup():
            self._running = False
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
                # leaving the select set stops accept dispatch while the listener itself stays open until _shutdown, so a replacement server cannot bind this port while this process is still running
                self._loop.unregister_listener(listener)
                # saved before the drain, not after: a client that never reads holds the drain to its whole timeout, and an operator who gives up and sends SIGKILL must not find the snapshot was the thing waiting behind it. the path is checked here and not through the arming guard, because this save runs with --snapshot-interval 0, where that guard never arms, and it is skipped when _ignored_snapshot_is_left_in_place says the file is to be left alone
                if self.snapshot_path is not None and not self._ignored_snapshot_is_left_in_place:
                    self._guard_task("shutdown snapshot save", self._save_snapshot)
                self._draining = True
                self._drain_for(self.shutdown_drain_timeout, listener)
            finally:
                # reached from every way out, including a drain that raised, so the port and the selector are still released
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
    # This function is the startup signal handlers and nothing else. Everything that can take
    # time or fail is in _parse_and_run below, so that a stop signal arriving at any point after
    # this process reached main() is recorded rather than lost:
    # 1. clear any request left by an earlier in-process main(), so this one starts clean
    # 2. install the recording handler for both stop signals
    # 3. do the whole of the work with them installed
    # 4. restore whatever was there before, on every way out, so an in-process main() leaves the
    #    host process's handlers as it found them
    # It is here rather than at import, for the reason logging is configured here rather than at
    # import: importing this module must leave an embedding program's handlers alone.
    # What is left unprotected is the interpreter's own startup and this module's imports, which
    # is the floor -- nothing this program runs can install a handler before it is running. It is
    # a fixed cost that does not grow with the keyspace, where the window this closes did: the
    # snapshot load is inside Server's construction, so it used to run with SIGTERM at its
    # default disposition, and in a container the server is PID 1, where the kernel does not
    # deliver a default-disposition signal at all. `docker stop` was discarded for as long as the
    # load took, the container served on for the rest of the stop timeout, died on SIGKILL with
    # no save, and a write acknowledged in that window was gone after a restart
    _consume_stop_requested_during_startup()
    with contextlib.ExitStack() as startup_signals:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous = signal.signal(signum, _note_stop_requested_during_startup)
            startup_signals.callback(signal.signal, signum, previous)
        _parse_and_run(argv)


def _parse_and_run(argv) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        _check_water_marks(
            args.write_buffer_high_water, args.write_buffer_low_water,
            "write buffer high water", "write buffer low water")
    except ValueError as exc:
        # caught around this call alone and not around the construction below, which would fold every other ValueError into a one-line message. a pair of flags that contradict each other is a usage error and exits 2, where a corrupt snapshot is not and exits 1
        parser.error(str(exc))
    # the one place logging is configured. importing this module must leave the host application's logging alone, and Server reads no level: it logs, and whoever started the process decided what is shown
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
            host=args.host,
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
