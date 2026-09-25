"""The periodic tick, both its arms, and the snapshot load: `_tick()` driven against an
injected monotonic clock installed by rebinding the `time` attribute on the `server`
module -- never by patching `time.monotonic` itself, which every module in this process
reads through -- so nothing here can flake on a loaded machine or wait on a wall clock.
Every `Server` a test constructs closes `server._loop` in a `finally`, and every path a
test names is inside pytest's `tmp_path`.

One test is the exception to both halves of that, and has to be: no other test in this
module drives `run()` at all -- however the rest reach the two arms, they leave the line
in `run()`'s loop body that calls `_tick()`, and `run()`'s own arming of the two
deadlines, verified by nothing. That one drives the real loop, so it runs on a wall
clock -- and in a subprocess with a deadline, because a loop that never ticks is a loop
that never returns, which in-process hangs the suite instead of failing it.
"""

import contextlib
import io
import logging
import os
import pathlib
import signal
import subprocess
import sys
import time
import types

import pytest

import persistence
import server as server_mod
from server import (
    MAX_SCHEDULABLE_INTERVAL, SWEEP_BUDGET_SECONDS, SWEEP_SAMPLE_SIZE, Server, build_arg_parser,
)
from store import Store


class _Clock:
    def __init__(self, start=1_000.0):
        self.t = start

    def monotonic(self):
        return self.t


@contextlib.contextmanager
def _injected_clock(start=1_000.0):
    # the server module's own `time` is rebound, not time.monotonic itself: patching the
    # real function reaches every module in this process
    clock = _Clock(start)
    real = server_mod.time
    server_mod.time = types.SimpleNamespace(
        monotonic=clock.monotonic, time=real.time, sleep=real.sleep
    )
    try:
        yield clock
    finally:
        server_mod.time = real


@contextlib.contextmanager
def _recording_calls(cls, method_name):
    # records each call's arguments before calling through to the original, and
    # restores the original method whether or not the block raises
    calls = []
    real = getattr(cls, method_name)

    def wrapper(self, *args, **kwargs):
        calls.append(args)
        return real(self, *args, **kwargs)

    setattr(cls, method_name, wrapper)
    try:
        yield calls
    finally:
        setattr(cls, method_name, real)


def _flag_help(parser, flag):
    # the parser's own action list rather than format_help(): the rendered text wraps at
    # whatever width argparse finds, and these help strings name other flags, so a wrapped
    # line beginning with `--snapshot-path` truncates any line-based reader of the entry
    for action in parser._actions:
        if flag in action.option_strings:
            return " ".join(action.help.split())
    raise AssertionError("%s is not a flag this parser knows" % flag)


# normal


def test_a_periodic_arm_does_not_fire_before_its_interval_has_elapsed():
    with _injected_clock() as clock:
        server = Server(0, snapshot_interval=0, expiry_sweep_interval=100)
        try:
            fired = []
            server._sweep_expired = lambda: fired.append(clock.monotonic())
            server._next_sweep_at = clock.monotonic() + 0.1
            clock.t += 0.099
            server._tick()
            assert fired == [], "an arm fired before its interval elapsed"
        finally:
            server._loop.close()


def test_a_snapshot_written_by_the_save_arm_loads_back_into_an_equal_keyspace(tmp_path):
    path = tmp_path / "dump.mrdb"
    with _injected_clock() as clock:
        server = Server(0, snapshot_path=str(path), snapshot_interval=60,
                        expiry_sweep_interval=0)
        try:
            server._store.write(b"a", b"v", keep_ttl=False)
            server._store.expire_at(b"a", server._store.now_ms() + 3_600_000)
            server._store.write(b"b", b"w", keep_ttl=False)
            server._next_snapshot_at = clock.monotonic() + 60
            clock.t += 61.0
            server._tick()
            live_items = sorted(server._store.snapshot_items())
            live_deadline_a = server._store.deadline(b"a")
        finally:
            server._loop.close()
    loaded = persistence.load(str(path))
    assert sorted(loaded.snapshot_items()) == live_items, (
        "the reloaded keyspace does not match what was saved -- keys, kinds, values or "
        "an absolute deadline diverged")
    assert loaded.deadline(b"a") == live_deadline_a, (
        "b\"a\"'s TTL did not survive the round trip")


def test_the_sweep_reclaims_a_key_no_client_asked_about():
    with _injected_clock() as clock:
        server = Server(0, snapshot_interval=0, expiry_sweep_interval=100)
        try:
            server._store.write(b"forgotten", b"v", keep_ttl=False)
            server._store.expire_at(b"forgotten", server._store.now_ms() - 1)
            server._next_sweep_at = clock.monotonic() + 0.1
            clock.t += 0.2
            server._tick()
            assert b"forgotten" not in server._store._data
            server._store.check_invariants()
        finally:
            server._loop.close()


# edge


def test_a_periodic_arm_fires_at_most_once_per_tick_not_once_per_missed_interval():
    with _injected_clock() as clock:
        server = Server(0, snapshot_interval=0, expiry_sweep_interval=100)
        try:
            fired = []
            server._sweep_expired = lambda: fired.append(clock.monotonic())
            server._next_sweep_at = clock.monotonic() + 0.1

            per_tick = []
            for step in [0.03] * 12:
                clock.t += step
                before = len(fired)
                server._tick()
                per_tick.append(len(fired) - before)
            assert per_tick == [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1], per_tick

            # a clock jump spanning ten intervals fires once, not a burst of skipped sweeps
            fired.clear()
            server._next_sweep_at = clock.monotonic() + 0.1
            clock.t += 1.0
            server._tick()
            assert len(fired) == 1, (
                "a jump spanning ten intervals fired more than once", fired)
        finally:
            server._loop.close()


def test_an_interval_of_zero_arms_nothing_and_never_fires(tmp_path):
    with _injected_clock() as clock:
        # a real path, not None: the CLI always supplies one, so a zero interval is the
        # only thing between it and a save on every tick. with no path the snapshot arm
        # stays unarmed for the other reason, and the interval's own check goes untested
        server = Server(0, snapshot_path=str(tmp_path / "dump.mrdb"), snapshot_interval=0,
                        expiry_sweep_interval=0)
        try:
            server._arm_periodic_tasks()
            assert (server._next_sweep_at, server._next_snapshot_at) == (None, None)
            calls = []
            server._sweep_expired = lambda: calls.append("sweep")
            server._save_snapshot = lambda: calls.append("save")
            clock.t += 3_600.0
            server._tick()
            assert calls == [], calls
        finally:
            server._loop.close()


def test_a_server_constructed_and_left_unrun_schedules_nothing():
    server = Server(0)
    try:
        assert server._next_sweep_at is None
        assert server._next_snapshot_at is None
    finally:
        server._loop.close()


def test_the_snapshot_arm_does_not_fire_without_a_snapshot_path(tmp_path):
    with _injected_clock() as clock:
        server = Server(0, snapshot_path=None, snapshot_interval=60, expiry_sweep_interval=0)
        try:
            server._arm_periodic_tasks()
            assert server._next_snapshot_at is None
            calls = []
            server._save_snapshot = lambda: calls.append("save")
            clock.t += 3_600.0
            server._tick()
            assert calls == [], "the snapshot arm fired with no path configured"
        finally:
            server._loop.close()


def test_the_expiry_sweep_interval_is_read_as_milliseconds_not_seconds():
    def fires(advance_seconds):
        with _injected_clock() as clock:
            server = Server(0, snapshot_interval=0, expiry_sweep_interval=100)
            try:
                fired = []
                server._sweep_expired = lambda: fired.append(1)
                server._arm_periodic_tasks()
                clock.t += advance_seconds
                server._tick()
                return len(fired)
            finally:
                server._loop.close()

    assert fires(0.099) == 0, "a 99 ms advance fired a 100 ms interval"
    assert fires(0.101) == 1, (
        "a 101 ms advance did not fire a 100 ms interval -- milliseconds were read as "
        "seconds, which is a sweep every hundred seconds")

    # the arm above only proves the first deadline was read as milliseconds; _tick()
    # re-arms the same interval on every fire through its own division, which the
    # single fire above never revisits
    with _injected_clock() as clock:
        server = Server(0, snapshot_interval=0, expiry_sweep_interval=100)
        try:
            fired = []
            server._sweep_expired = lambda: fired.append(1)
            server._arm_periodic_tasks()
            clock.t += 0.101
            server._tick()
            assert len(fired) == 1, "the arm did not fire after its first interval"
            clock.t += 0.099
            server._tick()
            assert len(fired) == 1, (
                "the re-arm fired before a full 100 ms interval had elapsed since the "
                "last fire -- milliseconds were read as seconds on the re-arm")
            clock.t += 0.002
            server._tick()
            assert len(fired) == 2, (
                "the re-arm did not fire a full 100 ms interval after the last fire")
        finally:
            server._loop.close()


def test_the_sweep_re_loops_while_more_than_a_quarter_of_a_sample_expired():
    server = Server(0)
    try:
        store = server._store
        now = store.now_ms()
        for i in range(SWEEP_SAMPLE_SIZE):
            key = b"k%d" % i
            store.write(key, b"v", keep_ttl=False)
            store.expire_at(key, now - 1 if i < 6 else now + 3_600_000)
        with _recording_calls(Store, "sample_and_expire") as calls:
            server._sweep_expired()
        store.check_invariants()
        assert len(calls) == 2, (
            "six of twenty is above a quarter and must re-loop once", calls)
        assert store.live_count() == 14
    finally:
        server._loop.close()


def test_the_sweep_stops_on_a_sample_at_or_below_the_threshold():
    server = Server(0)
    try:
        store = server._store
        now = store.now_ms()
        for i in range(SWEEP_SAMPLE_SIZE):
            key = b"k%d" % i
            store.write(key, b"v", keep_ttl=False)
            store.expire_at(key, now - 1 if i < 5 else now + 3_600_000)
        with _recording_calls(Store, "sample_and_expire") as calls:
            server._sweep_expired()
        assert len(calls) == 1, (
            "five of twenty is exactly a quarter, not more -- the threshold was "
            "compared with >=", calls)
        assert store.live_count() == 15
    finally:
        server._loop.close()


def test_the_sweep_stops_after_one_sample_on_an_empty_expiry_index():
    server = Server(0)
    try:
        with _recording_calls(Store, "sample_and_expire") as calls:
            server._sweep_expired()
        assert len(calls) == 1, ("an empty index must stop the loop after one sample", calls)
    finally:
        server._loop.close()


def test_the_sweep_drains_the_effect_queue_once_per_tick_not_once_per_sampling_pass():
    with _injected_clock():
        server = Server(0)
        try:
            store = server._store
            now = store.now_ms()
            # ten samples' worth of wholly expired keys, and a clock that does not
            # advance, so the loop runs until the index is empty rather than until the
            # budget is spent
            for i in range(SWEEP_SAMPLE_SIZE * 10):
                key = b"k%d" % i
                store.write(key, b"v", keep_ttl=False)
                store.expire_at(key, now - 1)
            with _recording_calls(Store, "sample_and_expire") as samples, \
                 _recording_calls(Store, "take_effects") as drains:
                server._sweep_expired()
            assert len(drains) == 1, (
                "the sweep drained once per sampling pass rather than once per tick",
                len(drains), len(samples))
            assert len(samples) > 1, (
                "the sweep made a single pass, so this run could not have distinguished "
                "a per-pass drain from a per-tick one", len(samples))
            assert store._effects == [], (
                "the sweep left effects for the next dispatched command to be blamed for")
            assert store.live_count() == 0 and store._data == {}
            store.check_invariants()
        finally:
            server._loop.close()


def test_a_negative_interval_is_refused_at_the_cli_and_in_the_constructor():
    for flag, label in (("--snapshot-interval", "snapshot interval"),
                        ("--expiry-sweep-interval", "expiry sweep interval")):
        err = io.StringIO()
        try:
            with contextlib.redirect_stderr(err):
                build_arg_parser().parse_args([flag, "-1"])
            pytest.fail("%s -1 was accepted" % flag)
        except SystemExit as exc:
            assert exc.code == 2, (flag, exc.code)
        text = err.getvalue()
        assert "%s cannot be negative; 0 disables the check, not -1" % label in text, (
            flag, text)
        assert "_check_not_negative" not in text and "_numeric_limit" not in text, (
            "a private validator's name leaked into a user-facing message", text)

    for kwargs, label in (({"snapshot_interval": -1}, "snapshot_interval"),
                          ({"expiry_sweep_interval": -1}, "expiry_sweep_interval")):
        try:
            Server(0, **kwargs)
        except ValueError as exc:
            assert str(exc) == (
                "%s cannot be negative; 0 disables the check, not -1" % label), str(exc)
        else:
            pytest.fail("Server(0, **%r) was accepted" % kwargs)


def test_the_cli_snapshot_path_default_and_the_constructor_default_differ():
    args = build_arg_parser().parse_args([])
    assert args.snapshot_path == "./dump.mrdb", args.snapshot_path
    server = Server(0)
    try:
        assert server.snapshot_path is None, (
            "the constructor defaults to None and the CLI to ./dump.mrdb, deliberately: "
            "a constructor default of ./dump.mrdb writes into whatever directory a test "
            "started in", server.snapshot_path)
    finally:
        server._loop.close()


def test_the_interval_and_ignore_defaults_are_the_documented_values_at_both_doors():
    # README.md publishes both interval defaults, and every other test here passes its
    # intervals explicitly, so a default that moved would move with nothing to notice it
    args = build_arg_parser().parse_args([])
    assert (args.snapshot_interval, args.expiry_sweep_interval, args.ignore_snapshot) == (
        60, 100, False), (args.snapshot_interval, args.expiry_sweep_interval,
                          args.ignore_snapshot)
    server = Server(0)
    try:
        assert (server.snapshot_interval, server.expiry_sweep_interval,
                server.ignore_snapshot) == (60, 100, False)
    finally:
        server._loop.close()


# error


def test_an_arm_that_raises_is_logged_with_its_traceback_and_keeps_its_schedule():
    records = []
    handler = logging.Handler()
    handler.emit = records.append
    logging.getLogger("server").addHandler(handler)
    try:
        with _injected_clock() as clock:
            server = Server(0, snapshot_interval=0, expiry_sweep_interval=100)
            try:
                attempts = []

                def raising():
                    attempts.append(clock.monotonic())
                    raise OSError(28, "No space left on device")

                server._sweep_expired = raising
                server._next_sweep_at = clock.monotonic() + 0.1
                for _ in range(3):
                    clock.t += 0.1
                    server._tick()
                assert len(attempts) == 3, (
                    "an arm that raised was not retried on its next interval", attempts)

                # the loop above advances by exactly one interval per attempt, so a
                # deadline left stuck in the past by the last failure would fire on
                # that same cadence and this could not tell the two apart. ticking at
                # the instant of the last failure, then short of a full interval, then
                # past one, is what separates "fires again next interval" from "fires
                # on every tick"
                server._tick()
                assert len(attempts) == 3, (
                    "a further attempt happened with no time having passed at all",
                    attempts)
                clock.t += 0.05
                server._tick()
                assert len(attempts) == 3, (
                    "a further attempt happened before a full interval had elapsed "
                    "since the last failure", attempts)
                clock.t += 0.06
                server._tick()
                assert len(attempts) == 4, (
                    "the arm did not retry once a full interval had elapsed since its "
                    "last failure", attempts)
            finally:
                server._loop.close()
    finally:
        logging.getLogger("server").removeHandler(handler)
    logged = [r for r in records if r.levelno >= logging.ERROR and r.exc_info]
    assert len(logged) == 4, "each failure must be logged with its traceback"
    assert all("expiry sweep" in r.getMessage() for r in logged), (
        "the log line must name which arm failed", [r.getMessage() for r in logged])


def test_an_arm_that_raises_does_not_escape_the_tick(tmp_path):
    scratch_calls = []
    with _injected_clock() as clock:
        server = Server(0, snapshot_interval=60, expiry_sweep_interval=0)
        try:
            def failing_save():
                scratch_calls.append(1)
                raise OSError(28, "No space left on device")

            server.snapshot_path = str(tmp_path / "unused.mrdb")
            server._save_snapshot = failing_save
            server._next_snapshot_at = clock.monotonic() + 60
            clock.t += 60
            server._tick()  # must not raise
        finally:
            server._loop.close()
    assert scratch_calls == [1], "the failing arm was never even attempted"


def test_the_save_arm_leaves_the_previous_snapshot_intact_when_its_rename_fails(
    tmp_path, monkeypatch
):
    # persistence.save()'s own tests prove the swap is atomic; this is what proves the save
    # arm goes through it. an arm that wrote its path in place would pass every test that
    # only reads the file back afterwards, and would destroy the previous snapshot the
    # moment anything went wrong partway through the write
    path = tmp_path / "dump.mrdb"
    previous = Store()
    previous.write(b"previous", b"v", keep_ttl=False)
    persistence.save(previous, str(path))
    before = path.read_bytes()

    def refused_rename(src, dst):
        raise OSError("the rename was refused")

    with _injected_clock() as clock:
        server = Server(0, snapshot_path=str(path), snapshot_interval=60,
                        expiry_sweep_interval=0)
        try:
            server._store.write(b"newer", b"v", keep_ttl=False)
            server._next_snapshot_at = clock.monotonic() + 60
            clock.t += 61.0
            monkeypatch.setattr(os, "rename", refused_rename)
            server._tick()
            monkeypatch.undo()
        finally:
            server._loop.close()
    assert path.read_bytes() == before, "the save arm wrote over the previous snapshot"
    assert [p.name for p in tmp_path.iterdir()] == ["dump.mrdb"], (
        "the failed save left a temporary file behind", sorted(p.name for p in tmp_path.iterdir()))


def test_the_sweep_stops_on_its_time_budget_not_on_an_iteration_count():
    def passes_when_each_sample_costs(cost):
        with _injected_clock() as clock:
            real_sample = Store.sample_and_expire
            calls = []

            def sample(self, count):
                calls.append(clock.monotonic())
                clock.t += cost
                return (count, count)

            Store.sample_and_expire = sample
            try:
                server = Server(0)
                try:
                    server._sweep_expired()
                    return len(calls)
                finally:
                    server._loop.close()
            finally:
                Store.sample_and_expire = real_sample

    slow = passes_when_each_sample_costs(SWEEP_BUDGET_SECONDS / 3)
    fast = passes_when_each_sample_costs(SWEEP_BUDGET_SECONDS / 1000)
    assert slow <= 6, ("a sample costing a third of the budget ran %d passes" % slow)
    assert fast >= 100, (
        "a sample costing a thousandth of the budget ran only %d passes, which is what "
        "an iteration cap gives" % fast)
    assert fast > 10 * slow, (
        "the two arms ran comparable numbers of passes, so this run could not have "
        "distinguished a time budget from an iteration cap", slow, fast)
    # both arms take their costs from SWEEP_BUDGET_SECONDS itself, so a budget raised a
    # hundredfold scales them with it and passes. the millisecond the sweep is held to is
    # pinned here, against the literal
    assert SWEEP_BUDGET_SECONDS == 0.001, SWEEP_BUDGET_SECONDS


def test_a_corrupt_snapshot_refuses_construction_and_opens_no_selector(tmp_path):
    good = tmp_path / "good.mrdb"
    bad = tmp_path / "bad.mrdb"
    seed = Store()
    seed.write(b"k", b"v", keep_ttl=False)
    persistence.save(seed, str(good))
    blob = bytearray(good.read_bytes())
    blob[len(blob) // 2] ^= 0x01
    bad.write_bytes(bytes(blob))

    built = []
    real_loop = server_mod.EventLoop

    def counting_event_loop(*args, **kwargs):
        built.append(1)
        return real_loop(*args, **kwargs)

    server_mod.EventLoop = counting_event_loop
    try:
        try:
            Server(0, snapshot_path=str(bad))
            pytest.fail("a corrupt snapshot was accepted")
        except persistence.SnapshotError as exc:
            assert str(bad) in str(exc), "the refusal must name the path"
        assert built == [], (
            "the load ran after EventLoop was constructed, so every refused "
            "construction leaks a selector", built)

        # the control: an empty counter is a load placed early, not a counter that
        # cannot see anything
        ok = Server(0, snapshot_path=str(good))
        try:
            assert built == [1], built
            assert ok._store.lookup(b"k") == b"v"
        finally:
            ok._loop.close()
    finally:
        server_mod.EventLoop = real_loop


def test_startup_names_a_temporary_file_an_interrupted_save_left_and_leaves_it_alone(
    tmp_path
):
    path = tmp_path / "dump.mrdb"
    good = Store()
    good.write(b"k", b"v", keep_ttl=False)
    persistence.save(good, str(path))
    stranded = tmp_path / "dump.mrdb.x1y2z3w4.tmp.mrdb"
    stranded.write_bytes(b"left by a killed save")

    def warnings_while(step):
        records = []
        handler = logging.Handler()
        handler.emit = records.append
        logging.getLogger("server").addHandler(handler)
        try:
            step()
        finally:
            logging.getLogger("server").removeHandler(handler)
        return [r.getMessage() for r in records if r.levelno >= logging.WARNING]

    def construct(interval):
        server = Server(0, snapshot_path=str(path), snapshot_interval=interval)
        try:
            assert server._store.lookup(b"k") == b"v", "the snapshot itself was not loaded"
        finally:
            server._loop.close()

    # saving on and saving off: such a file belongs to the path, not to the interval
    for interval in (60, 0):
        warned = warnings_while(lambda: construct(interval))
        assert [m for m in warned if stranded.name in m and str(path) in m], (interval, warned)
        assert stranded.read_bytes() == b"left by a killed save", (
            "startup touched a file that may hold the only copy of a newer save", interval)

    # before the load, not after it: a start refused over a corrupt snapshot is the one
    # moment that file may be the way back, so its name has to be out by then
    blob = bytearray(path.read_bytes())
    blob[len(blob) // 2] ^= 0x01
    path.write_bytes(bytes(blob))

    def refused():
        with pytest.raises(persistence.SnapshotError):
            Server(0, snapshot_path=str(path))

    warned = warnings_while(refused)
    assert [m for m in warned if stranded.name in m], warned
    assert stranded.read_bytes() == b"left by a killed save"

    # and ahead of the writability check as well: a start refused because the first save
    # could not write the path names the file just the same. the refusal is stood in for by
    # failing the check's own probe, since a test run as root can write a mode-0555 directory
    def unwritable():
        with pytest.MonkeyPatch.context() as patch:
            def refuse(*args, **kwargs):
                raise PermissionError(13, "Permission denied")

            patch.setattr(persistence.tempfile, "mkstemp", refuse)
            with pytest.raises(persistence.SnapshotError, match="not writable"):
                Server(0, snapshot_path=str(path))

    warned = warnings_while(unwritable)
    assert [m for m in warned if stranded.name in m], warned
    assert stranded.read_bytes() == b"left by a killed save"


def test_the_two_shapes_of_stranded_temporary_file_are_warned_about_differently(tmp_path):
    # is_legacy_temporary_name() is the one thing that tells the two shapes apart: a name
    # built from the snapshot's own basename can only be this path's own interrupted
    # save, where the bare tmpXXXXXXXX tempfile.mkstemp() itself would leave carries
    # nothing about which snapshot -- or which program -- left it, so a warning claiming
    # it holds THIS save's snapshot is a guess dressed as a fact
    path = tmp_path / "dump.mrdb"
    good = Store()
    good.write(b"k", b"v", keep_ttl=False)
    persistence.save(good, str(path))
    current_shape = tmp_path / "dump.mrdb.a1b2c3d4.tmp.mrdb"
    current_shape.write_bytes(b"left by a killed save of this snapshot")
    legacy_shape = tmp_path / "tmpabcd1234"
    legacy_shape.write_bytes(b"left by an earlier build, or by something else entirely")

    records = []
    handler = logging.Handler()
    handler.emit = records.append
    logging.getLogger("server").addHandler(handler)
    try:
        server = Server(0, snapshot_path=str(path), snapshot_interval=60)
    finally:
        logging.getLogger("server").removeHandler(handler)
    try:
        messages = [r.getMessage() for r in records if r.levelno >= logging.WARNING]
        current_shape_warnings = [m for m in messages if current_shape.name in m]
        legacy_shape_warnings = [m for m in messages if legacy_shape.name in m]
        assert current_shape_warnings, messages
        assert legacy_shape_warnings, messages
        assert "may hold that save's snapshot" in current_shape_warnings[0], (
            current_shape_warnings)
        assert "may hold that save's snapshot" not in legacy_shape_warnings[0], (
            "the bare temporary-file name was described as holding this save's own "
            "snapshot, which nothing about its name can say", legacy_shape_warnings)
        assert "may have nothing to do with this server" in legacy_shape_warnings[0], (
            legacy_shape_warnings)
    finally:
        server._loop.close()


def test_a_path_the_first_save_could_not_write_refuses_construction_and_opens_no_selector(
    tmp_path
):
    missing = str(tmp_path / "missing" / "dump.mrdb")
    (tmp_path / "in-the-way.mrdb").mkdir()
    in_the_way = str(tmp_path / "in-the-way.mrdb")

    built = []
    real_loop = server_mod.EventLoop

    def counting_event_loop(*args, **kwargs):
        built.append(1)
        return real_loop(*args, **kwargs)

    server_mod.EventLoop = counting_event_loop
    try:
        # --ignore-snapshot is refused as well: a replacement at the next interval is
        # exactly what its warning promises, and neither path could take one as it stands
        for path in (missing, in_the_way):
            for ignore in (False, True):
                with pytest.raises(persistence.SnapshotError, match="cannot write snapshot"):
                    Server(0, snapshot_path=path, ignore_snapshot=ignore)
        # with saving off the directory at the path is still refused, by the load rather
        # than by the write check: without --ignore-snapshot the refusal never depends on
        # the interval, only which of the two reasons it gives does. with the flag and
        # saving off, nothing reads or writes the path at all and the server starts
        with pytest.raises(persistence.SnapshotError, match="cannot read snapshot"):
            Server(0, snapshot_path=in_the_way, snapshot_interval=0)
        assert built == [], ("a refused construction opened a selector", built)

        # the control: with saving off nothing is ever written there, so the same missing
        # directory constructs -- the refusal is about saves, not about the path as such
        off = Server(0, snapshot_path=missing, snapshot_interval=0)
        try:
            assert built == [1], built
            assert off._store.live_count() == 0
        finally:
            off._loop.close()
    finally:
        server_mod.EventLoop = real_loop

    err = io.StringIO()
    try:
        with contextlib.redirect_stderr(err):
            server_mod.main(["--port", "0", "--snapshot-path", missing])
        pytest.fail("main() started over a path the first save could not write")
    except SystemExit as exc:
        assert exc.code == 1, ("refused like a corrupt snapshot, not argparse's 2", exc.code)
    assert err.getvalue().startswith("error: cannot write snapshot %s" % missing), (
        err.getvalue())


def test_ignore_snapshot_starts_empty_and_warns_naming_the_path(tmp_path):
    seed = Store()
    seed.write(b"k", b"v", keep_ttl=False)
    bad = tmp_path / "bad.mrdb"
    persistence.save(seed, str(bad))
    blob = bytearray(bad.read_bytes())
    blob[len(blob) // 2] ^= 0x01
    bad.write_bytes(bytes(blob))

    records = []
    handler = logging.Handler()
    handler.emit = records.append
    logging.getLogger("server").addHandler(handler)
    try:
        server = Server(0, snapshot_path=str(bad), ignore_snapshot=True)
        try:
            assert server._store.live_count() == 0
            assert server.snapshot_path == str(bad), (
                "--ignore-snapshot keeps saving to the path it refused to read")
        finally:
            server._loop.close()
    finally:
        logging.getLogger("server").removeHandler(handler)
    warned = [r for r in records
              if r.levelno >= logging.WARNING and str(bad) in r.getMessage()]
    assert warned, ("--ignore-snapshot must warn, naming the path it will replace",
                    [r.getMessage() for r in records])

    # snapshot_interval=0 arms no save at all, so the same warning must not promise a
    # replacement periodic saving, being off, can never deliver
    records_no_interval = []
    handler_no_interval = logging.Handler()
    handler_no_interval.emit = records_no_interval.append
    logging.getLogger("server").addHandler(handler_no_interval)
    try:
        server = Server(0, snapshot_path=str(bad), snapshot_interval=0,
                        ignore_snapshot=True)
        try:
            assert server._store.live_count() == 0
        finally:
            server._loop.close()
    finally:
        logging.getLogger("server").removeHandler(handler_no_interval)
    warned_no_interval = [r for r in records_no_interval
                          if r.levelno >= logging.WARNING and str(bad) in r.getMessage()]
    assert warned_no_interval, (
        "--ignore-snapshot must still warn with --snapshot-interval 0, naming the path",
        [r.getMessage() for r in records_no_interval])
    assert not any("replaced" in r.getMessage() for r in warned_no_interval), (
        "an interval of 0 arms no save, so the warning must not promise a replacement",
        [r.getMessage() for r in warned_no_interval])


def test_ignore_snapshot_warns_about_nothing_when_no_file_is_there(tmp_path):
    # the warning names a file it is about to discard and says what becomes of it; with
    # nothing at the path there is no file to discard and no replacement to promise
    records = []
    handler = logging.Handler()
    handler.emit = records.append
    logging.getLogger("server").addHandler(handler)
    try:
        server = Server(0, snapshot_path=str(tmp_path / "absent.mrdb"), ignore_snapshot=True)
        try:
            assert server._store.live_count() == 0
        finally:
            server._loop.close()
    finally:
        logging.getLogger("server").removeHandler(handler)
    assert not [r for r in records if r.levelno >= logging.WARNING], (
        [r.getMessage() for r in records])


def test_main_exits_one_naming_the_path_when_the_snapshot_is_corrupt(tmp_path):
    bad = tmp_path / "bad.mrdb"
    bad.write_bytes(b"MRDB" + b"\x00" * 32)
    err = io.StringIO()
    try:
        with contextlib.redirect_stderr(err):
            server_mod.main(["--port", "0", "--snapshot-path", str(bad)])
        pytest.fail("main() started over a corrupt snapshot")
    except SystemExit as exc:
        assert exc.code == 1, (
            "a corrupt snapshot is not a usage error, so not argparse's 2", exc.code)
    assert str(bad) in err.getvalue(), err.getvalue()


REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# run()'s own loop, in a subprocess with a deadline: the failure this pins is a loop that
# ticks nothing, and the stop below comes from a plain sleep on another thread so that
# failure prints counts rather than hanging the suite. the tick counter is a subclass in
# the shape tests/test_server_lifecycle.py:344-374 already uses for _open_listener
_TICK_FROM_RUN = """
import pathlib, sys, threading, time
sys.path.insert(0, sys.argv[1])
from server import Server

ticks = []
armed = []


class Counting(Server):
    def _tick(self):
        ticks.append(1)
        # read inside the loop rather than after run() returns, because run() disarms both
        # on its way out
        armed.append((self._next_sweep_at is not None, self._next_snapshot_at is not None))
        super()._tick()


server = Counting(0, snapshot_path=sys.argv[2], snapshot_interval=1,
                  expiry_sweep_interval=50)
server._store.write(b"forgotten", b"v", keep_ttl=False)
server._store.expire_at(b"forgotten", server._store.now_ms() - 1)
server._store.write(b"kept", b"v", keep_ttl=False)


def stop_later():
    # long enough to clear the one-second snapshot interval below, not just the sweep's
    # 50 ms one -- a shorter sleep could still show ticks and a swept key while never
    # proving the save arm fires at all
    time.sleep(2.0)
    server._running = False


threading.Thread(target=stop_later, daemon=True).start()
server.run()

print("ticks=%d" % len(ticks))
print("sweep-armed=%s" % (bool(armed) and all(sweep for sweep, _ in armed)))
print("snapshot-armed=%s" % (bool(armed) and all(snapshot for _, snapshot in armed)))
print("disarmed-after=%s" % (server._next_sweep_at is None and server._next_snapshot_at is None))
print("swept=%s" % (b"forgotten" not in server._store._data))
print("saved=%s" % pathlib.Path(sys.argv[2]).exists())
"""


def test_run_arms_both_deadlines_and_ticks_from_its_own_loop(tmp_path):
    # the one test that drives run() itself -- no other test in this module calls it, so
    # deleting the tick from run()'s loop body, or deleting the self._arm_periodic_tasks()
    # call from run(), leaves every other test in this module green and ships a server that
    # never sweeps and never saves
    path = tmp_path / "dump.mrdb"
    probe = subprocess.run(
        [sys.executable, "-c", _TICK_FROM_RUN, str(REPO_ROOT), str(path)],
        capture_output=True, text=True, timeout=60,
    )
    assert probe.returncode == 0, (probe.returncode, probe.stdout, probe.stderr)
    seen = dict(line.split("=", 1) for line in probe.stdout.splitlines() if "=" in line)

    assert int(seen["ticks"]) > 0, (
        "run()'s loop body never called _tick(), so neither arm can fire however the two "
        "of them are written", probe.stdout, probe.stderr)
    assert seen["sweep-armed"] == "True" and seen["snapshot-armed"] == "True", (
        "run() did not arm both deadlines for every tick, and _tick() cannot fire an arm "
        "whose deadline is still None", probe.stdout)
    assert seen["disarmed-after"] == "True", (
        "run() returned with a deadline still set, so a stopped server goes on reporting a "
        "save schedule through CONFIG GET save", probe.stdout)
    assert seen["swept"] == "True", (
        "two seconds of a running loop at a 50 ms sweep interval reclaimed nothing -- the "
        "interval was armed as seconds, which is a sweep every fifty seconds", probe.stdout)
    assert seen["saved"] == "True", (
        "two seconds of a running loop at a one-second snapshot interval wrote no file",
        probe.stdout)
    loaded = persistence.load(str(path))
    assert b"kept" in loaded._data, (
        "the save arm wrote a file that does not hold the keyspace", sorted(loaded._data))


def test_ignore_snapshot_discards_a_valid_snapshot_and_says_so(tmp_path):
    # both existing checks pair the flag with a CORRUPTED file, which is why nothing
    # noticed that the branch never calls persistence.load at all: a perfectly good
    # snapshot is discarded too, and the next interval overwrites it
    seed = Store()
    for key in (b"cart:42", b"sessions", b"users"):
        seed.write(key, b"v", keep_ttl=False)
    path = tmp_path / "dump.mrdb"
    persistence.save(seed, str(path))

    ignoring = Server(0, snapshot_path=str(path), ignore_snapshot=True)
    try:
        assert ignoring._store.live_count() == 0, (
            "--ignore-snapshot is unconditional and discarded nothing here",
            sorted(ignoring._store._data))
    finally:
        ignoring._loop.close()

    # the control: the same file without the flag, so the empty keyspace above is the flag
    # and not an unloadable fixture
    loading = Server(0, snapshot_path=str(path))
    try:
        assert sorted(loading._store._data) == [b"cart:42", b"sessions", b"users"], (
            sorted(loading._store._data))
    finally:
        loading._loop.close()

    # every surface that describes this flag: a wording corrected in one place can
    # survive in another place nothing checks, so a check that inspects only one
    # surface can still miss it
    UNCONDITIONAL = "whether or not it is readable"
    FORBIDDEN = ("empty keyspace if", "if the snapshot", "if a snapshot", "if the file",
                 "that fails to load", "that is corrupt", "that cannot be read",
                 "empty over a corrupt", "corrupt file rather than refuse")
    # both are required: the help text and README.md each have to spell the flag out
    # as unconditional, because a wording corrected in only one of them can still
    # survive in the other, where nothing checks it
    REQUIRED = {"--help", "README.md"}
    surfaces = [("--help", _flag_help(build_arg_parser(), "--ignore-snapshot"))]
    published = REPO_ROOT / "README.md"
    # README.md travels with this suite wherever it goes, so it is required to exist
    assert published.exists(), (
        "README.md is where this claim is published and this tree has none")
    joined = " ".join(published.read_text().split())
    for after in joined.split("--ignore-snapshot")[1:]:
        surfaces.append(("README.md", after[:400]))

    spelled_out = {name for name, text in surfaces if UNCONDITIONAL in text.lower()}
    assert REQUIRED <= spelled_out, (
        "the flag never reads the file, so no surface may make the empty start conditional "
        "on the file being corrupt -- an operator who put it in a unit file on that reading "
        "loses the keyspace and then the only copy of it", sorted(spelled_out))
    conditional = [(name, bad) for name, text in surfaces
                   for bad in FORBIDDEN if bad in text.lower()]
    assert not conditional, (
        "a surface still promises a conditional the branch does not implement", conditional)

    # the control: a sentence that makes the empty start conditional on a broken file,
    # planted here, has to be refused by both arms above -- the one requiring the
    # unconditional wording and the one forbidding the conditional phrases -- or the
    # check could not fail
    planted = ("--ignore-snapshot takes no value of its own: passed, a snapshot file that "
               "fails to load starts the keyspace empty instead of refusing to start.")
    assert UNCONDITIONAL not in planted, "the arm that requires the wording cannot fail"
    assert [bad for bad in FORBIDDEN if bad in planted], (
        "the arm that refuses a conditional cannot see the wording it exists to refuse")


def test_the_snapshot_arm_re_arms_a_whole_interval_out(tmp_path):
    with _injected_clock() as clock:
        server = Server(0, snapshot_path=str(tmp_path / "dump.mrdb"),
                        snapshot_interval=60, expiry_sweep_interval=0)
        try:
            saves = []
            server._save_snapshot = lambda: saves.append(clock.monotonic())
            server._next_snapshot_at = clock.monotonic() + 60
            clock.t += 61.0
            server._tick()
            assert len(saves) == 1, ("the first save never fired", saves)
            # the re-arm read back, which one tick alone never does. a spurious /1000
            # saves every 60 ms and pins the single-threaded loop; a spurious *1000 saves
            # every 16.7 hours and makes README's "up to one interval" a lie by three
            # orders of magnitude. both are one token
            clock.t += 59.0
            server._tick()
            assert len(saves) == 1, (
                "the arm fired 59 seconds into its 60-second interval -- the re-arm was "
                "divided by a thousand", saves)
            clock.t += 2.0
            server._tick()
            assert len(saves) == 2, (
                "the arm did not fire 61 seconds after the last save -- the re-arm was "
                "multiplied by a thousand", saves)
        finally:
            server._loop.close()


def test_a_sampling_pass_that_raises_still_drains_what_the_earlier_passes_queued():
    # the drain is the sweep's last act whatever ends it, because the queue it leaves
    # behind is read by whoever comes next: _dispatch_batch drains once per dispatched
    # command, so DELs the sweep queued and did not take back are handed to the next
    # command as if that command had caused them. a drain written as the loop's last
    # line instead of a finally is skipped by exactly the case that leaves entries on
    # the queue -- a pass that raises after an earlier one already removed keys
    with _injected_clock():
        server = Server(0)
        try:
            store = server._store
            now = store.now_ms()
            for i in range(SWEEP_SAMPLE_SIZE * 2):
                key = b"k%d" % i
                store.write(key, b"v", keep_ttl=False)
                store.expire_at(key, now - 1)
            real_sample = store.sample_and_expire
            passes = []

            def sample_then_fail(count):
                passes.append(count)
                if len(passes) > 1:
                    raise MemoryError("injected partway through the sweep")
                return real_sample(count)

            store.sample_and_expire = sample_then_fail
            with pytest.raises(MemoryError):
                server._sweep_expired()
            assert len(passes) > 1, (
                "the sweep stopped before the raising pass, so this run never reached "
                "the case the finally exists for", passes)
            assert store.take_effects() == [], (
                "the sweep left its own DELs on the queue for the next dispatched "
                "command to be blamed for")
        finally:
            server._loop.close()


@pytest.mark.parametrize("flag, keyword", [
    ("--snapshot-interval", "snapshot_interval"),
    ("--expiry-sweep-interval", "expiry_sweep_interval"),
], ids=["snapshot", "sweep"])
def test_an_interval_past_what_can_be_scheduled_is_refused_at_both_doors(flag, keyword):
    # the other end of the rule the negative check holds: a value this large reaches the
    # arithmetic that schedules it -- a clock reading plus the interval, or the interval
    # divided into seconds -- and leaves as an OverflowError traceback, which is the one
    # shape of bad value the CLI answered with a crash rather than a refusal
    too_large = 10 ** 400
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args([flag, str(too_large)])
    with pytest.raises(ValueError, match="cannot be scheduled"):
        Server(0, **{keyword: too_large})


@pytest.mark.parametrize("flag, keyword", [
    ("--snapshot-interval", "snapshot_interval"),
    ("--expiry-sweep-interval", "expiry_sweep_interval"),
], ids=["snapshot", "sweep"])
def test_the_scheduling_ceiling_itself_is_accepted_and_one_past_it_is_refused(flag, keyword):
    # _check_schedulable compares with `>`, never `>=`: the ceiling itself still has to
    # reach the arithmetic that schedules it -- a clock reading plus the interval, or the
    # interval divided into seconds -- rather than being refused alongside the value one
    # past it, which is the one the check exists to keep out of that arithmetic
    args = build_arg_parser().parse_args([flag, str(MAX_SCHEDULABLE_INTERVAL)])
    assert getattr(args, keyword) == MAX_SCHEDULABLE_INTERVAL

    server = Server(0, **{keyword: MAX_SCHEDULABLE_INTERVAL})
    try:
        assert getattr(server, keyword) == MAX_SCHEDULABLE_INTERVAL
    finally:
        server._loop.close()

    one_past = MAX_SCHEDULABLE_INTERVAL + 1
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args([flag, str(one_past)])
    with pytest.raises(ValueError, match="cannot be scheduled"):
        Server(0, **{keyword: one_past})


def test_the_snapshot_arm_counts_its_interval_from_when_the_save_finished(tmp_path):
    # the interval is the gap between saves, not a window a long save eats into, which is
    # what `CONFIG GET save` claims when it spells the schedule in the reference's syntax:
    # there a rule fires once the clock is past its seconds counted from the last
    # completed save. a deadline taken before the save runs charges the save's own
    # duration against the next interval, so a save longer than its interval runs again
    # the moment it returns -- back to back, on the one thread that answers commands,
    # with the reply still advertising a gap between them
    with _injected_clock() as clock:
        server = Server(0, snapshot_path=str(tmp_path / "dump.mrdb"),
                        snapshot_interval=60, expiry_sweep_interval=0)
        try:
            saves = []

            def slow_save():
                saves.append(clock.monotonic())
                # a keyspace big enough to outlast the interval it is saved on
                clock.t += 90.0

            server._save_snapshot = slow_save
            server._next_snapshot_at = clock.monotonic() + 60
            clock.t += 60.0
            server._tick()
            assert len(saves) == 1, ("the first save never fired", saves)
            clock.t += 59.0
            server._tick()
            assert len(saves) == 1, (
                "a second save fired 59 seconds after the first one returned -- the "
                "interval was counted from before the save rather than from its end",
                saves)
            clock.t += 2.0
            server._tick()
            assert len(saves) == 2, (
                "no save fired 61 seconds after the first one returned", saves)
        finally:
            server._loop.close()


def test_the_sweep_arm_counts_its_interval_from_when_the_tick_started_not_from_when_the_sweep_returned():
    # the mirror of the test above, and the opposite rule: the sweep re-arms from the
    # same clock reading _tick() took at its own start, before _sweep_expired ever runs,
    # where the save arm re-arms from a fresh reading taken after the save returns. the
    # sweep is held to its own millisecond budget rather than to _tick()'s cadence, so
    # counting its interval from after a slow pass returned would run it back to back
    # with itself exactly the way a slow save is not allowed to -- moving the sweep's
    # re-arm after _guard_task, to match the save arm's own ordering, passes every other
    # test in this module and fails only here
    with _injected_clock() as clock:
        server = Server(0, snapshot_path=None, snapshot_interval=0,
                        expiry_sweep_interval=100)
        try:
            sweeps = []

            def slow_sweep():
                sweeps.append(clock.monotonic())
                # a pass that runs long relative to its own 100 ms interval
                clock.t += 90.0

            server._sweep_expired = slow_sweep
            server._next_sweep_at = clock.monotonic() + 0.1
            clock.t += 0.1
            tick_started_at = clock.monotonic()
            server._tick()
            assert len(sweeps) == 1, ("the sweep never fired", sweeps)
            assert server._next_sweep_at == pytest.approx(tick_started_at + 0.1), (
                "the sweep's next deadline was not counted from the reading the tick "
                "took before the sweep ran", server._next_sweep_at, tick_started_at)
            # the clock already moved 90 seconds inside the sweep, so the rearmed
            # deadline -- tick_started_at + 0.1 -- is already well behind it; the very
            # next tick has to fire the sweep again rather than waiting a fresh 0.1
            # seconds from here
            server._tick()
            assert len(sweeps) == 2, (
                "the sweep did not fire again although its rearmed deadline was already "
                "in the past -- the rearm must have been taken before the slow pass ran, "
                "not after it returned", sweeps)
        finally:
            server._loop.close()


def test_a_second_run_on_an_already_ran_server_arms_nothing(tmp_path):
    # run()'s own ordering: the if self._ran: check has to run before
    # _arm_periodic_tasks(), not after -- swap the two and a server that already ran and
    # stopped answers CONFIG GET save's schedule again the moment run() is called a
    # second time, even though the second call never gets past the RuntimeError to open
    # a listener, tick anything, or reach the finally that would disarm it again
    server = Server(0, snapshot_path=str(tmp_path / "dump.mrdb"), snapshot_interval=60,
                    expiry_sweep_interval=0)
    try:
        server._ran = True
        with pytest.raises(RuntimeError):
            server.run()
        assert server.armed_snapshot_interval == 0, (
            "a second run() on an already-ran server left a schedule armed that it will "
            "never honour")
    finally:
        server._loop.close()


def test_main_lets_a_non_snapshot_construction_failure_propagate(monkeypatch):
    # main()'s own comment promises this: only persistence.SnapshotError is turned into
    # a one-line message and a clean exit. anything else raised while constructing a
    # Server -- a bug, an unrelated OSError -- is left to crash with its own traceback,
    # which `except Exception` here would swallow just as readily as the one exception
    # this is supposed to catch
    def exploding_server(*args, **kwargs):
        raise RuntimeError("not a SnapshotError")

    monkeypatch.setattr(server_mod, "Server", exploding_server)
    with pytest.raises(RuntimeError, match="not a SnapshotError"):
        server_mod.main(["--port", "0"])


def test_a_periodic_task_raising_keyboardinterrupt_escapes_the_tick():
    # _guard_task's `except Exception` is deliberate, not a stand-in for a broader catch:
    # a KeyboardInterrupt or SystemExit raised inside a periodic task has to leave the
    # tick rather than being logged and swallowed, the same rule _guard already follows
    # for a connection, and what lets an interrupt arriving during a save still end the
    # process. widening the clause to `except BaseException` passes every other test in
    # this module, and this one drives _tick() directly so that mutant fails here on the
    # missing exception rather than on a loop that never ends
    server = Server(0, snapshot_path=None, snapshot_interval=0, expiry_sweep_interval=1)
    try:
        def exploding():
            raise KeyboardInterrupt()

        server._sweep_expired = exploding
        server._arm_periodic_tasks()
        server._next_sweep_at = time.monotonic() - 1
        with pytest.raises(KeyboardInterrupt):
            server._tick()
    finally:
        server._loop.close()


def test_run_unwinds_cleanly_when_a_periodic_task_raises_keyboardinterrupt():
    # the other half of the rule above, at run()'s own level: an interrupt from inside a
    # task takes the same way out as any other raise -- the loop closed, both deadlines
    # disarmed, both signal handlers restored -- rather than leaving a half-stopped
    # server behind. the task raises once and then stops the loop, so a boundary that
    # swallowed the interrupt ends this test on the missing exception instead of hanging
    # the suite on a sweep that raises every interval forever
    before = (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM))
    server = Server(0, snapshot_path=None, snapshot_interval=0, expiry_sweep_interval=1)
    raised = []

    def exploding_once():
        if raised:
            server._running = False
            return
        raised.append(1)
        raise KeyboardInterrupt()

    server._sweep_expired = exploding_once
    with pytest.raises(KeyboardInterrupt):
        server.run()
    assert server._loop._selector.get_map() is None, (
        "the selector must still be closed when the tick's own exception escapes run()")
    assert (server._next_sweep_at, server._next_snapshot_at) == (None, None), (
        "both deadlines must still be disarmed on the way out")
    assert (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)) == before, (
        "the signal handlers must still be restored when run() exits through this path")
