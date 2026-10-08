"""The sliding-window limiter, its two flags and the role exemption.

Pinned at --rate-limit 100 / --rate-limit-window 1 wherever the shape matters, because
that pairing is what makes the flood deterministic: 100 permitted, the 101st refused, and
a boundary case exactly one window later.

The window takes an injected clock reading rather than a real one. A test that slept out a
real second would be the slowest in the suite and would still be asserting the clock
rather than the window.
"""

import contextlib
import types

import pytest

import ratelimit
import resp
import server as server_module
from connection import Role
from server import DEFAULT_RATE_LIMIT, DEFAULT_RATE_LIMIT_WINDOW_SECONDS, Server, build_arg_parser
from tests.test_server_lifecycle import listening, pump

LIMIT = 100
WINDOW = 1
PING = b"*1\r\n$4\r\nPING\r\n"
REFUSED = resp.encode_error(b"ERR rate limit exceeded")


def test_the_limit_permits_exactly_its_budget_and_refuses_the_next():
    window = ratelimit.SlidingWindow(LIMIT, WINDOW)
    # every reading inside one window, so nothing can be popped and the count is the budget
    assert all(window.allow(i / (LIMIT * 10)) for i in range(LIMIT)), "the budget itself was refused"
    assert not window.allow(0.5), "the command past the budget was permitted"


def test_the_window_slides_rather_than_resetting_on_a_boundary():
    # a FIXED window would let 100 land at 0.99 and another 100 at 1.01, which is the burst
    # this shape exists to prevent. so the assertion is not just that a later command is
    # permitted, it is that the permits come back ONE AT A TIME as the old ones age out
    #
    # the probe is 0.5 ms past one window rather than exactly one window, and that margin is
    # deliberate: `2.0 - WINDOW` is 1.0 exactly, but a cutoff computed from a reading that is
    # not a dyadic rational is not -- 1.9 - 1 is 0.8999999999999999, which leaves a reading
    # at 0.9 inside the window and refuses a command that should be permitted. an earlier
    # draft of this test asserted at exactly one window and failed on that, against a
    # limiter that was correct. the boundary itself gets its own test below, on values
    # binary floating point represents exactly
    window = ratelimit.SlidingWindow(LIMIT, WINDOW)
    for i in range(LIMIT):
        assert window.allow(1.0 + i / 1000)
    assert not window.allow(1.0995), "the budget was spent and this was permitted"
    # past the oldest reading by half a millisecond and no further: exactly one has aged out
    assert window.allow(2.0005), "the oldest reading did not age out of the window"
    assert not window.allow(2.0005), "two permits came back where one reading had aged out"


def test_a_reading_exactly_one_window_old_has_left_the_window():
    # the boundary itself, both sides of it. <= and not <, for the reason an expiry
    # deadline equal to now is already expired
    window = ratelimit.SlidingWindow(1, WINDOW)
    assert window.allow(0.0)
    assert not window.allow(0.999999), "a reading inside the window was treated as expired"
    assert window.allow(1.0), "a reading exactly one window old was treated as still inside"


def test_the_readings_are_popped_rather_than_counted():
    # the failure this catches answers every other test here correctly: a window that
    # filtered on read instead of popping would permit and refuse exactly as it should and
    # keep one reading per request for the life of the connection
    window = ratelimit.SlidingWindow(LIMIT, WINDOW)
    for i in range(LIMIT * 5):
        window.allow(float(i))
    assert len(window) <= LIMIT, (
        "the deque grew past the budget, so nothing is being popped and this is a leak "
        "that behaves correctly", len(window))
    assert len(window) == 1, (
        "one reading per whole window should survive this spacing", len(window))


@pytest.mark.parametrize("role", [Role.FOLLOWER, Role.LEADER_LINK],
                         ids=["follower", "leader_link"])
def test_a_connection_that_is_not_a_client_is_never_limited(role):
    # both roles, separately and by name. a check written `is not Role.FOLLOWER` passes the
    # first of these and fails the second, and the second is this process's own outbound
    # connection to its leader -- rate-limiting that would throttle the replication stream
    # with no error anywhere saying so. that is the whole reason the enum has three values
    server = Server(0, rate_limit=1, rate_limit_window=WINDOW)
    try:
        conn = _a_connection(server, role=role)
        for _ in range(LIMIT):
            assert server._rate_limit_permits(conn), "%s was rate-limited" % role
        assert conn.rate_limit_state is None, (
            "an exempt connection was given limiter state, so the exemption is after the "
            "window rather than before it")
    finally:
        server._loop.close()


def test_a_client_is_limited_where_the_other_roles_are_not():
    # the control for the two cases above: without it they would pass against a limiter
    # that never refuses anybody
    server = Server(0, rate_limit=1, rate_limit_window=WINDOW)
    try:
        conn = _a_connection(server, role=Role.CLIENT)
        assert server._rate_limit_permits(conn)
        assert not server._rate_limit_permits(conn), "the client was not limited"
    finally:
        server._loop.close()


def test_the_limiter_is_off_with_no_flag_and_allocates_nothing():
    assert DEFAULT_RATE_LIMIT == 0, "the default must be off; the benchmark depends on it"
    assert build_arg_parser().parse_args([]).rate_limit == 0
    server = Server(0)
    try:
        conn = _a_connection(server, role=Role.CLIENT)
        for _ in range(LIMIT * 3):
            assert server._rate_limit_permits(conn), "a default server refused a command"
        assert conn.rate_limit_state is None, (
            "a server with no limit built limiter state, which every command then pays for")
    finally:
        server._loop.close()


def test_the_window_default_is_one_second_at_both_doors():
    assert DEFAULT_RATE_LIMIT_WINDOW_SECONDS == 1
    assert build_arg_parser().parse_args([]).rate_limit_window == 1
    server = Server(0)
    try:
        assert server.rate_limit_window == 1
    finally:
        server._loop.close()


def test_the_window_refuses_zero_with_its_own_ending_at_both_doors(capsys):
    # every other numeric setting here reads 0 as "off". this one cannot: 100 requests per
    # 0 seconds puts every reading instantly outside the window and permits everything, so
    # an operator who typed it meaning "off" would get a limiter that never fires and says
    # nothing. the refusal has to point at the flag that does turn it off
    with pytest.raises(ValueError) as refusal:
        Server(0, rate_limit_window=0)
    assert "--rate-limit 0" in str(refusal.value), (
        "the refusal does not say which flag disables the limiter", str(refusal.value))
    with pytest.raises(SystemExit) as exited:
        build_arg_parser().parse_args(["--rate-limit-window", "0"])
    assert exited.value.code == 2
    assert "--rate-limit 0" in capsys.readouterr().err

    # and the ending is this flag's own, not the shared one: a test asserting only
    # "cannot be negative" would pass for a flag handed the wrong message
    assert "0 disables the check" not in str(refusal.value)


@pytest.mark.parametrize("flag, value", [("--rate-limit", "-1"), ("--rate-limit-window", "-1")])
def test_a_negative_value_is_refused_at_the_cli(flag, value, capsys):
    with pytest.raises(SystemExit) as exited:
        build_arg_parser().parse_args([flag, value])
    assert exited.value.code == 2
    assert flag.lstrip("-").replace("-", " ") in capsys.readouterr().err


def test_a_window_past_the_scheduling_ceiling_is_refused_at_both_doors():
    past = server_module.MAX_SCHEDULABLE_INTERVAL + 1
    with pytest.raises(ValueError):
        Server(0, rate_limit_window=past)
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args(["--rate-limit-window", str(past)])


def test_n_pipelined_commands_consume_n_units_not_one():
    # the clause the gate names, asserted through a real accept and a real read so that the
    # check's POSITION is what is under test: one recv carries the whole batch, and a check
    # run once per readable event would charge all of it one unit. the budget is small so
    # the whole thing fits in one buffer
    budget = 4
    with listening(rate_limit=budget, rate_limit_window=WINDOW) as (server, connect, _listener):
        sock = connect()
        pump(server)
        sock.sendall(PING * (budget + 2))
        pump(server)
        replies = _read_until_quiet(sock)
    assert replies.count(b"+PONG\r\n") == budget, (
        "the batch did not consume one unit per command", replies[:200])
    assert replies.count(REFUSED) == 2, (
        "the commands past the budget were not refused individually", replies[:200])
    # order matters: the permitted ones come first, then the refusals
    assert replies == b"+PONG\r\n" * budget + REFUSED * 2, replies[:300]


def test_a_refused_command_leaves_the_connection_open_and_serving():
    # refused is not closed. a limiter that closed the connection would be
    # indistinguishable from a protocol error to the client, and the next window would
    # never arrive because the client would have to reconnect -- which also hands it a
    # fresh budget, so closing would defeat the limit it is enforcing
    with listening(rate_limit=1, rate_limit_window=WINDOW) as (server, connect, _listener):
        sock = connect()
        pump(server)
        sock.sendall(PING * 3)
        pump(server)
        assert _read_until_quiet(sock) == b"+PONG\r\n" + REFUSED * 2
        assert len(server._connections) == 1, "the refusal closed the connection"
        assert not next(iter(server._connections)).closed
        # and it is still being read: the window has not moved, so this is refused too,
        # which is the proof the connection is live rather than merely unclosed
        sock.sendall(PING)
        pump(server)
        assert _read_until_quiet(sock) == REFUSED


def test_a_refused_command_is_not_dispatched():
    # the reply says refused; this says the keyspace never saw it, which is the half a
    # reply assertion cannot reach
    with listening(rate_limit=1, rate_limit_window=WINDOW) as (server, connect, _listener):
        sock = connect()
        pump(server)
        sock.sendall(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
                     b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n")
        pump(server)
        assert _read_until_quiet(sock) == b"+OK\r\n" + REFUSED
        assert server._store.lookup(b"a") == b"1", "the permitted write did not land"
        assert server._store.lookup(b"b") is None, (
            "the refused command was dispatched anyway, so the limiter only shapes replies")


def test_a_batch_of_refusals_is_bounded_by_the_write_buffer_limit(make_refusing_connection):
    # the sixth review's finding 35, found independently by two reviewers. a refusal is a reply
    # like any other, and the in-loop size check is what bounds what one recv can queue -- the
    # peak four published sentences call "the limit plus one reply". the refusal path used to
    # `continue` past that check, so a batch of refusals queued without bound: measured, 283,953
    # bytes past a 100-byte limit where the permitted path held to 105, and the in-loop flush
    # fired 9,855 times with the limiter off against once with it on
    server, conn = make_refusing_connection(rate_limit=1, write_buffer_limit=100)
    server._dispatch_batch(conn, [[b"PING"]] * 200)
    assert len(conn.write_buffer) <= 100 + len(REFUSED), (
        "a batch of refusals queued past the limit, so the refusal path is skipping the in-loop "
        "check that bounds one recv", len(conn.write_buffer))
    assert conn.closed, "the limit was exceeded and the connection was not closed"


def test_the_permitted_path_is_still_bounded_the_same_way(make_refusing_connection):
    # the control: without it the test above would pass against a limiter that refused nothing,
    # and against a dispatch loop whose size check had been deleted outright
    server, conn = make_refusing_connection(rate_limit=0, write_buffer_limit=100)
    server._dispatch_batch(conn, [[b"PING"]] * 200)
    assert len(conn.write_buffer) <= 100 + len(b"+PONG\r\n"), len(conn.write_buffer)
    assert conn.closed


@pytest.fixture
def make_refusing_connection():
    # a connection whose send() always refuses, so the queue is observable rather than drained by
    # the kernel. the limit is what closes it, which is the behaviour under test
    import socket as socket_module

    from connection import Connection
    opened = []

    def make(**settings):
        server = Server(0, rate_limit_window=WINDOW, **settings)
        first, second = socket_module.socketpair()
        opened.extend((first, second, server))

        class RefusesEverySend:
            def __init__(self, sock):
                self._sock = sock

            def fileno(self):
                return self._sock.fileno()

            def send(self, data):
                raise BlockingIOError()

            def close(self):
                self._sock.close()

        conn = Connection(RefusesEverySend(first), ("stub", 0))
        server._loop.register(conn)
        server._connections.add(conn)
        return server, conn

    yield make
    for item in opened:
        if isinstance(item, Server):
            item._loop.close()
        else:
            item.close()


class _Clock:
    def __init__(self, start=1_000.0):
        self.t = start

    def monotonic(self):
        return self.t


@contextlib.contextmanager
def _injected_clock(start=1_000.0):
    # the idiom this project already owns, from tests/test_incomplete_command_timeout.py: the
    # server module's own `time` is rebound rather than time.monotonic itself, because patching
    # the real function reaches every module in the process. a local copy for the reason that
    # module gives for its own copy -- the name is private there, so reaching across for it means
    # a rename in that file breaks this one silently
    clock = _Clock(start)
    real = server_module.time
    server_module.time = types.SimpleNamespace(
        monotonic=clock.monotonic, time=real.time, sleep=real.sleep)
    try:
        yield clock
    finally:
        server_module.time = real


def test_the_server_level_window_slides_on_the_injected_clock():
    # the sixth review's finding 38, the first of six mutants that survived every test in this
    # module. `window.allow(time.monotonic())` replaced by `allow(0.0)` passed all 51 tests in the
    # two touched modules: the window slid only in the direct SlidingWindow tests, and nothing
    # proved the LIMITER -- the thing the flag configures -- ever gives a permit back
    server = Server(0, rate_limit=2, rate_limit_window=10)
    try:
        conn = _a_connection(server, role=Role.CLIENT)
        with _injected_clock() as clock:
            assert server._rate_limit_permits(conn)
            assert server._rate_limit_permits(conn)
            assert not server._rate_limit_permits(conn), "the budget was not spent"
            clock.t += 9.0
            assert not server._rate_limit_permits(conn), "a permit came back inside the window"
            clock.t += 1.5
            assert server._rate_limit_permits(conn), (
                "no permit came back a window later, so the server-level window never slides")
    finally:
        server._loop.close()


def test_the_server_reads_the_monotonic_clock_and_not_the_wall_clock():
    # the headline claim of this feature's design -- a duration is measured on time.monotonic(),
    # because a wall clock stepped forward empties the window and permits a flood -- and
    # substituting time.time() for it passed the entire suite, 1,096 tests. the injected namespace
    # advances monotonic alone, so a limiter reading the wall clock sees a frozen clock and never
    # gives a permit back
    server = Server(0, rate_limit=1, rate_limit_window=5)
    try:
        conn = _a_connection(server, role=Role.CLIENT)
        with _injected_clock() as clock:
            assert server._rate_limit_permits(conn)
            assert not server._rate_limit_permits(conn)
            clock.t += 6.0
            assert server._rate_limit_permits(conn), (
                "the window did not slide when only the monotonic reading advanced, so the "
                "limiter is reading a different clock than the one it is specified to read")
    finally:
        server._loop.close()


def test_the_window_the_flag_configures_is_the_window_the_limiter_uses():
    # third surviving mutant: SlidingWindow(self.rate_limit, self.rate_limit_window) replaced by
    # (self.rate_limit, 1) passed everything, because every server-level test used a window of 1,
    # which is also the default. so --rate-limit-window could have been ignored entirely. the
    # wiring test in test_size_caps.py only checks that the attribute arrives on the Server
    server = Server(0, rate_limit=1, rate_limit_window=30)
    try:
        conn = _a_connection(server, role=Role.CLIENT)
        with _injected_clock() as clock:
            assert server._rate_limit_permits(conn)
            clock.t += 2.0
            assert not server._rate_limit_permits(conn), (
                "a permit came back two seconds into a thirty-second window, so the limiter is "
                "not using the window the flag configured")
            assert conn.rate_limit_state.window == 30
    finally:
        server._loop.close()


@pytest.mark.parametrize("kwargs, wanted", [
    ({"rate_limit": -1}, "rate_limit cannot be negative"),
    ({"rate_limit_window": -1}, "rate_limit_window must be positive"),
], ids=["rate_limit", "rate_limit_window"])
def test_a_negative_value_is_refused_at_the_constructor_door_too(kwargs, wanted):
    # fourth surviving mutant: deleting _check_not_negative(rate_limit, ...) from Server.__init__
    # passed every test, because nothing tested a negative at the constructor door -- only at the
    # CLI. the design's criterion says BOTH doors, and Server is built directly by this suite and
    # by anything embedding it, so the constructor is the door that matters more. a rate_limit of
    # -1 would refuse every command, since len(readings) >= -1 is true of an empty deque
    with pytest.raises(ValueError) as refusal:
        Server(0, **kwargs)
    assert wanted in str(refusal.value), str(refusal.value)


def test_a_limit_of_zero_is_accepted_through_the_parser():
    # fifth surviving mutant: --rate-limit's validator swapped for the window's, which refuses 0
    # with "--rate-limit 0 is what disables the limiter" -- the very value it was telling you to
    # use. no test passed 0 through the parser
    assert build_arg_parser().parse_args(["--rate-limit", "0"]).rate_limit == 0


def test_the_readings_are_popped_in_a_loop_and_not_one_at_a_time():
    # sixth surviving mutant: the `while` that pops aged readings reduced to an `if`. every answer
    # stays correct and the deque stops being bounded -- after a burst and then silence, len() is
    # the whole burst instead of what is still inside the window. the existing pop test spaces its
    # traffic evenly, which pops at most one per call and cannot tell the two apart
    window = ratelimit.SlidingWindow(LIMIT, WINDOW)
    for i in range(LIMIT):
        window.allow(1.0 + i / (LIMIT * 10))
    assert len(window) == LIMIT
    window.allow(100.0)
    assert len(window) == 1, (
        "a burst followed by silence left more than the one reading inside the window, so the "
        "aged readings are being popped one per call rather than in a loop", len(window))


def _a_connection(server, role):
    import socket as socket_module

    from connection import Connection
    first, second = socket_module.socketpair()
    conn = Connection(first, ("stub", 0))
    conn.role = role
    server._connections.add(conn)
    _OPENED.extend((first, second))
    return conn


_OPENED = []


@pytest.fixture(autouse=True)
def _close_the_stub_sockets():
    yield
    while _OPENED:
        _OPENED.pop().close()


def _read_until_quiet(sock, rounds=4):
    # bounded by rounds that achieve nothing rather than by a clock: a reply this misses is
    # a failing assertion below, not a hang
    sock.settimeout(0.2)
    out = bytearray()
    for _ in range(rounds):
        try:
            chunk = sock.recv(65536)
        except TimeoutError:
            break
        if not chunk:
            break
        out.extend(chunk)
    return bytes(out)
