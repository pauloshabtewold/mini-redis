"""EXPIRE, PEXPIRE, PEXPIREAT, TTL, PTTL: absolute deadlines and lazy expiry.

A key whose deadline has passed is gone on access before any sweep exists to run, and
the deadline it was given is an absolute timestamp rather than an offset.
"""

import pytest

import commands
import resp
from store import Store
from commands.registry import INT64_MAX
from tests.conftest import FROZEN, FrozenStore, r
from tests.int_ceiling import NO_CEILING_REASON, NO_CONVERSION_CEILING, OVERSIZED_DIGIT_RUN


@pytest.fixture
def store():
    return FrozenStore()


# --- normal cases -----------------------------------------------------------------------


def test_expire_sets_a_ttl_in_seconds(store):
    store.write(b"k", b"v", keep_ttl=False)
    assert r(store, b"EXPIRE", b"k", b"100") == (
        b":1\r\n", [[b"PEXPIREAT", b"k", b"%d" % (FROZEN + 100_000)]])
    assert store.deadline(b"k") == FROZEN + 100_000


def test_pexpire_sets_a_ttl_in_milliseconds(store):
    store.write(b"k", b"v", keep_ttl=False)
    assert r(store, b"PEXPIRE", b"k", b"50000") == (
        b":1\r\n", [[b"PEXPIREAT", b"k", b"%d" % (FROZEN + 50_000)]])
    assert store.deadline(b"k") == FROZEN + 50_000


def test_pexpireat_propagates_the_same_absolute_value_it_was_given(store):
    store.write(b"k", b"v", keep_ttl=False)
    deadline = FROZEN + 7
    assert r(store, b"PEXPIREAT", b"k", b"%d" % deadline) == (
        b":1\r\n", [[b"PEXPIREAT", b"k", b"%d" % deadline]])
    assert store.deadline(b"k") == deadline


def test_expire_on_a_missing_key_is_zero_with_no_effects(store):
    assert r(store, b"EXPIRE", b"nosuchkey", b"10") == (b":0\r\n", [])


def test_ttl_and_pttl_on_a_live_key(store):
    store.write(b"n", b"v", keep_ttl=False)
    store.expire_at(b"n", FROZEN + 10_000)
    assert r(store, b"TTL", b"n") == (b":10\r\n", [])
    assert r(store, b"PTTL", b"n") == (b":10000\r\n", [])


def test_ttl_and_pttl_on_a_key_with_no_deadline(store):
    store.write(b"n", b"v", keep_ttl=False)
    assert r(store, b"TTL", b"n") == (b":-1\r\n", [])
    assert r(store, b"PTTL", b"n") == (b":-1\r\n", [])


def test_ttl_and_pttl_on_a_missing_key(store):
    assert r(store, b"TTL", b"nosuchkey") == (b":-2\r\n", [])
    assert r(store, b"PTTL", b"nosuchkey") == (b":-2\r\n", [])


@pytest.mark.parametrize(
    "remaining, expected",
    [(2500, 3), (2499, 2), (1600, 2), (1500, 2), (1499, 1), (500, 1), (499, 0),
     (400, 0), (1, 0)],
)
def test_ttl_rounds_to_the_nearest_second(store, remaining, expected):
    # a live SET ... PX then TTL cannot pin this: the two calls land in the same
    # millisecond or not, so the formula is pinned on exact remaining values instead
    store.write(b"t", b"v", keep_ttl=False)
    store.expire_at(b"t", FROZEN + remaining)
    assert r(store, b"TTL", b"t") == (resp.encode_integer(expected), [])
    assert r(store, b"PTTL", b"t") == (resp.encode_integer(remaining), [])


# --- edge cases --------------------------------------------------------------------------


@pytest.mark.parametrize("command, argument", [(b"EXPIRE", b"-1"), (b"EXPIRE", b"0")])
def test_expire_with_a_non_positive_amount_deletes_immediately(store, command, argument):
    store.write(b"p", b"v", keep_ttl=False)
    assert r(store, command, b"p", argument) == (b":1\r\n", [[b"DEL", b"p"]])
    assert b"p" not in store._data
    assert store.take_effects() == []


def test_pexpireat_at_or_before_now_deletes_immediately(store):
    store.write(b"p", b"v", keep_ttl=False)
    assert r(store, b"PEXPIREAT", b"p", b"1") == (b":1\r\n", [[b"DEL", b"p"]])
    assert b"p" not in store._data
    assert store.take_effects() == []


def test_a_deadline_exactly_equal_to_now_deletes(store):
    store.write(b"p", b"v", keep_ttl=False)
    assert r(store, b"PEXPIREAT", b"p", b"%d" % FROZEN) == (b":1\r\n", [[b"DEL", b"p"]])


def test_expire_on_a_logically_expired_key_answers_zero(store):
    store.write(b"e", b"v", keep_ttl=False)
    store.expire_at(b"e", FROZEN - 1)
    assert r(store, b"EXPIRE", b"e", b"10") == (b":0\r\n", [])
    assert store.take_effects() == [[b"DEL", b"e"]]


def test_ttl_on_a_logically_expired_key_is_absent_not_negative(store):
    store.write(b"n", b"v", keep_ttl=False)
    store.expire_at(b"n", FROZEN - 1)
    assert r(store, b"TTL", b"n") == (b":-2\r\n", [])
    assert store.take_effects() == [[b"DEL", b"n"]]


def test_expire_and_pexpireat_are_type_agnostic():
    # measured: EXPIRE, TTL, PTTL and PEXPIREAT all operate on a list key on 7.2.7
    class ListKinded(Store):
        def now_ms(self):
            return FROZEN

        def kind_of(self, value):
            return b"list"

    s = ListKinded()
    s.write(b"l", b"v", keep_ttl=False)
    assert r(s, b"EXPIRE", b"l", b"10") == (
        b":1\r\n", [[b"PEXPIREAT", b"l", b"%d" % (FROZEN + 10_000)]])
    assert r(s, b"TTL", b"l") == (b":10\r\n", [])
    assert r(s, b"PTTL", b"l") == (b":10000\r\n", [])
    assert r(s, b"PEXPIREAT", b"l", b"%d" % (FROZEN + 20_000)) == (
        b":1\r\n", [[b"PEXPIREAT", b"l", b"%d" % (FROZEN + 20_000)]])


def test_expire_accepts_the_value_one_step_inside_the_overflow_boundary(store):
    # the accepting side of the same boundary: an off-by-a-multiplier guard rejects this
    # and passes every rejecting case, so only the accept side catches it -- the same
    # shape of case, applied to the expiry family's own arithmetic
    store.write(b"k", b"v", keep_ttl=False)
    assert r(store, b"EXPIRE", b"k", b"999999999999999") == (
        b":1\r\n", [[b"PEXPIREAT", b"k", b"%d" % (FROZEN + 999999999999999 * 1000)]])


def test_pexpireat_accepts_int64_max_as_a_legal_deadline(store):
    store.write(b"k", b"v", keep_ttl=False)
    assert r(store, b"PEXPIREAT", b"k", b"9223372036854775807") == (
        b":1\r\n", [[b"PEXPIREAT", b"k", b"9223372036854775807"]])
    assert store.deadline(b"k") == 9223372036854775807


def test_invariants_hold_after_a_mix_of_expiry_operations(store):
    store.write(b"a", b"v", keep_ttl=False)
    r(store, b"EXPIRE", b"a", b"10")
    store.write(b"b", b"v", keep_ttl=False)
    r(store, b"EXPIRE", b"b", b"-1")
    store.check_invariants()


# --- error cases -------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [b"abc", b"011", b"-0", b"+5", b"1_0", b""])
def test_expire_and_pexpireat_reject_every_bad_integer(store, bad):
    store.write(b"k", b"v", keep_ttl=False)
    assert r(store, b"EXPIRE", b"k", bad) == (
        b"-ERR value is not an integer or out of range\r\n", [])
    assert r(store, b"PEXPIREAT", b"k", bad) == (
        b"-ERR value is not an integer or out of range\r\n", [])


def test_expire_rejects_a_seconds_value_that_would_overflow_the_millisecond_conversion(store):
    store.write(b"k", b"v", keep_ttl=False)
    assert r(store, b"EXPIRE", b"k", b"9223372036854775807") == (
        b"-ERR invalid expire time in 'expire' command\r\n", [])


def test_pexpire_rejects_a_deadline_past_int64_max(store):
    store.write(b"k", b"v", keep_ttl=False)
    assert r(store, b"PEXPIRE", b"k", b"9223372036854775807") == (
        b"-ERR invalid expire time in 'pexpire' command\r\n", [])


@pytest.mark.parametrize(
    "argv, expected",
    [
        ([b"EXPIRE", b"k", b"10", b"NX"],
         b"-ERR wrong number of arguments for 'expire' command\r\n"),
        ([b"PEXPIRE", b"k", b"100", b"NX"],
         b"-ERR wrong number of arguments for 'pexpire' command\r\n"),
        ([b"PEXPIREAT", b"k", b"%d" % (FROZEN + 100), b"NX"],
         b"-ERR wrong number of arguments for 'pexpireat' command\r\n"),
    ],
)
def test_expire_family_arity_is_exact_not_a_minimum(store, argv, expected):
    # real Redis 7 takes NX/XX/GT/LT here; this server does not, so arity 3 rather than
    # -3 rejects the trailing option instead of silently ignoring it
    store.write(b"k", b"v", keep_ttl=False)
    assert commands.dispatch(store, None, argv) == (expected, [])


@pytest.mark.parametrize(
    "seconds, expected",
    [
        # the reference divides its own bound with C semantics, which truncate toward
        # zero; Python's // floors, and the two differ on exactly this one value. a
        # floored bound accepts it, resolves a deadline far in the past, and deletes
        # the key -- where the reference answers an error and the key survives
        (b"-9223372036854776", b"-ERR invalid expire time in 'expire' command\r\n"),
        # one step inside it is still accepted, so the bound rejects the boundary
        # rather than everything near it. measured against redis-server 7.2.7, which
        # answers :1 here and the error above
        (b"-9223372036854775", b":1\r\n"),
        (b"-9223372036854774", b":1\r\n"),
        # the positive side never reached this bound: every value large enough to
        # trouble it is already caught by the millisecond conversion below, which is
        # why only the negative half of the range moved
        (b"9223372036854775", b"-ERR invalid expire time in 'expire' command\r\n"),
        (b"9223372036854776", b"-ERR invalid expire time in 'expire' command\r\n"),
    ],
    ids=["below-min", "at-min", "inside-min", "at-max", "above-max"],
)
def test_expire_bounds_its_seconds_argument_the_way_the_reference_does(store, seconds, expected):
    store.write(b"k", b"v", keep_ttl=False)
    response, _effects = r(store, b"EXPIRE", b"k", seconds)
    assert response == expected


def test_expire_at_the_negative_bound_leaves_the_key_alone(store):
    # the consequence the bound above exists to prevent, stated as the keyspace fact:
    # a refused EXPIRE must not have deleted anything on its way to refusing
    store.write(b"k", b"v", keep_ttl=False)
    r(store, b"EXPIRE", b"k", b"-9223372036854776")
    assert store.lookup(b"k") == b"v"


@pytest.mark.skipif(NO_CONVERSION_CEILING, reason=NO_CEILING_REASON)
@pytest.mark.parametrize("argv0", [b"EXPIRE", b"PEXPIRE", b"PEXPIREAT"])
def test_a_digit_run_past_the_conversion_ceiling_is_refused_not_raised(store, argv0):
    # parse_int64's length guard is what keeps this off int(), which raises above the
    # interpreter's conversion ceiling -- an escaping ValueError closes the client's
    # connection and logs a traceback where the protocol asks for one error reply
    store.write(b"k", b"v", keep_ttl=False)
    assert r(store, argv0, b"k", OVERSIZED_DIGIT_RUN) == (
        b"-ERR value is not an integer or out of range\r\n", [])


def test_a_remaining_time_is_never_reported_as_one_of_the_two_negative_facts():
    # _remaining_ms reads the clock twice -- once inside lookup(), once for the
    # subtraction -- and a deadline that falls between the two reads makes the
    # difference negative. -1 and -2 are not small numbers here, they are "this key has
    # no deadline" and "this key does not exist", and a key that has both would be
    # answering the client the opposite of the truth. the reference clamps for this
    # reason; this pins the clamp with a clock that always crosses the deadline
    class ClockCrossesTheDeadline(Store):
        def __init__(self):
            super().__init__()
            self._reads = 0

        def now_ms(self):
            # the first read is what lookup() sees, and it must be before the deadline
            # or the key is expired and gone; every read after it is well past
            self._reads += 1
            return FROZEN if self._reads == 1 else FROZEN + 10_000

    store = ClockCrossesTheDeadline()
    store.write(b"k", b"v", keep_ttl=False)
    store.expire_at(b"k", FROZEN + 1)
    assert r(store, b"PTTL", b"k") == (b":0\r\n", [])

    store = ClockCrossesTheDeadline()
    store.write(b"k", b"v", keep_ttl=False)
    store.expire_at(b"k", FROZEN + 1)
    assert r(store, b"TTL", b"k") == (b":0\r\n", [])


@pytest.mark.parametrize("argv0, unit, error", [
    (b"EXPIRE", 1000, b"-ERR invalid expire time in 'expire' command\r\n"),
    (b"PEXPIRE", 1, b"-ERR invalid expire time in 'pexpire' command\r\n"),
])
def test_the_millisecond_conversion_bound_is_exact_at_int64_max(argv0, unit, error):
    # the existing coverage lands grossly past INT64_MAX, so `> INT64_MAX` and
    # `>= INT64_MAX` look identical to it. a deadline landing exactly on INT64_MAX is
    # legal -- PEXPIREAT accepts one -- and the check must not refuse it.
    #
    # the clock is chosen rather than frozen at the shared value, because EXPIRE multiplies
    # by 1000 and INT64_MAX - FROZEN is not a multiple of 1000: no whole number of seconds
    # lands on the boundary from there, so the comparison is simply unreachable and a test
    # written against that clock cannot tell > from >= however it is phrased
    amount = 4_000_000
    now = INT64_MAX - amount * unit

    class ClockOnTheBoundary(Store):
        def now_ms(self):
            return now

    store = ClockOnTheBoundary()
    store.write(b"k", b"v", keep_ttl=False)
    assert r(store, argv0, b"k", b"%d" % amount) == (
        b":1\r\n", [[b"PEXPIREAT", b"k", b"%d" % INT64_MAX]])

    store = ClockOnTheBoundary()
    store.write(b"k", b"v", keep_ttl=False)
    assert r(store, argv0, b"k", b"%d" % (amount + 1))[0] == error
