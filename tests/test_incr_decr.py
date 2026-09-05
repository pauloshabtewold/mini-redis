"""INCR, DECR: 64-bit signed arithmetic on a string value, with the TTL preserved."""

import pytest

import commands
from tests.conftest import FROZEN, FrozenStore, r


@pytest.fixture
def store():
    return FrozenStore()


# --- normal cases -----------------------------------------------------------------------


def test_incr_on_a_missing_key_starts_at_zero(store):
    assert r(store, b"INCR", b"fresh") == (b":1\r\n", [[b"INCR", b"fresh"]])


def test_decr_on_a_missing_key_starts_at_zero(store):
    assert r(store, b"DECR", b"fresh") == (b":-1\r\n", [[b"DECR", b"fresh"]])


def test_incr_and_decr_on_an_existing_value(store):
    store.write(b"k", b"10", keep_ttl=False)
    assert r(store, b"INCR", b"k") == (b":11\r\n", [[b"INCR", b"k"]])
    assert r(store, b"DECR", b"k") == (b":10\r\n", [[b"DECR", b"k"]])


def test_the_effect_is_the_command_verbatim_not_a_computed_set(store):
    store.write(b"k", b"10", keep_ttl=False)
    _, effects = r(store, b"INCR", b"k")
    assert effects == [[b"INCR", b"k"]]


# --- edge cases --------------------------------------------------------------------------


def test_incr_at_int64_max_minus_one_still_succeeds(store):
    # the value one step inside the bound -- what separates a correct bound from one set
    # an integer too low
    store.write(b"max", b"9223372036854775806", keep_ttl=False)
    assert r(store, b"INCR", b"max") == (
        b":9223372036854775807\r\n", [[b"INCR", b"max"]])


def test_incr_at_int64_max_overflows(store):
    store.write(b"max", b"9223372036854775807", keep_ttl=False)
    assert r(store, b"INCR", b"max") == (
        b"-ERR increment or decrement would overflow\r\n", [])


def test_decr_at_int64_min_plus_one_still_succeeds(store):
    store.write(b"min", b"-9223372036854775807", keep_ttl=False)
    assert r(store, b"DECR", b"min") == (
        b":-9223372036854775808\r\n", [[b"DECR", b"min"]])


def test_decr_at_int64_min_overflows(store):
    store.write(b"min", b"-9223372036854775808", keep_ttl=False)
    assert r(store, b"DECR", b"min") == (
        b"-ERR increment or decrement would overflow\r\n", [])


def test_zero_is_the_one_legal_leading_zero(store):
    store.write(b"zero", b"0", keep_ttl=False)
    assert r(store, b"INCR", b"zero") == (b":1\r\n", [[b"INCR", b"zero"]])


def test_incr_preserves_an_existing_ttl(store):
    # the assertion that catches INCR built on SET's write path, which clears a TTL
    # (measured: SET tk 5 EX 100 then INCR tk leaves TTL 100 on 7.2.7)
    store.write(b"tk", b"5", keep_ttl=False)
    store.expire_at(b"tk", FROZEN + 100_000)
    assert r(store, b"INCR", b"tk") == (b":6\r\n", [[b"INCR", b"tk"]])
    assert store.deadline(b"tk") == FROZEN + 100_000


def test_incr_on_a_lazily_expired_key_queues_the_del_before_its_own_effect(store):
    store.write(b"c", b"5", keep_ttl=False)
    store.expire_at(b"c", FROZEN - 1)
    assert r(store, b"INCR", b"c") == (b":1\r\n", [[b"INCR", b"c"]])
    # the queued DEL is drained before this command's own effects: a follower applying
    # INCR before DEL ends with the key absent while the leader holds 1
    assert store.take_effects() == [[b"DEL", b"c"]]


def test_invariants_hold_after_incr_and_decr(store):
    store.write(b"k", b"5", keep_ttl=False)
    r(store, b"INCR", b"k")
    r(store, b"DECR", b"k")
    store.check_invariants()


# --- error cases -------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    [b"abc", b"011", b" 5", b"5 ", b"+5", b"1_0", b"-0", b"", b"5.0",
     b"99999999999999999999"],
)
def test_incr_rejects_every_stored_value_the_protocol_does_not_accept(store, bad):
    store.write(b"bad", bad, keep_ttl=False)
    assert r(store, b"INCR", b"bad") == (
        b"-ERR value is not an integer or out of range\r\n", [])


def test_incr_wrong_arity(store):
    store.write(b"c", b"5", keep_ttl=False)
    assert r(store, b"INCR", b"c", b"a") == (
        b"-ERR wrong number of arguments for 'incr' command\r\n", [])


def test_incr_raises_wrongtype_through_dispatch(store):
    r(store, b"RPUSH", b"w", b"v")
    assert r(store, b"INCR", b"w") == (
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", [])


def test_decr_raises_wrongtype_through_dispatch(store):
    r(store, b"RPUSH", b"w", b"v")
    assert r(store, b"DECR", b"w") == (
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", [])
