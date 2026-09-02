"""SET, GET, DEL, EXISTS, TYPE: the option grammar, WRONGTYPE, and lazy expiry."""

import socket

import pytest

import commands
from commands.registry import parse_int64
from connection import Connection
from store import Store
from tests.conftest import FROZEN, FrozenStore, ListKinded


@pytest.fixture
def conn():
    a, b = socket.socketpair()
    a.setblocking(False)
    connection = Connection(a, ("127.0.0.1", 0))
    yield connection
    a.close()
    b.close()


@pytest.fixture
def store():
    return Store()


# --- normal cases -----------------------------------------------------------------------


def test_set_then_get_round_trips(store, conn):
    assert commands.dispatch(store, conn, [b"SET", b"k", b"v"]) == (
        b"+OK\r\n", [[b"SET", b"k", b"v"]])
    assert commands.dispatch(store, conn, [b"GET", b"k"]) == (b"$1\r\nv\r\n", [])


def test_get_round_trips_a_binary_value_containing_crlf(store, conn):
    value = b"a\r\nb"
    commands.dispatch(store, conn, [b"SET", b"k", value])
    assert commands.dispatch(store, conn, [b"GET", b"k"]) == (b"$4\r\na\r\nb\r\n", [])


def test_get_on_a_missing_key_is_a_null_bulk_string_with_no_effects(store, conn):
    assert commands.dispatch(store, conn, [b"GET", b"nosuchkey"]) == (b"$-1\r\n", [])


def test_the_empty_key_and_the_empty_value_round_trip(store, conn):
    assert commands.dispatch(store, conn, [b"SET", b"", b""]) == (
        b"+OK\r\n", [[b"SET", b"", b""]])
    assert commands.dispatch(store, conn, [b"GET", b""]) == (b"$0\r\n\r\n", [])


def test_type_on_a_string_and_on_a_missing_key(store, conn):
    commands.dispatch(store, conn, [b"SET", b"k", b"v"])
    assert commands.dispatch(store, conn, [b"TYPE", b"k"]) == (b"+string\r\n", [])
    assert commands.dispatch(store, conn, [b"TYPE", b"nosuchkey"]) == (b"+none\r\n", [])


def test_exists_over_present_and_absent_keys(store, conn):
    commands.dispatch(store, conn, [b"SET", b"k", b"v"])
    assert commands.dispatch(store, conn, [b"EXISTS", b"k"]) == (b":1\r\n", [])
    assert commands.dispatch(store, conn, [b"EXISTS", b"nosuchkey"]) == (b":0\r\n", [])


def test_exists_counts_duplicates(store, conn):
    commands.dispatch(store, conn, [b"SET", b"k", b"v"])
    assert commands.dispatch(store, conn, [b"EXISTS", b"k", b"k"]) == (b":2\r\n", [])


def test_del_over_present_and_absent_keys(store, conn):
    commands.dispatch(store, conn, [b"SET", b"k", b"v"])
    assert commands.dispatch(store, conn, [b"DEL", b"k"]) == (b":1\r\n", [[b"DEL", b"k"]])
    assert commands.dispatch(store, conn, [b"DEL", b"nosuchkey"]) == (b":0\r\n", [])


def test_set_ex_alone_sets_a_ttl_in_seconds():
    s = FrozenStore()
    response, effects = commands.dispatch(s, None, [b"SET", b"k", b"v", b"EX", b"10"])
    assert response == b"+OK\r\n"
    assert effects == [[b"SET", b"k", b"v"], [b"PEXPIREAT", b"k", b"%d" % (FROZEN + 10_000)]]
    assert s.deadline(b"k") == FROZEN + 10_000


def test_set_px_alone_sets_a_ttl_in_milliseconds():
    s = FrozenStore()
    response, effects = commands.dispatch(s, None, [b"SET", b"k", b"v", b"PX", b"10000"])
    assert response == b"+OK\r\n"
    assert effects == [[b"SET", b"k", b"v"], [b"PEXPIREAT", b"k", b"%d" % (FROZEN + 10_000)]]
    assert s.deadline(b"k") == FROZEN + 10_000


def test_set_nx_on_a_fresh_key_succeeds_and_writes(store, conn):
    assert commands.dispatch(store, conn, [b"SET", b"fresh", b"v", b"NX"]) == (
        b"+OK\r\n", [[b"SET", b"fresh", b"v"]])


def test_set_nx_on_an_existing_key_fails_without_writing(store, conn):
    commands.dispatch(store, conn, [b"SET", b"k", b"v"])
    assert commands.dispatch(store, conn, [b"SET", b"k", b"v2", b"NX"]) == (b"$-1\r\n", [])
    assert commands.dispatch(store, conn, [b"GET", b"k"]) == (b"$1\r\nv\r\n", [])


def test_set_xx_on_an_existing_key_succeeds(store, conn):
    commands.dispatch(store, conn, [b"SET", b"k", b"v"])
    assert commands.dispatch(store, conn, [b"SET", b"k", b"v2", b"XX"]) == (
        b"+OK\r\n", [[b"SET", b"k", b"v2"]])


def test_set_xx_on_a_missing_key_fails_without_writing(store, conn):
    assert commands.dispatch(store, conn, [b"SET", b"nosuchkey", b"v", b"XX"]) == (
        b"$-1\r\n", [])
    assert commands.dispatch(store, conn, [b"EXISTS", b"nosuchkey"]) == (b":0\r\n", [])


def test_set_options_are_case_insensitive(store, conn):
    # redis-cli forwards whatever case the user typed
    assert commands.dispatch(store, conn, [b"SET", b"ci", b"v", b"ex", b"10"])[0] == b"+OK\r\n"
    assert commands.dispatch(store, conn, [b"SET", b"ci", b"v", b"nx"]) == (b"$-1\r\n", [])


# --- edge cases --------------------------------------------------------------------------


def test_plain_set_discards_an_existing_ttl():
    s = FrozenStore()
    commands.dispatch(s, None, [b"SET", b"k", b"v", b"EX", b"100"])
    assert s.deadline(b"k") == FROZEN + 100_000
    commands.dispatch(s, None, [b"SET", b"k", b"v2"])
    assert s.deadline(b"k") is None


def test_set_effect_drops_the_nx_condition():
    # the leader already evaluated NX; a follower re-evaluating it would need its own
    # notion of existence to match, which the follower's expiry rule deliberately does not
    s = FrozenStore()
    _, effects = commands.dispatch(s, None, [b"SET", b"n", b"v", b"NX", b"EX", b"10"])
    assert effects[0] == [b"SET", b"n", b"v"]
    assert b"NX" not in effects[0]


def test_set_repeated_option_is_ordinary_and_the_last_value_wins():
    # the reply is +OK under both first-value-wins and last-value-wins readings; only the
    # resulting deadline tells them apart
    s = FrozenStore()
    response, effects = commands.dispatch(
        s, None, [b"SET", b"k", b"v", b"EX", b"10", b"EX", b"20"])
    assert response == b"+OK\r\n"
    assert effects == [[b"SET", b"k", b"v"], [b"PEXPIREAT", b"k", b"%d" % (FROZEN + 20_000)]]
    assert s.deadline(b"k") == FROZEN + 20_000


def test_set_xx_xx_behaves_as_one_xx():
    s = FrozenStore()
    s.write(b"k", b"old", keep_ttl=False)
    assert commands.dispatch(s, None, [b"SET", b"k", b"v", b"XX", b"XX"]) == (
        b"+OK\r\n", [[b"SET", b"k", b"v"]])


def test_del_counts_only_what_it_removed(store, conn):
    commands.dispatch(store, conn, [b"SET", b"k", b"v"])
    assert commands.dispatch(store, conn, [b"DEL", b"k", b"nosuchkey"]) == (
        b":1\r\n", [[b"DEL", b"k"]])


def test_del_over_two_present_keys_is_one_argv_naming_both_in_order(store, conn):
    # every other DEL case here removes at most one key, and cannot tell a single argv
    # naming both apart from one DEL effect per key
    commands.dispatch(store, conn, [b"SET", b"d1", b"v"])
    commands.dispatch(store, conn, [b"SET", b"d2", b"v"])
    assert commands.dispatch(store, conn, [b"DEL", b"d1", b"d2"]) == (
        b":2\r\n", [[b"DEL", b"d1", b"d2"]])


def test_lazy_expiry_through_get_queues_the_del_not_the_handler():
    s = FrozenStore()
    s.write(b"g", b"v", keep_ttl=False)
    s.expire_at(b"g", FROZEN - 1)
    assert commands.dispatch(s, None, [b"GET", b"g"]) == (b"$-1\r\n", [])
    assert s.take_effects() == [[b"DEL", b"g"]]
    assert b"g" not in s._data


def test_a_logically_expired_key_does_not_block_set_nx():
    s = FrozenStore()
    s.write(b"n", b"old", keep_ttl=False)
    s.expire_at(b"n", FROZEN - 1)
    assert commands.dispatch(s, None, [b"SET", b"n", b"new", b"NX"]) == (
        b"+OK\r\n", [[b"SET", b"n", b"new"]])
    assert s.take_effects() == [[b"DEL", b"n"]]


def test_set_nx_ex_against_a_lazily_expired_key_chains_three_commands():
    # the one input that produces a three-command chain across both channels: [DEL k] on
    # the store's own queue, then [SET k v] and [PEXPIREAT k <abs>] from the handler
    s = FrozenStore()
    s.write(b"x", b"old", keep_ttl=False)
    s.expire_at(b"x", FROZEN - 1)
    assert commands.dispatch(s, None, [b"SET", b"x", b"new", b"NX", b"EX", b"10"]) == (
        b"+OK\r\n", [[b"SET", b"x", b"new"], [b"PEXPIREAT", b"x", b"%d" % (FROZEN + 10_000)]])
    assert s.take_effects() == [[b"DEL", b"x"]]
    assert s.deadline(b"x") == FROZEN + 10_000


def test_lazy_expiry_through_exists_and_type_carries_nothing():
    s = FrozenStore()
    s.write(b"e", b"v", keep_ttl=False)
    s.expire_at(b"e", FROZEN - 1)
    assert commands.dispatch(s, None, [b"EXISTS", b"e"]) == (b":0\r\n", [])
    s.write(b"t", b"v", keep_ttl=False)
    s.expire_at(b"t", FROZEN - 1)
    assert commands.dispatch(s, None, [b"TYPE", b"t"]) == (b"+none\r\n", [])
    assert s.take_effects() == [[b"DEL", b"e"], [b"DEL", b"t"]]
    s.check_invariants()


# --- error cases -------------------------------------------------------------------------


@pytest.mark.parametrize(
    "argv, expected",
    [
        ([b"SET", b"k", b"v", b"NX", b"XX"], b"-ERR syntax error\r\n"),
        ([b"SET", b"k", b"v", b"EX", b"10", b"XX", b"NX"], b"-ERR syntax error\r\n"),
        ([b"SET", b"k", b"v", b"BOGUS"], b"-ERR syntax error\r\n"),
        ([b"SET", b"k", b"v", b"EX"], b"-ERR syntax error\r\n"),
        ([b"SET", b"k", b"v", b"EX", b"10", b"PX", b"20000", b"EX", b"30"],
         b"-ERR syntax error\r\n"),
        ([b"SET", b"k", b"v", b"EX", b"abc"],
         b"-ERR value is not an integer or out of range\r\n"),
        ([b"SET", b"k", b"v", b"EX", b"011"],
         b"-ERR value is not an integer or out of range\r\n"),
        ([b"SET", b"k", b"v", b"EX", b"+5"],
         b"-ERR value is not an integer or out of range\r\n"),
        ([b"SET", b"k", b"v", b"EX", b"-0"],
         b"-ERR value is not an integer or out of range\r\n"),
        ([b"SET", b"k", b"v", b"EX", b" 5"],
         b"-ERR value is not an integer or out of range\r\n"),
        ([b"SET", b"k", b"v", b"EX", b"5 "],
         b"-ERR value is not an integer or out of range\r\n"),
        ([b"SET", b"k", b"v", b"EX", b"1_0"],
         b"-ERR value is not an integer or out of range\r\n"),
        ([b"SET", b"k", b"v", b"EX", b""],
         b"-ERR value is not an integer or out of range\r\n"),
        ([b"SET", b"k", b"v", b"EX", b"0"],
         b"-ERR invalid expire time in 'set' command\r\n"),
        ([b"SET", b"k", b"v", b"EX", b"-1"],
         b"-ERR invalid expire time in 'set' command\r\n"),
        ([b"SET", b"k", b"v", b"EX", b"9999999999999999"],
         b"-ERR invalid expire time in 'set' command\r\n"),
        ([b"SET", b"k", b"v", b"PX", b"9223372036854775807"],
         b"-ERR invalid expire time in 'set' command\r\n"),
        ([b"SET", b"k"], b"-ERR wrong number of arguments for 'set' command\r\n"),
        ([b"GET", b"k", b"a"], b"-ERR wrong number of arguments for 'get' command\r\n"),
        ([b"EXISTS"], b"-ERR wrong number of arguments for 'exists' command\r\n"),
    ],
)
def test_set_option_grammar_and_arity_errors(store, conn, argv, expected):
    response, effects = commands.dispatch(store, conn, argv)
    assert response == expected
    assert effects == []


def test_get_raises_wrongtype_through_dispatch():
    s = ListKinded()
    s.write(b"w", b"v", keep_ttl=False)
    assert commands.dispatch(s, None, [b"GET", b"w"]) == (
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", [])


def test_set_del_exists_type_stay_type_agnostic_against_the_same_store():
    # the three assertions after SET are the ones that fail against a WRONGTYPE check
    # added to SET, EXISTS or DEL "for consistency" -- a behaviour change list support
    # would have to undo
    s = ListKinded()
    s.write(b"w", b"v", keep_ttl=False)
    assert commands.dispatch(s, None, [b"SET", b"w", b"v2"])[0] == b"+OK\r\n"
    assert commands.dispatch(s, None, [b"EXISTS", b"w"]) == (b":1\r\n", [])
    assert commands.dispatch(s, None, [b"TYPE", b"w"]) == (b"+list\r\n", [])
    assert commands.dispatch(s, None, [b"DEL", b"w"]) == (b":1\r\n", [[b"DEL", b"w"]])


def test_parse_int64_grammar():
    accept = {
        b"0": 0, b"1": 1, b"-1": -1, b"-12": -12,
        b"9223372036854775807": 2**63 - 1, b"-9223372036854775808": -(2**63),
    }
    reject = [
        b"", b"-", b"-0", b"011", b"+5", b" 5", b"5 ", b"1_0", b"0x10", b"5.0",
        b"9223372036854775808", b"-9223372036854775809", b"9" * 30,
    ]
    for field, value in accept.items():
        assert parse_int64(field) == value, field
    for field in reject:
        assert parse_int64(field) is None, field


@pytest.mark.parametrize(
    "options, expected",
    [
        # a repeat overrides, so only the surviving occurrence is validated -- measured
        # against redis-server 7.2.7, which answers +OK to all three of these
        ([b"EX", b"abc", b"EX", b"10"], b"+OK\r\n"),
        ([b"PX", b"abc", b"PX", b"9999"], b"+OK\r\n"),
        ([b"EX", b"abc", b"EX", b"xyz", b"EX", b"10"], b"+OK\r\n"),
        # the last occurrence is the one that has to parse
        ([b"EX", b"10", b"EX", b"abc"], b"-ERR value is not an integer or out of range\r\n"),
        ([b"EX", b"abc"], b"-ERR value is not an integer or out of range\r\n"),
        # meeting the other member of the family is still a syntax error the moment it
        # happens, and outranks the unparsed field before it -- also measured
        ([b"EX", b"abc", b"PX", b"10000"], b"-ERR syntax error\r\n"),
        # and the value that survives is the one that takes effect, not merely accepted
        ([b"EX", b"9223372036854775807", b"EX", b"10"], b"+OK\r\n"),
    ],
    ids=["ex-repeat", "px-repeat", "ex-thrice", "last-is-bad", "single-bad",
         "cross-family", "overflow-then-valid"],
)
def test_set_validates_only_the_ttl_option_that_survives_a_repeat(store, conn, options, expected):
    # parsing inside the scanning loop rejects a malformed earlier occurrence that a
    # later one legally overrides. the overflow check was already deferred past the loop
    # for exactly this reason; the grammar check has to be deferred with it
    response, _effects = commands.dispatch(store, conn, [b"SET", b"k", b"v"] + options)
    assert response == expected


def test_the_surviving_ttl_option_is_the_one_applied(conn):
    # +OK alone would pass even if the wrong occurrence won, so this reads the deadline back
    store = FrozenStore()
    response, _effects = commands.dispatch(
        store, conn, [b"SET", b"k", b"v", b"EX", b"abc", b"EX", b"10"])
    assert response == b"+OK\r\n"
    assert store.deadline(b"k") == FROZEN + 10_000


# --- the four options SET used to refuse ------------------------------------------------


def test_get_returns_the_replaced_value_and_still_writes(store, conn):
    commands.dispatch(store, conn, [b"SET", b"k", b"old"])
    assert commands.dispatch(store, conn, [b"SET", b"k", b"new", b"GET"]) == (
        b"$3\r\nold\r\n", [[b"SET", b"k", b"new"]])
    assert store.lookup(b"k") == b"new"


def test_get_on_a_key_that_was_not_there_answers_nil(store, conn):
    assert commands.dispatch(store, conn, [b"SET", b"k", b"v", b"GET"]) == (
        b"$-1\r\n", [[b"SET", b"k", b"v"]])


@pytest.mark.parametrize("condition, preset, expected", [
    # GET reports what was there whether or not the condition let the write happen, which
    # is the half a reply built only from the write's outcome would get wrong
    (b"NX", b"old", b"$3\r\nold\r\n"),
    (b"XX", None, b"$-1\r\n"),
])
def test_get_reports_the_old_value_even_when_the_condition_refuses_the_write(
        store, conn, condition, preset, expected):
    if preset is not None:
        commands.dispatch(store, conn, [b"SET", b"k", preset])
    response, effects = commands.dispatch(
        store, conn, [b"SET", b"k", b"v", condition, b"GET"])
    assert (response, effects) == (expected, [])
    assert store.lookup(b"k") == preset, "the refused write must not have happened"


def test_keepttl_keeps_the_deadline_a_plain_set_would_discard(conn):
    store = FrozenStore()
    commands.dispatch(store, conn, [b"SET", b"k", b"v", b"EX", b"100"])
    assert store.deadline(b"k") == FROZEN + 100_000
    commands.dispatch(store, conn, [b"SET", b"k", b"w", b"KEEPTTL"])
    assert store.deadline(b"k") == FROZEN + 100_000, "KEEPTTL must not clear the deadline"
    commands.dispatch(store, conn, [b"SET", b"k", b"x"])
    assert store.deadline(b"k") is None, "a plain SET must still discard it"


@pytest.mark.parametrize("token, argument, expected_deadline", [
    # EXAT and PXAT are absolute where EX and PX are durations -- the deadline is the
    # argument itself, not the argument added to now
    (b"EXAT", b"2000000000", 2_000_000_000_000),
    (b"PXAT", b"2000000000000", 2_000_000_000_000),
])
def test_exat_and_pxat_take_an_absolute_deadline(conn, token, argument, expected_deadline):
    store = FrozenStore()
    assert commands.dispatch(store, conn, [b"SET", b"k", b"v", token, argument]) == (
        b"+OK\r\n", [[b"SET", b"k", b"v"],
                     [b"PEXPIREAT", b"k", b"%d" % expected_deadline]])
    assert store.deadline(b"k") == expected_deadline


def test_an_absolute_deadline_already_past_is_accepted_and_the_key_reads_as_gone(conn):
    # the reference bounds the argument at zero, not the resulting deadline, so PXAT 1
    # is a legal request for a moment that has been and gone
    store = FrozenStore()
    assert commands.dispatch(store, conn, [b"SET", b"k", b"v", b"PXAT", b"1"])[0] == b"+OK\r\n"
    assert store.lookup(b"k") is None


@pytest.mark.parametrize("options, expected", [
    # every member of the deadline family conflicts with every other one
    ([b"EX", b"10", b"KEEPTTL"], b"-ERR syntax error\r\n"),
    ([b"KEEPTTL", b"EX", b"10"], b"-ERR syntax error\r\n"),
    ([b"EXAT", b"1", b"EX", b"10"], b"-ERR syntax error\r\n"),
    ([b"PX", b"1", b"PXAT", b"1"], b"-ERR syntax error\r\n"),
    # but a repeat of the same one is ordinary, and GET has nothing to conflict with
    ([b"KEEPTTL", b"KEEPTTL"], b"+OK\r\n"),
    ([b"EXAT", b"2000000000", b"EXAT", b"2000000001"], b"+OK\r\n"),
    ([b"GET", b"GET"], b"$-1\r\n"),
    # and the argument shapes the family shares
    ([b"EXAT", b"0"], b"-ERR invalid expire time in 'set' command\r\n"),
    ([b"EXAT", b"-1"], b"-ERR invalid expire time in 'set' command\r\n"),
    ([b"EXAT", b"abc"], b"-ERR value is not an integer or out of range\r\n"),
    ([b"EXAT"], b"-ERR syntax error\r\n"),
    ([b"EXAT", b"9223372036854775807"], b"-ERR invalid expire time in 'set' command\r\n"),
], ids=lambda v: v if isinstance(v, bytes) else "-".join(o.decode() for o in v))
def test_the_deadline_family_grammar(store, conn, options, expected):
    assert commands.dispatch(store, conn, [b"SET", b"k", b"v"] + options)[0] == expected


@pytest.mark.parametrize("options", [
    [b"get"], [b"keepttl"], [b"exat", b"2000000000"], [b"pxat", b"2000000000000"],
], ids=["get", "keepttl", "exat", "pxat"])
def test_the_new_options_are_case_insensitive_like_the_old_ones(store, conn, options):
    assert commands.dispatch(store, conn, [b"SET", b"k", b"v"] + options)[0] != (
        b"-ERR syntax error\r\n")


def test_keepttl_is_carried_into_the_effect_so_a_replay_keeps_the_deadline_too(conn):
    # the effect is what a follower replays, and a plain SET's own keep_ttl=False clears
    # the deadline this command exists to preserve. dropping the token leaves the two
    # ends disagreeing about when the key dies, with nothing to notice
    leader = FrozenStore()
    commands.dispatch(leader, conn, [b"SET", b"k", b"v1", b"EX", b"100"])
    _response, effects = commands.dispatch(leader, conn, [b"SET", b"k", b"v2", b"KEEPTTL"])
    assert effects == [[b"SET", b"k", b"v2", b"KEEPTTL"]]

    follower = FrozenStore()
    commands.dispatch(follower, conn, [b"SET", b"k", b"v1", b"EX", b"100"])
    for effect in effects:
        commands.dispatch(follower, conn, effect)
    assert follower.deadline(b"k") == leader.deadline(b"k") == FROZEN + 100_000


@pytest.mark.parametrize("token, argument", [(b"EXAT", b"1"), (b"PXAT", b"1")],
                         ids=["exat", "pxat"])
def test_an_absolute_deadline_already_past_deletes_rather_than_scheduling(conn, token, argument):
    # EXAT and PXAT are the only options that resolve a valid argument to an instant
    # already gone. Writing that into the expiry index leaves a key only a later lookup
    # would collect, and with no sweep nothing guarantees one ever comes -- so the key
    # stays resident forever. EXPIRE's own path deletes on the spot for this reason
    store = FrozenStore()
    response, effects = commands.dispatch(store, conn, [b"SET", b"k", b"v", token, argument])
    assert response == b"+OK\r\n"
    assert effects == [[b"DEL", b"k"]], "a past deadline propagates as the deletion it is"
    assert b"k" not in store._data, "the key must not be left resident"
    assert b"k" not in store._expiry, "and must not be left in the expiry index"


def test_a_past_deadline_with_get_still_answers_the_replaced_value(conn):
    store = FrozenStore()
    commands.dispatch(store, conn, [b"SET", b"k", b"old"])
    assert commands.dispatch(store, conn, [b"SET", b"k", b"new", b"PXAT", b"1", b"GET"]) == (
        b"$3\r\nold\r\n", [[b"DEL", b"k"]])
    assert b"k" not in store._data


def test_a_thousand_past_dated_writes_leave_nothing_behind(conn):
    # the shape the leak took: every key client-invisible, every key still in memory
    store = FrozenStore()
    for i in range(1000):
        commands.dispatch(store, conn, [b"SET", b"g%d" % i, b"v", b"PXAT", b"1"])
    assert store._data == {} and store._expiry == {}
    store.check_invariants()


def test_set_with_get_raises_wrongtype_through_dispatch():
    # SET is type-agnostic and overwrites anything, but GET makes it read the value it is
    # replacing, and reading a value of the wrong kind is WRONGTYPE. Without the kind on
    # that lookup the command answers a plausible-looking old value and overwrites the
    # key -- the corruption the check exists to prevent. Unreachable while only strings
    # exist; live the day commands/list.py ships, which is when nobody will be looking
    store = ListKinded()
    store.write(b"w", b"v", keep_ttl=False)
    assert commands.dispatch(store, None, [b"SET", b"w", b"new", b"GET"]) == (
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", [])
    assert store.lookup(b"w") == b"v", "the refused command must not have written"
