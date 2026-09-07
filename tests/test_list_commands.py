"""LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN: push order, the index matrix, and the
empty-container cleanup that removes a list's last element along with the key.
"""

from collections import deque

import pytest

import commands
from store import Store
from tests.conftest import FROZEN, FrozenStore


@pytest.fixture
def store():
    return Store()


# --- normal cases -----------------------------------------------------------------------


def test_rpush_appends_left_to_right(store, conn):
    assert commands.dispatch(store, conn, [b"RPUSH", b"k", b"a", b"b", b"c"]) == (
        b":3\r\n", [[b"RPUSH", b"k", b"a", b"b", b"c"]])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"0", b"-1"]) == (
        b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n", [])


def test_lpush_prepends_left_to_right_and_ends_up_reversed(store, conn):
    # each element is prepended in turn, so the last one pushed ends up at the head --
    # the opposite order from RPUSH over the same arguments
    assert commands.dispatch(store, conn, [b"LPUSH", b"k", b"a", b"b", b"c"]) == (
        b":3\r\n", [[b"LPUSH", b"k", b"a", b"b", b"c"]])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"0", b"-1"]) == (
        b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n", [])


def test_push_onto_an_existing_list_appends_rather_than_replacing(store, conn):
    commands.dispatch(store, conn, [b"RPUSH", b"k", b"a"])
    assert commands.dispatch(store, conn, [b"RPUSH", b"k", b"b"]) == (
        b":2\r\n", [[b"RPUSH", b"k", b"b"]])
    assert commands.dispatch(store, conn, [b"LLEN", b"k"]) == (b":2\r\n", [])


def test_llen_on_a_live_list(store, conn):
    commands.dispatch(store, conn, [b"RPUSH", b"k", b"a", b"b", b"c"])
    assert commands.dispatch(store, conn, [b"LLEN", b"k"]) == (b":3\r\n", [])


def test_lrange_a_single_element_at_each_end(store, conn):
    commands.dispatch(store, conn, [b"RPUSH", b"k", b"a", b"b", b"c"])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"0", b"0"]) == (
        b"*1\r\n$1\r\na\r\n", [])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"-1", b"-1"]) == (
        b"*1\r\n$1\r\nc\r\n", [])


def test_lrange_indices_wider_than_the_list_still_returns_everything(store, conn):
    commands.dispatch(store, conn, [b"RPUSH", b"k", b"a", b"b", b"c"])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"-100", b"100"]) == (
        b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n", [])


def test_lpop_and_rpop_take_the_element_from_the_matching_end(store, conn):
    commands.dispatch(store, conn, [b"RPUSH", b"k", b"a", b"b", b"c"])
    assert commands.dispatch(store, conn, [b"LPOP", b"k"]) == (b"$1\r\na\r\n", [[b"LPOP", b"k"]])
    assert commands.dispatch(store, conn, [b"RPOP", b"k"]) == (b"$1\r\nc\r\n", [[b"RPOP", b"k"]])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"0", b"-1"]) == (
        b"*1\r\n$1\r\nb\r\n", [])


def test_type_on_a_list(store, conn):
    commands.dispatch(store, conn, [b"RPUSH", b"k", b"a"])
    assert commands.dispatch(store, conn, [b"TYPE", b"k"]) == (b"+list\r\n", [])


# --- edge cases --------------------------------------------------------------------------


@pytest.mark.parametrize("lo, hi", [(b"2", b"1"), (b"5", b"10"), (b"-1", b"-2")])
def test_lrange_boundaries_that_resolve_to_nothing(store, conn, lo, hi):
    commands.dispatch(store, conn, [b"RPUSH", b"k", b"a", b"b", b"c"])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", lo, hi]) == (b"*0\r\n", [])


def test_lrange_on_a_missing_key_is_an_empty_array_not_nil(store, conn):
    assert commands.dispatch(store, conn, [b"LRANGE", b"nosuchkey", b"0", b"-1"]) == (
        b"*0\r\n", [])


def test_lpop_and_rpop_on_a_missing_key_answer_nil_with_no_effects(store, conn):
    assert commands.dispatch(store, conn, [b"LPOP", b"nosuchkey"]) == (b"$-1\r\n", [])
    assert commands.dispatch(store, conn, [b"RPOP", b"nosuchkey"]) == (b"$-1\r\n", [])


def test_llen_on_a_missing_key_is_zero(store, conn):
    assert commands.dispatch(store, conn, [b"LLEN", b"nosuchkey"]) == (b":0\r\n", [])


def test_popping_the_last_element_removes_the_key_entirely(store, conn):
    commands.dispatch(store, conn, [b"RPUSH", b"k", b"a"])
    store.take_effects()
    reply, effects = commands.dispatch(store, conn, [b"LPOP", b"k"])
    assert reply == b"$1\r\na\r\n"
    # the pop's own effect and nothing beside it -- a DEL here would double up the
    # moment a follower runs the identical cleanup on its own copy of this same pop
    assert effects == [[b"LPOP", b"k"]]
    assert store.take_effects() == []
    assert commands.dispatch(store, conn, [b"EXISTS", b"k"]) == (b":0\r\n", [])
    assert commands.dispatch(store, conn, [b"TYPE", b"k"]) == (b"+none\r\n", [])
    assert commands.dispatch(store, conn, [b"LLEN", b"k"]) == (b":0\r\n", [])
    store.check_invariants()


def test_popping_the_last_element_from_the_right_also_removes_the_key(store, conn):
    commands.dispatch(store, conn, [b"RPUSH", b"k", b"a"])
    store.take_effects()
    reply, effects = commands.dispatch(store, conn, [b"RPOP", b"k"])
    assert reply == b"$1\r\na\r\n"
    assert effects == [[b"RPOP", b"k"]]
    assert store.take_effects() == []
    assert commands.dispatch(store, conn, [b"EXISTS", b"k"]) == (b":0\r\n", [])


def test_popping_the_last_element_drops_a_ttl_set_on_the_key():
    # the container is gone, and store.remove() clears the expiry index along with the
    # keyspace entry -- a deadline outliving the key it was set on is exactly the
    # orphan check_invariants() watches for
    s = FrozenStore()
    commands.dispatch(s, None, [b"RPUSH", b"t", b"a"])
    commands.dispatch(s, None, [b"EXPIRE", b"t", b"100"])
    s.take_effects()
    assert commands.dispatch(s, None, [b"RPOP", b"t"]) == (b"$1\r\na\r\n", [[b"RPOP", b"t"]])
    assert s.take_effects() == []
    assert commands.dispatch(s, None, [b"TTL", b"t"]) == (b":-2\r\n", [])
    s.check_invariants()


def test_a_push_preserves_an_existing_ttl():
    # in-place mutation is what makes this automatic: the key is never rewritten after
    # the first push creates it, so nothing here has to remember to carry a deadline
    # forward the way SET's own keep_ttl parameter does
    s = FrozenStore()
    commands.dispatch(s, None, [b"RPUSH", b"t", b"a", b"b"])
    commands.dispatch(s, None, [b"EXPIRE", b"t", b"100"])
    before = s.deadline(b"t")
    assert before == FROZEN + 100_000
    commands.dispatch(s, None, [b"RPUSH", b"t", b"c"])
    commands.dispatch(s, None, [b"LPUSH", b"t", b"z"])
    assert s.deadline(b"t") == before, "a push rewrote the container and cleared the TTL"
    assert commands.dispatch(s, None, [b"TTL", b"t"]) == (b":100\r\n", [])


def test_lrange_reads_the_same_run_from_whichever_end_is_nearer(store, conn):
    # lrange walks forward from the head for a range in the front half and backward off
    # reversed() for one in the back half, because islice only ever starts at the head and
    # a range near the tail would otherwise pay the whole length of the list. The two
    # walks have to agree on every range, including the ones that straddle the midpoint,
    # so the oracle here is a plain list slice -- a different computation from either
    # walk, and the one thing the backward path could get wrong without the forward path
    # noticing. Nine elements puts the boundary at index four with ranges on both sides
    elements = [b"e%d" % i for i in range(9)]
    commands.dispatch(store, conn, [b"RPUSH", b"k"] + elements)
    for start in range(-12, 13):
        for stop in range(-12, 13):
            reply, effects = commands.dispatch(
                store, conn, [b"LRANGE", b"k", b"%d" % start, b"%d" % stop])
            assert effects == []
            lo = start + 9 if start < 0 else start
            hi = stop + 9 if stop < 0 else stop
            expected = elements[max(lo, 0):hi + 1] if hi >= 0 else []
            assert reply.split(b"\r\n")[2:-1:2] == expected, (start, stop)


class _CountingDeque(deque):
    """A deque that records how many elements a walk over it actually visited.

    `Store.kind_of` recognises a value by `isinstance(value, deque)`, so a subclass is
    still a list to every command. Counting is what turns "reads from the nearer end"
    into something assertable without a clock in it.
    """

    def __init__(self, *args):
        super().__init__(*args)
        self.steps = 0

    def _counted(self, walk):
        for item in walk:
            self.steps += 1
            yield item

    def __iter__(self):
        return self._counted(super().__iter__())

    def __reversed__(self):
        return self._counted(super().__reversed__())


def test_lrange_walks_from_the_nearer_end_rather_than_always_from_the_head(store, conn):
    # the reason the walk has two directions, asserted as steps rather than as elapsed
    # time -- a wall-clock budget on a shared machine is a flake, and the step count is
    # the thing that actually changed. islice always starts at the head, so a head-first
    # read of the last of a thousand elements visits all thousand; from the tail it
    # visits one. Without this the suite cannot tell the two-ended walk from the
    # head-first one it replaced, since they answer identically on every input
    container = _CountingDeque(b"e%d" % i for i in range(1000))
    store.write(b"k", container, keep_ttl=False)

    container.steps = 0
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"-1", b"-1"])[0] == (
        b"*1\r\n$4\r\ne999\r\n")
    assert container.steps <= 2, container.steps

    container.steps = 0
    commands.dispatch(store, conn, [b"LRANGE", b"k", b"0", b"0"])
    assert container.steps <= 2, container.steps

    # the midpoint is genuinely half the list away from either end, and no choice of
    # direction avoids that -- the reference's own quicklist pays it there too
    container.steps = 0
    commands.dispatch(store, conn, [b"LRANGE", b"k", b"500", b"500"])
    assert 400 <= container.steps <= 600, container.steps


def test_lrange_reads_a_single_element_off_the_tail_without_walking_the_list(store, conn):
    # the shape the two-ended walk exists for. Timing is not the assertion -- a wall-clock
    # budget on a shared machine is a flake -- so this asserts the answer on a list long
    # enough that a head-first walk would be doing thousands of steps for one element
    commands.dispatch(
        store, conn, [b"RPUSH", b"k"] + [b"e%d" % i for i in range(5000)])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"-1", b"-1"]) == (
        b"*1\r\n$5\r\ne4999\r\n", [])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"-2", b"-1"]) == (
        b"*2\r\n$5\r\ne4998\r\n$5\r\ne4999\r\n", [])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"4999", b"4999"]) == (
        b"*1\r\n$5\r\ne4999\r\n", [])


def test_push_and_pop_round_trip_a_non_utf8_element(store, conn):
    value = b"\xff\x00\xfe\x80"
    commands.dispatch(store, conn, [b"RPUSH", b"k", value])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"0", b"-1"]) == (
        b"*1\r\n$4\r\n" + value + b"\r\n", [])
    assert commands.dispatch(store, conn, [b"LPOP", b"k"]) == (
        b"$4\r\n" + value + b"\r\n", [[b"LPOP", b"k"]])


def test_invariants_hold_after_a_mix_of_list_operations(store, conn):
    commands.dispatch(store, conn, [b"RPUSH", b"a", b"1"])
    commands.dispatch(store, conn, [b"EXPIRE", b"a", b"10"])
    commands.dispatch(store, conn, [b"LPOP", b"a"])
    commands.dispatch(store, conn, [b"RPUSH", b"b", b"1", b"2"])
    commands.dispatch(store, conn, [b"RPOP", b"b"])
    store.check_invariants()


# --- error cases -------------------------------------------------------------------------


NOT_INT = b"-ERR value is not an integer or out of range\r\n"
WRONGTYPE = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"


@pytest.mark.parametrize(
    "bad", [b"abc", b"011", b"+5", b"1_0", b"-0", b"", b"9223372036854775808"])
def test_lrange_rejects_every_bad_index_in_either_position(store, conn, bad):
    commands.dispatch(store, conn, [b"RPUSH", b"k", b"a"])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", bad, b"1"]) == (NOT_INT, [])
    assert commands.dispatch(store, conn, [b"LRANGE", b"k", b"0", bad]) == (NOT_INT, [])


def test_lrange_validates_the_index_before_it_looks_at_a_missing_key(store, conn):
    assert commands.dispatch(store, conn, [b"LRANGE", b"nosuchkey", b"abc", b"1"]) == (
        NOT_INT, [])


def test_lrange_validates_the_index_before_the_kind_check(store, conn):
    # a bad index and a wrong-kind key at once: the index error has to win, because
    # parsing runs before the key is ever looked up
    commands.dispatch(store, conn, [b"SET", b"s", b"v"])
    assert commands.dispatch(store, conn, [b"LRANGE", b"s", b"abc", b"1"]) == (NOT_INT, [])


@pytest.mark.parametrize("argv", [
    [b"LPUSH", b"s", b"x"], [b"RPUSH", b"s", b"x"], [b"LPOP", b"s"], [b"RPOP", b"s"],
    [b"LRANGE", b"s", b"0", b"-1"], [b"LLEN", b"s"],
])
def test_every_list_command_raises_wrongtype_against_a_string_key(store, conn, argv):
    commands.dispatch(store, conn, [b"SET", b"s", b"v"])
    assert commands.dispatch(store, conn, argv) == (WRONGTYPE, [])


@pytest.mark.parametrize("argv, expected", [
    ([b"LPUSH", b"k"], b"-ERR wrong number of arguments for 'lpush' command\r\n"),
    ([b"RPUSH", b"k"], b"-ERR wrong number of arguments for 'rpush' command\r\n"),
    ([b"LPOP"], b"-ERR wrong number of arguments for 'lpop' command\r\n"),
    ([b"RPOP"], b"-ERR wrong number of arguments for 'rpop' command\r\n"),
    ([b"LRANGE", b"k", b"0"], b"-ERR wrong number of arguments for 'lrange' command\r\n"),
    ([b"LLEN"], b"-ERR wrong number of arguments for 'llen' command\r\n"),
])
def test_arity_errors(store, conn, argv, expected):
    assert commands.dispatch(store, conn, argv) == (expected, [])


def test_lpop_and_rpop_reject_a_count_rather_than_implement_it(store, conn):
    commands.dispatch(store, conn, [b"RPUSH", b"k", b"a", b"b"])
    reason = "an exact arity refuses a trailing count instead of accepting and silently ignoring it"
    assert commands.dispatch(store, conn, [b"LPOP", b"k", b"2"]) == (
        b"-ERR wrong number of arguments for 'lpop' command\r\n", []), reason
    assert commands.dispatch(store, conn, [b"RPOP", b"k", b"2"]) == (
        b"-ERR wrong number of arguments for 'rpop' command\r\n", []), reason
