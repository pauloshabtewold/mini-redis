"""The keyspace's contract over chosen inputs: a value written and read back, expiry
deadlines, keep_ttl in both directions, the queue lazy expiry feeds and remove() does not,
and the WRONGTYPE check. test_store_properties.py covers the same contract over sequences
this module does not choose.
"""

from collections import deque

import pytest

from store import KIND_LIST, KIND_STRING, Store, WrongTypeError
from tests.conftest import FrozenStore


@pytest.fixture
def keyspace():
    return Store()


@pytest.fixture(autouse=True)
def _check_invariants_after_every_test(keyspace):
    # it runs from an autouse fixture at the end of every store test
    yield
    keyspace.check_invariants()


# normal


def test_a_written_value_reads_back(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    assert keyspace.lookup(b"k") == b"v"


def test_a_written_deadline_reads_back(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    future = keyspace.now_ms() + 3_600_000
    keyspace.expire_at(b"k", future)
    assert keyspace.deadline(b"k") == future


def test_write_without_keep_ttl_clears_an_existing_deadline(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace.expire_at(b"k", keyspace.now_ms() + 3_600_000)
    keyspace.write(b"k", b"v2", keep_ttl=False)
    assert keyspace.deadline(b"k") is None


def test_write_with_keep_ttl_preserves_an_existing_deadline(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    future = keyspace.now_ms() + 3_600_000
    keyspace.expire_at(b"k", future)
    keyspace.write(b"k", b"v2", keep_ttl=True)
    assert keyspace.deadline(b"k") == future


def test_remove_reports_whether_the_key_was_present(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    assert keyspace.remove(b"k") is True
    assert keyspace.remove(b"k") is False


def test_take_effects_returns_and_clears(keyspace):
    keyspace.write(b"e", b"v", keep_ttl=False)
    keyspace.expire_at(b"e", 1)
    keyspace.lookup(b"e")
    assert keyspace.take_effects() == [[b"DEL", b"e"]]
    assert keyspace.take_effects() == []


def test_kind_of_recognizes_a_deque_as_a_list(keyspace):
    assert keyspace.kind_of(deque([b"a"])) == KIND_LIST
    assert keyspace.kind_of(deque()) == KIND_LIST, "an empty deque is still a list"


def test_live_count_and_live_keys_exclude_an_expired_key_without_removing_it(keyspace):
    keyspace.write(b"live", b"v", keep_ttl=False)
    keyspace.write(b"gone", b"v", keep_ttl=False)
    keyspace.expire_at(b"gone", keyspace.now_ms() - 1)
    assert keyspace.live_count() == 1
    assert list(keyspace.live_keys()) == [b"live"]
    assert b"gone" in keyspace._data
    assert keyspace.take_effects() == []


def test_expiring_count_counts_only_unexpired_deadlines(keyspace):
    keyspace.write(b"a", b"v", keep_ttl=False)
    keyspace.write(b"b", b"v", keep_ttl=False)
    keyspace.expire_at(b"a", keyspace.now_ms() + 3_600_000)
    keyspace.expire_at(b"b", keyspace.now_ms() - 1)
    assert keyspace.expiring_count() == 1


def test_flush_removes_every_key_expired_or_not_and_reports_the_count(keyspace):
    keyspace.write(b"live", b"v", keep_ttl=False)
    keyspace.write(b"gone", b"v", keep_ttl=False)
    keyspace.expire_at(b"gone", keyspace.now_ms() - 1)
    assert keyspace.flush() == 2
    assert keyspace.live_count() == 0
    assert list(keyspace.live_keys()) == []
    assert keyspace._data == {} and keyspace._expiry == {}
    assert keyspace.take_effects() == []


def test_a_dropped_deadline_leaves_the_sampling_index_consistent(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace.expire_at(b"k", keyspace.now_ms() + 3_600_000)
    assert keyspace._expiry_keys == [b"k"] and keyspace._expiry_slots == {b"k": 0}
    keyspace.remove(b"k")
    assert keyspace._expiry_keys == [] and keyspace._expiry_slots == {}


def test_sample_and_expire_removes_only_the_expired_keys_and_queues_one_del_each(keyspace):
    now = keyspace.now_ms()
    for i in range(10):
        key = b"k%d" % i
        keyspace.write(key, b"v", keep_ttl=False)
        keyspace.expire_at(key, now - 1 if i % 2 == 0 else now + 3_600_000)
    sampled, expired = keyspace.sample_and_expire(10)
    assert (sampled, expired) == (10, 5)
    effects = keyspace.take_effects()
    assert len(effects) == 5
    assert all(effect[0] == b"DEL" for effect in effects)
    assert {effect[1] for effect in effects} == {b"k%d" % i for i in range(0, 10, 2)}
    assert sorted(keyspace._data) == sorted(b"k%d" % i for i in range(1, 10, 2))


def test_snapshot_items_reports_minus_one_for_a_key_with_no_deadline(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    assert list(keyspace.snapshot_items()) == [(b"k", KIND_STRING, b"v", -1)]


def test_from_items_round_trips_both_kinds_and_their_deadlines(keyspace):
    keyspace.write(b"s", b"v", keep_ttl=False)
    keyspace.write(b"l", deque([b"a", b"b"]), keep_ttl=False)
    keyspace.write(b"t", b"v", keep_ttl=False)
    future = keyspace.now_ms() + 3_600_000
    keyspace.expire_at(b"t", future)
    loaded = Store.from_items(list(keyspace.snapshot_items()))
    assert loaded.lookup(b"s") == b"v"
    assert list(loaded.lookup(b"l")) == [b"a", b"b"]
    assert loaded.deadline(b"t") == future
    assert loaded.deadline(b"s") is None


# edge


def test_an_empty_value_is_distinct_from_a_missing_key(keyspace):
    keyspace.write(b"empty", b"", keep_ttl=False)
    assert keyspace.lookup(b"empty") == b""
    assert keyspace.lookup(b"missing") is None


def test_a_deadline_equal_to_now_is_expired(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace.expire_at(b"k", keyspace.now_ms())
    assert keyspace.lookup(b"k") is None


def test_two_lookups_of_the_same_expired_key_queue_one_del(keyspace):
    keyspace.write(b"e", b"v", keep_ttl=False)
    keyspace.expire_at(b"e", 1)
    assert keyspace.lookup(b"e") is None
    assert keyspace.lookup(b"e") is None
    assert keyspace.take_effects() == [[b"DEL", b"e"]]


def test_remove_on_a_live_key_feeds_the_queue_nothing(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace.remove(b"k")
    assert keyspace.take_effects() == []


def test_lookup_with_a_matching_kind_returns_the_value(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    assert keyspace.lookup(b"k", KIND_STRING) == b"v"


def test_live_count_and_live_keys_treat_a_deadline_equal_to_now_as_expired(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace.expire_at(b"k", keyspace.now_ms())
    assert keyspace.live_count() == 0
    assert list(keyspace.live_keys()) == []
    assert b"k" in keyspace._data


def test_a_plain_set_over_a_key_with_a_ttl_drops_it_from_the_sampling_index(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace.expire_at(b"k", keyspace.now_ms() + 3_600_000)
    keyspace.write(b"k", b"v2", keep_ttl=False)
    assert keyspace._expiry_keys == [] and keyspace._expiry_slots == {}


def test_re_setting_a_deadline_appends_no_second_slot(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace.expire_at(b"k", keyspace.now_ms() + 3_600_000)
    keyspace.expire_at(b"k", keyspace.now_ms() + 7_200_000)
    assert keyspace._expiry_keys == [b"k"] and keyspace._expiry_slots == {b"k": 0}


def test_removing_the_last_indexed_key_leaves_the_index_consistent(keyspace):
    for name in (b"a", b"b", b"c"):
        keyspace.write(name, b"v", keep_ttl=False)
        keyspace.expire_at(name, keyspace.now_ms() + 3_600_000)
    keyspace.remove(b"c")
    assert keyspace._expiry_keys == [b"a", b"b"]
    assert keyspace._expiry_slots == {b"a": 0, b"b": 1}


def test_removing_a_middle_indexed_key_moves_the_last_one_into_its_slot(keyspace):
    for name in (b"a", b"b", b"c"):
        keyspace.write(name, b"v", keep_ttl=False)
        keyspace.expire_at(name, keyspace.now_ms() + 3_600_000)
    keyspace.remove(b"a")
    # which of the two remaining keys lands in the vacated slot is the swap-delete's own
    # choice; what must hold is that the list and the map agree on all of them
    assert sorted(keyspace._expiry_keys) == [b"b", b"c"]
    assert {key: keyspace._expiry_slots[key] for key in keyspace._expiry_keys} == {
        key: i for i, key in enumerate(keyspace._expiry_keys)
    }


def test_a_deadline_of_zero_is_dropped_rather_than_treated_as_absent(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace.expire_at(b"k", 0)
    assert keyspace._expiry == {b"k": 0} and keyspace._expiry_keys == [b"k"]
    keyspace.remove(b"k")
    assert keyspace._expiry == {}
    assert keyspace._expiry_keys == [] and keyspace._expiry_slots == {}

    # 0 is a legal past deadline and falsy, so a truth test over the popped value
    # would read it as absent rather than dropped -- driven through write()'s own
    # pop site too, not only remove()'s, so a regression at either call site is
    # caught rather than only the one this test happened to reach first
    keyspace.write(b"k2", b"v", keep_ttl=False)
    keyspace.expire_at(b"k2", 0)
    keyspace.write(b"k2", b"v2", keep_ttl=False)
    assert keyspace._expiry == {}
    assert keyspace._expiry_keys == [] and keyspace._expiry_slots == {}


def test_snapshot_items_includes_a_resident_expired_key_with_its_deadline(keyspace):
    keyspace.write(b"gone", b"v", keep_ttl=False)
    past = keyspace.now_ms() - 1
    keyspace.expire_at(b"gone", past)
    assert list(keyspace.snapshot_items()) == [(b"gone", KIND_STRING, b"v", past)]
    assert b"gone" in keyspace._data, "snapshot_items() must not remove anything"


def test_sample_and_expire_on_a_keyspace_with_no_deadlines_reports_nothing(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    # count of 20 against zero indexed keys -- this is exactly what
    # random.sample would refuse outright without sample_and_expire's own clamp
    assert keyspace.sample_and_expire(20) == (0, 0)


def test_sample_and_expire_never_draws_the_same_key_twice(keyspace):
    now = keyspace.now_ms()
    for i in range(200):
        key = b"d%d" % i
        keyspace.write(key, b"v", keep_ttl=False)
        keyspace.expire_at(key, now - 1)
    sampled, expired = keyspace.sample_and_expire(20)
    assert (sampled, expired) == (20, 20)
    drawn = [effect[1] for effect in keyspace.take_effects()]
    assert len(drawn) == len(set(drawn)) == 20

    # drawing all 20 keys the index holds: with replacement, 20 distinct results out of
    # 20 draws has probability 20!/20**20 (~2.3e-8), so a regression to sampling with
    # replacement fails this arm deterministically in practice, unlike the 200-key arm
    # above
    keyspace.flush()
    for i in range(20):
        key = b"e%d" % i
        keyspace.write(key, b"v", keep_ttl=False)
        keyspace.expire_at(key, now - 1)
    sampled, expired = keyspace.sample_and_expire(20)
    assert (sampled, expired) == (20, 20)
    drawn = [effect[1] for effect in keyspace.take_effects()]
    assert len(drawn) == len(set(drawn)) == 20


def test_from_items_builds_a_list_value_as_a_deque_not_a_list(keyspace):
    keyspace.write(b"l", deque([b"a", b"b"]), keep_ttl=False)
    loaded = Store.from_items(list(keyspace.snapshot_items()))
    assert type(loaded._data[b"l"]) is deque
    assert loaded.kind_of(loaded._data[b"l"]) == KIND_LIST


def test_from_items_on_a_subclass_returns_the_subclass():
    frozen = FrozenStore()
    frozen.write(b"k", b"v", keep_ttl=False)
    loaded = FrozenStore.from_items(list(frozen.snapshot_items()))
    assert type(loaded) is FrozenStore


# error


def test_expire_at_on_a_missing_key_raises_keyerror_naming_it(keyspace):
    with pytest.raises(KeyError) as exc_info:
        keyspace.expire_at(b"ghost", 1)
    assert b"ghost" in exc_info.value.args, exc_info.value.args


def test_write_with_keep_ttl_omitted_raises_typeerror(keyspace):
    # the only call site in this whole feature that exercises the missing keyword: every
    # other caller passes keep_ttl explicitly, so a silently added default is reached by
    # nothing else here
    with pytest.raises(TypeError):
        keyspace.write(b"k", b"v")


def test_lookup_with_a_mismatched_kind_raises_wrongtype(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    with pytest.raises(WrongTypeError) as exc_info:
        keyspace.lookup(b"k", b"list")
    assert exc_info.value.message == (
        b"WRONGTYPE Operation against a key holding the wrong kind of value"
    )


def test_lookup_with_a_mismatched_kind_raises_wrongtype_for_a_real_list(keyspace):
    keyspace.write(b"k", deque([b"a"]), keep_ttl=False)
    with pytest.raises(WrongTypeError) as exc_info:
        keyspace.lookup(b"k", KIND_STRING)
    assert exc_info.value.message == (
        b"WRONGTYPE Operation against a key holding the wrong kind of value"
    )


def test_a_missing_key_is_missing_before_it_is_the_wrong_type(keyspace):
    assert keyspace.lookup(b"gone", b"list") is None


@pytest.mark.parametrize("value", [123, None, [b"a"], {b"a"}, "str"],
                         ids=["int", "none", "list", "set", "str"])
def test_kind_of_on_a_non_bytes_value_raises_typeerror(keyspace, value):
    with pytest.raises(TypeError):
        keyspace.kind_of(value)


def test_check_invariants_raises_on_an_injected_orphan(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace._expiry[b"ghost"] = keyspace.now_ms() + 3_600_000
    with pytest.raises(AssertionError):
        keyspace.check_invariants()
    del keyspace._expiry[b"ghost"]  # the teardown fixture calls check_invariants() too


def test_check_invariants_raises_on_an_injected_sampling_index_disagreement(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace.expire_at(b"k", keyspace.now_ms() + 3_600_000)

    # size: appending a key to _expiry_keys alone, leaving _expiry and _expiry_slots
    # untouched, throws the three counts out of step before the per-key loop below
    # ever runs -- the only one of the three sites an appended key can reach
    keyspace._expiry_keys.append(b"ghost_size")
    with pytest.raises(AssertionError, match="disagrees with _expiry on size"):
        keyspace.check_invariants()
    keyspace._expiry_keys.pop()

    # no deadline: swapping the resident key for one _expiry has never heard of, in
    # place rather than appended, leaves every count equal -- only the per-key loop's
    # own membership check can still catch it
    slot = keyspace._expiry_slots.pop(b"k")
    keyspace._expiry_keys[slot] = b"ghost_deadline"
    keyspace._expiry_slots[b"ghost_deadline"] = slot
    with pytest.raises(AssertionError, match="holds a key with no deadline"):
        keyspace.check_invariants()
    del keyspace._expiry_slots[b"ghost_deadline"]
    keyspace._expiry_keys[slot] = b"k"
    keyspace._expiry_slots[b"k"] = slot

    # slot: the two structures agree on which keys exist and disagree only about where
    keyspace._expiry_slots[b"k"] = 7
    with pytest.raises(AssertionError, match="disagrees with itself about"):
        keyspace.check_invariants()
    keyspace._expiry_slots[b"k"] = 0  # the teardown fixture calls check_invariants() too
