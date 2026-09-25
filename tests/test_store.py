"""The keyspace's contract over chosen inputs: a value written and read back, expiry
deadlines, keep_ttl in both directions, the queue lazy expiry feeds and remove() does not,
and the WRONGTYPE check. test_store_properties.py covers the same contract over sequences
this module does not choose.
"""

import tracemalloc
from collections import deque

import pytest

from store import KIND_LIST, KIND_STRING, DuplicateKeyError, Store, WrongTypeError
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


def test_sample_and_expire_reads_the_clock_once_for_the_whole_draw_not_once_per_key(keyspace):
    # now_ms() is read once and reused for every key in the sample, not re-read inside
    # the per-key loop -- sample_and_expire's own comment promises this so a caller
    # sweeping many keys does not spend its budget re-reading a clock that has not
    # moved. a per-key read passes every other test in this module, since the extra
    # reads land within the same millisecond as the shared one and expire exactly the
    # same keys; only counting the calls tells the two apart
    for i in range(5):
        key = b"k%d" % i
        keyspace.write(key, b"v", keep_ttl=False)
        keyspace.expire_at(key, keyspace.now_ms() + 3_600_000)
    calls = []
    real_now_ms = Store.now_ms

    def counting_now_ms(self):
        calls.append(1)
        return real_now_ms(self)

    Store.now_ms = counting_now_ms
    try:
        keyspace.sample_and_expire(5)
    finally:
        Store.now_ms = real_now_ms
    assert len(calls) == 1, (
        "sample_and_expire read the clock %d times over a draw of 5 keys, not once"
        % len(calls))


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


def test_from_items_refuses_two_items_that_name_one_key():
    # a snapshot_items() walk can never produce one, since it walks a keyspace whose keys
    # are already unique, so this only arrives from bytes something else wrote. loading it
    # would keep the second item's value and deadline and drop the first's with nothing
    # said, which is the one shape of data loss a length-prefixed format cannot catch for
    # itself. counted rather than watched per key, so what fails here is the count
    items = [(b"k", KIND_STRING, b"first", 1_900_000_000_000),
             (b"k", KIND_STRING, b"second", -1)]
    with pytest.raises(ValueError, match="share one key"):
        Store.from_items(items)


def test_from_items_raises_duplicate_key_error_specifically_for_a_repeated_key():
    # persistence._decode() catches this one by type, to tell a repeated key apart from
    # the bare ValueError an unrecognised kind byte raises -- a caller that only matched
    # on ValueError would report a corrupt kind byte as a repeated key, or the reverse,
    # whichever from_items() happened to raise first
    items = [(b"k", KIND_STRING, b"first", -1),
             (b"k", KIND_STRING, b"second", -1)]
    with pytest.raises(DuplicateKeyError):
        Store.from_items(items)


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


def test_snapshot_items_reports_a_deadline_of_zero_as_zero_not_as_no_deadline(keyspace):
    # -1 is the "no deadline" marker and 0 is a legal deadline already past, and falsy: a
    # `deadline or -1` reads it as absent, and the key comes back from a reload with no
    # deadline at all -- live forever, where it went into the snapshot expired
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace.expire_at(b"k", 0)
    assert list(keyspace.snapshot_items()) == [(b"k", KIND_STRING, b"v", 0)]
    assert Store.from_items(list(keyspace.snapshot_items())).deadline(b"k") == 0


def test_sample_and_expire_on_a_keyspace_with_no_deadlines_reports_nothing(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    # count of 20 against zero indexed keys -- this is exactly what
    # random.sample would refuse outright without sample_and_expire's own clamp
    assert keyspace.sample_and_expire(20) == (0, 0)


def test_sample_and_expire_reclaims_a_key_whose_deadline_equals_now():
    # the sweep's boundary has to be lookup()'s: a deadline equal to now has already
    # passed. a sweep comparing with < leaves that key for a later pass while every read
    # of the same instant already answers that it is gone. a frozen clock, because a
    # live one can tick between expire_at() and the draw and let < pass for the wrong reason
    frozen = FrozenStore()
    frozen.write(b"k", b"v", keep_ttl=False)
    frozen.expire_at(b"k", frozen.now_ms())
    assert frozen.sample_and_expire(20) == (1, 1)
    assert b"k" not in frozen._data
    frozen.check_invariants()


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


def _bytes_allocated_by(call):
    # the peak over what was already allocated when the call began, so the index built
    # before it is not counted against it
    was_tracing = tracemalloc.is_tracing()
    if not was_tracing:
        tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        before, _ = tracemalloc.get_traced_memory()
        call()
        _, peak = tracemalloc.get_traced_memory()
        return peak - before
    finally:
        if not was_tracing:
            tracemalloc.stop()


def test_a_sample_allocates_nothing_that_grows_with_the_expiry_index(keyspace):
    # the sampling index exists so a draw costs the same at any size. drawing from a list
    # rebuilt out of _expiry on every call is the obvious shape and passes every other
    # test here -- the keys it picks are just as random -- while at a million keys it
    # spends milliseconds building that list before looking at one. what a draw
    # allocates is the witness rather than how long it takes, because an allocation does
    # not change size on a loaded machine
    now = keyspace.now_ms()
    for i in range(100_000):
        key = b"k%d" % i
        keyspace.write(key, b"v", keep_ttl=False)
        keyspace.expire_at(key, now + 3_600_000)
    # the control: the rejected shape, over this same index, has to clear the bound by a
    # wide margin, or a bound this loose could not tell the two shapes apart
    rebuilt = _bytes_allocated_by(lambda: list(keyspace._expiry))
    assert rebuilt > 512 * 1024, rebuilt
    drawn = _bytes_allocated_by(lambda: keyspace.sample_and_expire(20))
    assert drawn < 64 * 1024, (
        "a draw of twenty over 100,000 indexed keys allocated %d bytes -- it is building "
        "something the size of the index" % drawn)


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
