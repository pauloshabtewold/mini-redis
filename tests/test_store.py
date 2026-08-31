"""The keyspace's contract over chosen inputs: a value written and read back, expiry
deadlines, keep_ttl in both directions, the queue lazy expiry feeds and remove() does not,
and the WRONGTYPE check. test_store_properties.py covers the same contract over sequences
this module does not choose.
"""

import pytest

from store import KIND_STRING, Store, WrongTypeError


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


def test_a_missing_key_is_missing_before_it_is_the_wrong_type(keyspace):
    assert keyspace.lookup(b"gone", b"list") is None


def test_kind_of_on_a_non_bytes_value_raises_typeerror(keyspace):
    with pytest.raises(TypeError):
        keyspace.kind_of(123)


def test_check_invariants_raises_on_an_injected_orphan(keyspace):
    keyspace.write(b"k", b"v", keep_ttl=False)
    keyspace._expiry[b"ghost"] = keyspace.now_ms() + 3_600_000
    with pytest.raises(AssertionError):
        keyspace.check_invariants()
    del keyspace._expiry[b"ghost"]  # the teardown fixture calls check_invariants() too
