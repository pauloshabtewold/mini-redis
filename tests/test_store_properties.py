"""The keyspace's contract over sequences this module builds, not chosen input.

test_store.py names an input and the output it expects. This module instead runs random
sequences of write/remove/expire_at/lookup/take_effects/sample_and_expire over a small
key alphabet and checks, after every single operation, that the structures a `Store`
keeps still agree: `check_invariants()` holds, every key in `_expiry` is in `_data`,
every key in the sampling index maps back to its own slot, every value in `_data` is a
kind `kind_of` recognizes, every queued effect is exactly `[b"DEL", <bytes>]`, and any
key a `lookup` returned non-`None` for is still in `_data`. Two further tests assert the
corpus is wide enough to matter: one round holds a `bytes` and a `deque` value at once,
and one moves a key out of a middle sampling slot, so neither is vacuously true.

The corpus is built from a fixed seed, matching test_resp_properties.py's shape (there is
no `hypothesis` dependency in this project and none is added here). A failure names the
round, the step, and the sequence up to and including the step that broke the property.
"""

import random
from collections import deque

from store import Store

SEED = 20260821
ROUNDS = 500

KEYS = [b"a", b"b", b"c"]
# two shapes of each kind: bytes empty/non-empty as before, and now a deque empty/non-empty
# alongside them, so a write can land either kind and the corpus can reach a keyspace that
# holds both at once
VALUES = [b"", b"v", b"value", b"\x00\xff", deque(), deque([b"v"])]
# both sides of "now", and exactly on it -- so lookup's expiry branch and its live branch
# both fire, and the boundary itself (a deadline equal to now_ms() is expired) gets exercised
DEADLINE_OFFSETS = [-10_000, -1, 0, 1, 10_000]

OPS = ["write", "remove", "expire_at", "lookup", "take_effects", "sample_and_expire"]


def _random_sequence(rnd):
    # expire_at's deadline is an offset, resolved against now_ms() at replay time rather
    # than build time, so a slow test run can't make a "future" deadline arrive stale
    sequence = []
    for _ in range(rnd.randint(10, 60)):
        key = rnd.choice(KEYS)
        op = rnd.choice(OPS)
        if op == "write":
            sequence.append((op, key, rnd.choice(VALUES), rnd.choice([True, False])))
        elif op == "expire_at":
            sequence.append((op, key, rnd.choice(DEADLINE_OFFSETS)))
        else:
            sequence.append((op, key))
    return sequence


def _apply(store, op):
    # returns lookup's result so the caller can check its one extra property; every other
    # op returns None, which never collides with a real lookup result because the caller
    # only inspects the return value when op[0] == "lookup"
    name, key = op[0], op[1]
    if name == "write":
        _, _, value, keep_ttl = op
        store.write(key, value, keep_ttl=keep_ttl)
    elif name == "remove":
        store.remove(key)
    elif name == "expire_at":
        _, _, offset = op
        try:
            store.expire_at(key, store.now_ms() + offset)
        except KeyError:
            pass  # expire_at's own refusal of a key that isn't there, not a bug
    elif name == "lookup":
        # kind is never passed here -- a mismatched-kind lookup is test_store.py's own
        # chosen-input branch to cover, not something a random sequence needs to reach
        return store.lookup(key)
    elif name == "take_effects":
        store.take_effects()
    elif name == "sample_and_expire":
        # the key _random_sequence attached to this op is unused: a sample draws from
        # the whole index rather than acting on the one key every other op takes, the
        # same shape "take_effects" already has above. the count passed is the size of
        # the whole key alphabet, never smaller than the index could possibly be, which
        # keeps this landing on sample_and_expire's own population clamp whenever fewer
        # than all three keys currently carry a deadline
        store.sample_and_expire(len(KEYS))
    return None


def _assert_invariants_hold(store, op, result):
    store.check_invariants()
    for key in store._expiry:
        assert key in store._data
    for slot, key in enumerate(store._expiry_keys):
        assert store._expiry_slots.get(key) == slot
    for value in store._data.values():
        # delegates to the store's own definition of a recognized kind rather than
        # re-listing types here, so this stays honest as a third kind arrives: kind_of()
        # raises for anything it does not recognize, which is this invariant's failure
        store.kind_of(value)
    for effect in store._effects:
        assert len(effect) == 2 and effect[0] == b"DEL" and type(effect[1]) is bytes, effect
    if op[0] == "lookup" and result is not None:
        assert op[1] in store._data


def test_random_operation_sequences_preserve_the_stores_invariants():
    rnd = random.Random(SEED)
    for round_index in range(ROUNDS):
        store = Store()
        sequence = _random_sequence(rnd)
        for step, op in enumerate(sequence):
            result = _apply(store, op)
            try:
                _assert_invariants_hold(store, op, result)
            except AssertionError as exc:
                raise AssertionError(
                    "round %d step %d %r broke an invariant: %s\nsequence so far: %r"
                    % (round_index, step, op, exc, sequence[: step + 1])
                ) from exc


def test_random_operation_sequences_reach_a_mixed_kind_keyspace():
    # VALUES carries both bytes and deque shapes. Without this, widening it is a change a
    # green suite can't tell apart from a no-op: every invariant above holds just as well
    # over a keyspace that happens to never mix kinds as it does over one that does
    rnd = random.Random(SEED)
    mixed_rounds = 0
    for _ in range(ROUNDS):
        store = Store()
        for op in _random_sequence(rnd):
            _apply(store, op)
            kinds = {type(value) for value in store._data.values()}
            if bytes in kinds and deque in kinds:
                mixed_rounds += 1
                break
    assert mixed_rounds > 0, "no round ever held a bytes value and a list value at once"


def test_random_operation_sequences_move_a_key_out_of_a_middle_sampling_slot():
    # exercised only when the removed key sits in a middle slot and the index holds more
    # than one key -- a corpus that never grows the index past one entry would leave the
    # swap-delete's relocation branch untested while every invariant above still holds
    rnd = random.Random(SEED)
    moved_rounds = 0
    for _ in range(ROUNDS):
        store = Store()
        for op in _random_sequence(rnd):
            before_slots = dict(store._expiry_slots)
            _apply(store, op)
            moved = [key for key, slot in before_slots.items()
                     if key in store._expiry_slots and store._expiry_slots[key] != slot]
            if moved:
                moved_rounds += 1
                break
    assert moved_rounds > 0, "no round ever moved a key out of a middle sampling slot"
