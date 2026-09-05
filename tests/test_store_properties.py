"""The keyspace's contract over sequences this module builds, not chosen input.

test_store.py names an input and the output it expects. This module instead runs random
sequences of write/remove/expire_at/lookup/take_effects over a small key alphabet and
checks, after every single operation, that the two structures a `Store` keeps still agree:
`check_invariants()` holds, every key in `_expiry` is in `_data`, every value in `_data`
is a kind `kind_of` recognizes, every queued effect is exactly `[b"DEL", <bytes>]`, and
any key a `lookup` returned non-`None` for is still in `_data`. A second test asserts the
corpus itself is wide enough to matter: at least one round holds both a `bytes` and a
`deque` value in `_data` at the same time, so the invariant above is actually exercised
across kinds and not just vacuously true over a single one.

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

OPS = ["write", "remove", "expire_at", "lookup", "take_effects"]


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
    return None


def _assert_invariants_hold(store, op, result):
    store.check_invariants()
    for key in store._expiry:
        assert key in store._data
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
