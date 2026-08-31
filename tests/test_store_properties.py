"""The keyspace's contract over sequences this module builds, not chosen input.

test_store.py names an input and the output it expects. This module instead runs random
sequences of write/remove/expire_at/lookup/take_effects over a small key alphabet and
checks, after every single operation, that the two structures a `Store` keeps still agree:
`check_invariants()` holds, every key in `_expiry` is in `_data`, every value in `_data`
is `bytes`, every queued effect is exactly `[b"DEL", <bytes>]`, and any key a `lookup`
returned non-`None` for is still in `_data`.

The corpus is built from a fixed seed, matching test_resp_properties.py's shape (there is
no `hypothesis` dependency in this project and none is added here). A failure names the
round, the step, and the sequence up to and including the step that broke the property.
"""

import random

from store import Store

SEED = 20260821
ROUNDS = 500

KEYS = [b"a", b"b", b"c"]
VALUES = [b"", b"v", b"value", b"\x00\xff"]
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
        # only one kind exists this feature, so kind is never passed here -- WrongTypeError
        # has no second kind to fire against yet; test_store.py covers that branch directly
        return store.lookup(key)
    elif name == "take_effects":
        store.take_effects()
    return None


def _assert_invariants_hold(store, op, result):
    store.check_invariants()
    for key in store._expiry:
        assert key in store._data
    for value in store._data.values():
        assert type(value) is bytes
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
