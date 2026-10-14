"""The keyspace, the expiry index, and the pending-effects queue."""

import random
import time
from collections import deque
from collections.abc import Iterator

KIND_STRING = b"string"
KIND_LIST = b"list"

# b"" is a legal stored value and None never is, so the read-back value alone cannot tell
# "key not present" from a value; module-private, so nothing outside can construct one
_MISSING = object()


class DuplicateKeyError(ValueError):
    """Two items handed to `from_items()` named one key.

    Its own type, and not a bare `ValueError`, because `persistence._decode()` is the one
    caller that can say which key it was -- it still holds the entries -- and it has to
    know that a repeat is what it caught. Deciding by re-scanning instead named a repeat
    as the cause of a refusal an unrecognised kind byte had already raised.
    """


class WrongTypeError(Exception):
    """A key holds a value of a kind the caller did not ask for.

    message is the RESP error body, carried as `bytes` rather than `str` because it goes
    on the wire unchanged -- the same shape `resp.ProtocolError` uses, so the reply path
    formats this rather than inventing its own text.

    A plain class attribute, not an `__init__` parameter: `WrongTypeError()` takes no
    argument, and every instance reads the identical bytes however it was raised.
    """

    # measured against redis-server 7.2.7, byte for byte
    message = b"WRONGTYPE Operation against a key holding the wrong kind of value"


class Store:
    def __init__(self) -> None:
        self._data: dict[bytes, object] = {}
        self._expiry: dict[bytes, int] = {}
        self._effects: list[list[bytes]] = []
        # a uniform sample needs a sequence, and materialising one from _expiry is O(n) per tick
        # forever on an idle server; the index is what keeps removing a key out of the list O(1)
        self._expiry_keys: list[bytes] = []
        self._expiry_slots: dict[bytes, int] = {}

    def now_ms(self) -> int:
        # wall clock, not monotonic: PEXPIREAT is Unix ms by protocol, and a monotonic offset from
        # a per-process epoch means nothing after a restart. a method, so a test can freeze one
        return int(time.time() * 1000)

    def _has_passed(self, deadline: int | None, now: int) -> bool:
        # one predicate for four call sites, so the polarity and the None case cannot drift: a
        # deadline equal to now has passed. now is an argument so many keys read the clock once
        return deadline is not None and deadline <= now

    def lookup(self, key: bytes, kind: bytes | None = None) -> object | None:
        # the one place any handler may ask whether a key exists: a raw `key in self._data`
        # elsewhere would let a follower diverge from its leader with nothing said
        value = self._data.get(key, _MISSING)
        if value is _MISSING:
            return None
        deadline = self._expiry.get(key)
        if self._has_passed(deadline, self.now_ms()):
            # on the queue rather than back to the caller: the other producer is the expiry
            # sweep, which runs after select() with no command and no handler to return from
            self.remove(key)
            self._effects.append([b"DEL", key])
            return None
        if kind is not None and self.kind_of(value) != kind:
            raise WrongTypeError()
        return value

    def _drop_deadline(self, key: bytes) -> None:
        # against None, not for truth: a deadline of 0 is a legal past deadline and falsy, and
        # reading it as absent leaves the sampling index holding a key with no deadline
        if self._expiry.pop(key, None) is None:
            return
        slot = self._expiry_slots.pop(key)
        last_key = self._expiry_keys.pop()
        # the last entry fills the vacated slot rather than shifting everything after it, which
        # is what keeps this O(1); skipped for the last entry itself, whose slot is past the end
        if last_key != key:
            self._expiry_keys[slot] = last_key
            self._expiry_slots[last_key] = slot

    def write(self, key: bytes, value: object, *, keep_ttl: bool) -> None:
        # keyword-only and undefaulted: SET discards the TTL where INCR keeps it, and a default
        # wrong for either is a key vanishing seconds after a routine refresh
        self._data[key] = value
        if not keep_ttl:
            self._drop_deadline(key)

    def remove(self, key: bytes) -> bool:
        # the only thing that may delete from _data or _expiry, and it queues no effect: an
        # append here would make the last LPOP on a list emit a redundant LPOP/DEL pair
        present = key in self._data
        self._data.pop(key, None)
        self._drop_deadline(key)
        return present

    def expire_at(self, key: bytes, deadline_ms: int) -> None:
        # refuses a key that is not in _data: a deadline kept for a missing key is the same
        # corruption check_invariants watches for, arriving from the other direction
        if key not in self._data:
            raise KeyError(key)
        # only when the key has no slot: appending on a re-set TTL leaves the key in the list
        # twice with one slot, which _drop_deadline's swap-delete cannot unwind
        if key not in self._expiry:
            # the length is the key's future index, read before the append below changes it
            self._expiry_slots[key] = len(self._expiry_keys)
            self._expiry_keys.append(key)
        self._expiry[key] = deadline_ms

    def sample_and_expire(self, count: int) -> tuple[int, int]:
        # without replacement over the maintained index, and count capped at the population since
        # random.sample refuses a larger sample and a fixed batch size must not crash a sweep
        sample = random.sample(self._expiry_keys, min(count, len(self._expiry_keys)))
        now = self.now_ms()
        expired = 0
        for key in sample:
            if self._has_passed(self._expiry.get(key), now):
                self.remove(key)
                self._effects.append([b"DEL", key])
                expired += 1
        return len(sample), expired

    def deadline(self, key: bytes) -> int | None:
        # no expiry check here -- every caller has already been through lookup()
        return self._expiry.get(key)

    def kind_of(self, value: object) -> bytes:
        # TypeError and not WrongTypeError deliberately: it means the store holds what no command
        # can put there, a defect here and not a client's, and dispatch() catches WrongTypeError
        if isinstance(value, bytes):
            return KIND_STRING
        if isinstance(value, deque):
            return KIND_LIST
        raise TypeError("not a value kind this store recognizes: %r" % (type(value),))

    def live_count(self) -> int:
        # not counted through lookup(), which would delete each expired key it passed and turn a
        # count into a mutation; the correction is linear in the expiry index, not the keyspace
        now = self.now_ms()
        expired = sum(1 for deadline in self._expiry.values() if self._has_passed(deadline, now))
        return len(self._data) - expired

    def expiring_count(self) -> int:
        # INFO's expires= field: a separate method and not a second return value beside
        # live_count(), so a caller wanting one of the two numbers does not compute the other
        now = self.now_ms()
        return sum(1 for deadline in self._expiry.values() if not self._has_passed(deadline, now))

    def live_keys(self) -> Iterator[bytes]:
        # not lookup() per key: a read command must not delete what it was asked to list.
        # _data's own iteration order, since nothing here claims a stronger one
        now = self.now_ms()
        for key in self._data:
            deadline = self._expiry.get(key)
            if not self._has_passed(deadline, now):
                yield key

    def snapshot_items(self) -> Iterator[tuple[bytes, bytes, object, int]]:
        # _data directly, not live_keys(): hiding an expired key here would lose data a restart
        # can still expire. removes and queues nothing, so a save never mutates what it reads
        for key, value in self._data.items():
            # -1 for "no deadline" because the field is a plain int; 0 could not serve, being
            # itself a legal already-past deadline
            deadline = self._expiry.get(key, -1)
            yield key, self.kind_of(value), value, deadline

    @classmethod
    def from_items(cls, items: Iterator[tuple[bytes, bytes, object, int]]) -> "Store":
        # a classmethod, not a function returning Store(): FrozenStore.from_items() must return a
        # FrozenStore, or a frozen-clock caller gets a deadline test passing for the wrong reason
        store = cls()
        loaded = 0
        for key, kind, value, expiry in items:
            loaded += 1
            if kind == KIND_STRING:
                store.write(key, value, keep_ttl=False)
            elif kind == KIND_LIST:
                # deque whatever the source used: kind_of() knows bytes and deque only, so a
                # plain list here raises TypeError later and closes the connection
                store.write(key, deque(value), keep_ttl=False)
            else:
                raise ValueError("not a snapshot kind this store recognizes: %r" % (kind,))
            if expiry != -1:
                store.expire_at(key, expiry)
        # two items naming one key would keep the second's value with nothing said; counted and
        # not watched per key, and unnamed, since items may be an iterator already consumed
        if len(store._data) != loaded:
            raise DuplicateKeyError(
                "two of the %d items share one key: they produced %d keys"
                % (loaded, len(store._data)))
        return store

    def flush(self) -> int:
        # every key, expired or not: FLUSHALL empties the keyspace rather than reporting what was
        # live in it. list() is taken before the loop, since deleting while iterating _data raises
        return sum(1 for key in list(self._data) if self.remove(key))

    def take_effects(self) -> list[list[bytes]]:
        # returns the queue and clears it in the same step, so no consumer can drain it
        # twice and no second consumer is left starved by whichever one iterates first
        effects, self._effects = self._effects, []
        return effects

    def check_invariants(self) -> None:
        """Raise unless the keyspace and the expiry index still agree.

        Nothing in the running server calls this. Its caller today is the test suite,
        which runs it after every step of a randomised operation sequence -- that is
        what it is for, and naming it here saves the next reader working out from the
        call graph that a check this file describes as the only one of its kind is not
        actually running anywhere in production.

        It is written as an explicit raise rather than an `assert` anyway, because the
        day something does call it in the running process, `-O` would strip an assert
        out from under it without changing a line of this file.
        """
        orphans = [key for key in self._expiry if key not in self._data]
        if orphans:
            raise AssertionError(
                "expiry index holds keys absent from the keyspace: %r" % (orphans,)
            )
        # the size comparison catches what the loop below cannot: a key in _expiry but missing
        # from _expiry_keys is never visited, since the loop walks the sampling index
        if (len(self._expiry_keys) != len(self._expiry)
                or len(self._expiry_slots) != len(self._expiry)):
            raise AssertionError(
                "the sampling index disagrees with _expiry on size: %d keys, %d slots, "
                "%d deadlines" % (len(self._expiry_keys), len(self._expiry_slots),
                                   len(self._expiry))
            )
        for slot, key in enumerate(self._expiry_keys):
            if key not in self._expiry:
                raise AssertionError(
                    "the sampling index holds a key with no deadline: %r" % (key,)
                )
            if self._expiry_slots.get(key) != slot:
                raise AssertionError(
                    "the sampling index disagrees with itself about %r's slot: "
                    "_expiry_keys has it at %d, _expiry_slots says %r"
                    % (key, slot, self._expiry_slots.get(key))
                )
