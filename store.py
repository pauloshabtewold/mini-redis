"""The keyspace, the expiry index, and the pending-effects queue."""

import random
import time
from collections import deque
from collections.abc import Iterator

KIND_STRING = b"string"
KIND_LIST = b"list"

# distinguishes "key not present" from any value the key could legitimately hold -- b""
# is a legal stored value and None never is, so testing the read-back value itself could
# never tell the two apart. A module-private object nothing outside this file can construct.
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
        # a uniform sample over the keys that carry a deadline has to draw from a
        # sequence, and materialising one from _expiry every tick costs O(n) per tick --
        # unaffordable at the sizes this runs against forever on an idle server. the list
        # and its index keep that draw O(1), at the cost of maintaining both on every
        # write to _expiry. the index earns its place alongside the list rather than
        # being dropped in favour of it alone -- without it, removing an arbitrary key
        # back out of the list means searching for it first, exactly the O(n) this pair
        # exists to avoid
        self._expiry_keys: list[bytes] = []
        self._expiry_slots: dict[bytes, int] = {}

    def now_ms(self) -> int:
        # wall clock, not monotonic: PEXPIREAT is Unix milliseconds by protocol, and a
        # deadline two machines can agree on has to be a timestamp both read the same way.
        # a monotonic clock here would store an offset from a per-process epoch instead,
        # and that corruption stays invisible until the process restarts and the offset
        # means nothing. a method rather than a module function so a test can freeze one
        # instance and every handler holding it sees the frozen clock.
        return int(time.time() * 1000)

    def _has_passed(self, deadline: int | None, now: int) -> bool:
        # the one comparison every expiry check in this file makes: a deadline equal to
        # now has already passed, and no deadline at all never has. Four call sites is
        # four chances for the polarity or the None case to drift apart from the other
        # three; one predicate leaves exactly one place to get the boundary right and
        # exactly one place to change it. now arrives as an argument rather than a call
        # to now_ms() so a caller checking many keys still reads the clock once
        return deadline is not None and deadline <= now

    def lookup(self, key: bytes, kind: bytes | None = None) -> object | None:
        # the one place any handler -- read or write -- may ask whether a key exists.
        # replication will fork this on a delete_expired flag so a follower can answer nil
        # for an expired key without deleting it; a raw `key in self._data` written
        # anywhere else would make a follower diverge from its leader on the same input,
        # with no error to show for it
        value = self._data.get(key, _MISSING)
        if value is _MISSING:
            return None
        deadline = self._expiry.get(key)
        if self._has_passed(deadline, self.now_ms()):
            # the DEL this produces goes on the queue rather than back to the caller: its
            # other producer is the active expiry sweep, running outside dispatch
            # entirely, after select() returns, with no command and no handler to return
            # one from
            self.remove(key)
            self._effects.append([b"DEL", key])
            return None
        if kind is not None and self.kind_of(value) != kind:
            raise WrongTypeError()
        return value

    def _drop_deadline(self, key: bytes) -> None:
        # tested against None rather than for truth: a deadline of 0 is a legal past
        # deadline and falsy, and `if not self._expiry.pop(key, None):` reads it as
        # absent and leaves the sampling index holding a key with no deadline
        if self._expiry.pop(key, None) is None:
            return
        slot = self._expiry_slots.pop(key)
        last_key = self._expiry_keys.pop()
        # moving the list's last entry into the vacated slot avoids shifting everything
        # after it, which is what keeps this O(1) rather than O(n). skipped when key is
        # itself the last entry: last_key then equals key, and slot is now one past the
        # end of the list the pop above already shortened, so running the two lines
        # below would raise IndexError: list assignment index out of range rather than
        # anything more subtle
        if last_key != key:
            self._expiry_keys[slot] = last_key
            self._expiry_slots[last_key] = slot

    def write(self, key: bytes, value: object, *, keep_ttl: bool) -> None:
        # keyword-only and undefaulted because its two callers disagree on the answer:
        # plain SET discards whatever TTL was there, INCR preserves it, and a default that
        # picked wrong for either one is a key vanishing seconds after a routine refresh
        self._data[key] = value
        if not keep_ttl:
            self._drop_deadline(key)

    def remove(self, key: bytes) -> bool:
        # the only thing that may delete from _data or _expiry -- DEL's own primitive, and
        # the empty-container cleanup a container type will need. it feeds the effects
        # queue nothing: an append here would make the last LPOP on a list emit a redundant
        # LPOP/DEL pair instead of the LPOP alone
        present = key in self._data
        self._data.pop(key, None)
        self._drop_deadline(key)
        return present

    def expire_at(self, key: bytes, deadline_ms: int) -> None:
        # refuses a key that is not in _data: a deadline kept for a missing key is the same
        # corruption check_invariants watches for, arriving from the other direction
        if key not in self._data:
            raise KeyError(key)
        # only when the key has no slot yet: appending unconditionally on a re-set TTL
        # would leave _expiry_keys holding the same key twice with _expiry_slots pointing
        # at only one of the two, which _drop_deadline's swap-delete has no way to unwind
        if key not in self._expiry:
            # the length read here is the key's future index, taken before the append
            # below changes it -- computing it the other way round would record the key
            # one slot past where it actually lands
            self._expiry_slots[key] = len(self._expiry_keys)
            self._expiry_keys.append(key)
        self._expiry[key] = deadline_ms

    def sample_and_expire(self, count: int) -> tuple[int, int]:
        # draws without replacement over the maintained index rather than materialising
        # one from _expiry -- see the sampling index's own comment in __init__. count is
        # capped at the population since random.sample refuses a sample larger than the
        # sequence it draws from -- a caller sweeping with one fixed batch size must not
        # crash merely because a given tick finds fewer keys carrying a deadline than
        # that. one now_ms() read for the whole draw, not one per key, so a caller
        # checking many keys does not spend its budget re-reading a clock that has not moved
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
        # bytes is a string, deque is a list -- the raise below is a TypeError rather than
        # a WrongTypeError deliberately -- it means the store holds something no command can
        # put there, which is a defect in this file and not a client's mistake, so it travels
        # to the connection boundary rather than back to the client as a reply. dispatch()
        # catches WrongTypeError only. Whichever kind arrives third adds its branch here in
        # the same commit as the code that can create it, or every lookup(kind=) on it takes
        # this branch and closes a connection instead of answering WRONGTYPE
        if isinstance(value, bytes):
            return KIND_STRING
        if isinstance(value, deque):
            return KIND_LIST
        raise TypeError("not a value kind this store recognizes: %r" % (type(value),))

    def live_count(self) -> int:
        # DBSIZE reads this rather than counting through lookup(), which would delete each
        # expired key it scanned past and append a DEL to the effects queue for it -- a
        # count turning into a mutation with no write anyone asked for. len(_data) is O(1);
        # the correction below is linear in the expiry index rather than in the keyspace,
        # so a keyspace with few TTLs stays cheap no matter how large it grows
        now = self.now_ms()
        expired = sum(1 for deadline in self._expiry.values() if self._has_passed(deadline, now))
        return len(self._data) - expired

    def expiring_count(self) -> int:
        # INFO's expires= field -- the number of deadlines in _expiry that have not yet
        # passed. A separate method rather than a second return value alongside
        # live_count(), so a caller wanting only one of the two numbers does not have to
        # compute or discard the other
        now = self.now_ms()
        return sum(1 for deadline in self._expiry.values() if not self._has_passed(deadline, now))

    def live_keys(self) -> Iterator[bytes]:
        # KEYS reads this rather than lookup() per key, for the same reason live_count()
        # does -- a read command must not delete what it was only asked to list. _data's
        # own iteration order, since nothing here claims a stronger one
        now = self.now_ms()
        for key in self._data:
            deadline = self._expiry.get(key)
            if not self._has_passed(deadline, now):
                yield key

    def snapshot_items(self) -> Iterator[tuple[bytes, bytes, object, int]]:
        # walks _data directly rather than live_keys(): the same deliberate lookup()
        # bypass live_count() and live_keys() already are, for the opposite reason --
        # those hide an expired key from a client asking what exists, and a save doing
        # the same would lose data a restart could still legitimately expire. removes
        # nothing and queues nothing, so a save never mutates the store it is reading
        for key, value in self._data.items():
            # -1 marks "no deadline" here rather than None, since the field is a plain
            # int and every real deadline this store ever holds is a non-negative Unix-ms
            # timestamp. 0 could not serve that role: it is itself a legal, already-past
            # deadline
            deadline = self._expiry.get(key, -1)
            yield key, self.kind_of(value), value, deadline

    @classmethod
    def from_items(cls, items: Iterator[tuple[bytes, bytes, object, int]]) -> "Store":
        # a classmethod rather than a function returning Store(): FrozenStore.from_items(...)
        # must return a FrozenStore, or a frozen-clock caller is handed a wall-clock store
        # and a deadline test that passes for the wrong reason
        store = cls()
        loaded = 0
        for key, kind, value, expiry in items:
            loaded += 1
            if kind == KIND_STRING:
                store.write(key, value, keep_ttl=False)
            elif kind == KIND_LIST:
                # wrapped in deque regardless of what container the source actually used:
                # kind_of() only recognizes bytes and deque, so a plain list surviving
                # from here would make every later list command against this key raise a
                # bare TypeError instead of the WrongTypeError dispatch catches, closing
                # the connection rather than answering it
                store.write(key, deque(value), keep_ttl=False)
            else:
                raise ValueError("not a snapshot kind this store recognizes: %r" % (kind,))
            if expiry != -1:
                store.expire_at(key, expiry)
        # two items naming one key would leave the second one's value and deadline in the
        # keyspace and the first one's nowhere, with nothing said. counted rather than
        # watched per key: a membership test on every write costs every load a snapshot
        # with no repeat in it pays for nothing, where the keyspace this just built
        # already knows how many distinct keys arrived. which key it was is not named
        # here, because items may be an iterator this has already consumed -- the caller
        # holding the entries is the one that can still find it
        if len(store._data) != loaded:
            raise DuplicateKeyError(
                "two of the %d items share one key: they produced %d keys"
                % (loaded, len(store._data)))
        return store

    def flush(self) -> int:
        # every key, expired or not -- FLUSHALL empties the keyspace rather than reporting
        # on what was still live in it, which is why this walks _data directly instead of
        # live_keys(). list(self._data) is a snapshot taken before the loop starts, not a
        # live view of it: remove() mutates _data on every iteration, and iterating the
        # dict itself while deleting from it raises RuntimeError
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
        # this size comparison catches what the per-key loop below cannot: a key present
        # in _expiry but missing from _expiry_keys would never be visited by that loop,
        # since it only walks the sampling index and has nothing there to compare against
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
