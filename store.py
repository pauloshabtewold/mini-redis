"""The keyspace, the expiry index, and the pending-effects queue."""

import time

KIND_STRING = b"string"

# distinguishes "key not present" from any value the key could legitimately hold -- b""
# is a legal stored value and None never is, so testing the read-back value itself could
# never tell the two apart. A module-private object nothing outside this file can construct.
_MISSING = object()


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

    def now_ms(self) -> int:
        # wall clock, not monotonic: PEXPIREAT is Unix milliseconds by protocol, and a
        # deadline two machines can agree on has to be a timestamp both read the same way.
        # a monotonic clock here would store an offset from a per-process epoch instead,
        # and that corruption stays invisible until the process restarts and the offset
        # means nothing. a method rather than a module function so a test can freeze one
        # instance and every handler holding it sees the frozen clock.
        return int(time.time() * 1000)

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
        if deadline is not None and deadline <= self.now_ms():
            # the DEL this produces goes on the queue rather than back to the caller: its
            # other producer will be the active expiry sweep, running outside dispatch
            # entirely, after select() returns, with no command and no handler to return
            # one from
            self.remove(key)
            self._effects.append([b"DEL", key])
            return None
        if kind is not None and self.kind_of(value) != kind:
            raise WrongTypeError()
        return value

    def write(self, key: bytes, value: object, *, keep_ttl: bool) -> None:
        # keyword-only and undefaulted because its two callers disagree on the answer:
        # plain SET discards whatever TTL was there, INCR preserves it, and a default that
        # picked wrong for either one is a key vanishing seconds after a routine refresh
        self._data[key] = value
        if not keep_ttl:
            self._expiry.pop(key, None)

    def remove(self, key: bytes) -> bool:
        # the only thing that may delete from _data or _expiry -- DEL's own primitive, and
        # the empty-container cleanup a container type will need. it feeds the effects
        # queue nothing: an append here would make the last LPOP on a list emit a redundant
        # LPOP/DEL pair instead of the LPOP alone
        present = key in self._data
        self._data.pop(key, None)
        self._expiry.pop(key, None)
        return present

    def expire_at(self, key: bytes, deadline_ms: int) -> None:
        # refuses a key that is not in _data: a deadline kept for a missing key is the same
        # corruption check_invariants watches for, arriving from the other direction
        if key not in self._data:
            raise KeyError(key)
        self._expiry[key] = deadline_ms

    def deadline(self, key: bytes) -> int | None:
        # no expiry check here -- every caller has already been through lookup()
        return self._expiry.get(key)

    def kind_of(self, value: object) -> bytes:
        # one entry today; list support adds one
        if isinstance(value, bytes):
            return KIND_STRING
        raise TypeError("not a value kind this store recognizes: %r" % (type(value),))

    def take_effects(self) -> list[list[bytes]]:
        # returns the queue and clears it in the same step, so no consumer can drain it
        # twice and no second consumer is left starved by whichever one iterates first
        effects, self._effects = self._effects, []
        return effects

    def check_invariants(self) -> None:
        # an explicit raise, never `assert` -- `-O` strips assert statements from the
        # running process, and this is the only check that _expiry and _data still agree
        orphans = [key for key in self._expiry if key not in self._data]
        if orphans:
            raise AssertionError(
                "expiry index holds keys absent from the keyspace: %r" % (orphans,)
            )
