"""String and key commands: SET, GET, DEL, EXISTS, TYPE, EXPIRE, PEXPIRE, PEXPIREAT, TTL, PTTL, INCR, DECR."""

from commands.registry import (
    INT64_MAX,
    INT64_MIN,
    Kind,
    NOT_AN_INTEGER,
    Reply,
    SYNTAX_ERROR,
    command,
    parse_int64,
)
import resp
from store import KIND_STRING

INVALID_EXPIRE_TIME = b"ERR invalid expire time in 'set' command"

# a repeat of one token is ordinary and a different member of the same family is a syntax error
# at once: staged and validated instead, it accepts "EX 10 PX 20000 EX 30", which Redis refuses
_TTL_TOKENS = (b"EX", b"PX", b"EXAT", b"PXAT")
_KEEPTTL_TOKEN = b"KEEPTTL"
_CONDITION_TOKENS = (b"NX", b"XX")
_GET_TOKEN = b"GET"

# how each deadline token turns its argument into absolute Unix milliseconds. EX and PX
# are durations from now; EXAT and PXAT are already absolute, which is the whole difference
_TTL_SCALE = {b"EX": 1000, b"PX": 1, b"EXAT": 1000, b"PXAT": 1}
_TTL_IS_ABSOLUTE = {b"EX": False, b"PX": False, b"EXAT": True, b"PXAT": True}


@command(b"SET", arity=-3, kind=Kind.WRITE)
def set_(store, conn, argv: list[bytes]) -> Reply:
    key, value = argv[1], argv[2]

    ttl_kind = None
    ttl_field = None
    condition = None
    want_old = False

    i = 3
    while i < len(argv):
        token = argv[i].upper()
        if token in _TTL_TOKENS:
            if ttl_kind is not None and ttl_kind != token:
                return resp.encode_error(SYNTAX_ERROR), []
            if i + 1 >= len(argv):
                return resp.encode_error(SYNTAX_ERROR), []
            # kept, not parsed: the reference validates only the occurrence that survives, so parsing
            # here rejects "EX abc EX 10", which it answers +OK
            ttl_kind, ttl_field = token, argv[i + 1]
            i += 2
        elif token == _KEEPTTL_TOKEN:
            # same family as the four above, so meeting one of them here is the same
            # error -- it just has no argument to carry
            if ttl_kind is not None and ttl_kind != token:
                return resp.encode_error(SYNTAX_ERROR), []
            ttl_kind = token
            i += 1
        elif token in _CONDITION_TOKENS:
            if condition is not None and condition != token:
                return resp.encode_error(SYNTAX_ERROR), []
            condition = token
            i += 1
        elif token == _GET_TOKEN:
            # a family of one, so a repeat is ordinary and there is nothing to conflict with
            want_old = True
            i += 1
        else:
            return resp.encode_error(SYNTAX_ERROR), []

    deadline = None
    keep_ttl = ttl_kind == _KEEPTTL_TOKEN
    if ttl_kind is not None and not keep_ttl:
        ttl_amount = parse_int64(ttl_field)
        if ttl_amount is None:
            return resp.encode_error(NOT_AN_INTEGER), []
        millis = ttl_amount * _TTL_SCALE[ttl_kind]
        # the reference bounds the argument at zero either way, which is why EXAT 0 is an error
        # and PXAT 1 is accepted and expires at once: the check is on what was asked for
        deadline = millis if _TTL_IS_ABSOLUTE[ttl_kind] else store.now_ms() + millis
        if ttl_amount <= 0 or not (INT64_MIN <= deadline <= INT64_MAX):
            return resp.encode_error(INVALID_EXPIRE_TIME), []

    # the expiry-aware lookup, never a raw dict test: a logically-expired key must not block NX.
    # read before anything is written, and with a kind only when GET owes the caller the old value
    previous = store.lookup(key, kind=KIND_STRING) if want_old else store.lookup(key)

    # GET reports what was there whether or not the condition let the write happen, so
    # its reply is decided here and the condition only decides whether to write
    refused = (condition == b"NX" and previous is not None) or (
        condition == b"XX" and previous is None)
    if refused:
        return (resp.encode_bulk_string(previous) if want_old
                else resp.encode_bulk_string(None)), []

    # keep_ttl is False unless KEEPTTL asked: a plain SET discards whatever TTL was there, and a
    # deadline resolved above is re-applied just after and cannot be combined with KEEPTTL
    store.write(key, value, keep_ttl=keep_ttl)
    # KEEPTTL is carried into the effect: without it a follower replays a plain SET, whose
    # keep_ttl=False clears the very deadline this command was written to preserve
    effects = [[b"SET", key, value] + ([_KEEPTTL_TOKEN] if keep_ttl else [])]
    if deadline is not None:
        if deadline <= store.now_ms():
            # a past deadline deletes here, so the DEL rides with the command: a follower may not
            # remove a key on its own. EX/PX reach this too -- 6 of 20,000 `SET k v PX 1` calls did
            store.remove(key)
            # the DEL alone, not a SET/DEL pair: the follower needs the end state, and
            # store.remove's own comment gives the reason a redundant pair is worse
            return (resp.encode_bulk_string(previous) if want_old
                    else resp.encode_simple_string(b"OK")), [[b"DEL", key]]
        store.expire_at(key, deadline)
        # the unconditional form, NX/XX dropped: the leader evaluated the condition, and a
        # follower re-evaluating it would depend on its own notion of existence
        effects.append([b"PEXPIREAT", key, b"%d" % deadline])
    if want_old:
        return resp.encode_bulk_string(previous), effects
    return resp.encode_simple_string(b"OK"), effects


@command(b"GET", arity=2, kind=Kind.READ)
def get(store, conn, argv: list[bytes]) -> Reply:
    # one of nine commands that pass a kind unconditionally; DEL, EXISTS and TYPE stay
    # type-agnostic, on a real server and here
    value = store.lookup(argv[1], kind=KIND_STRING)
    return resp.encode_bulk_string(value), []


@command(b"DEL", arity=-2, kind=Kind.WRITE)
def del_(store, conn, argv: list[bytes]) -> Reply:
    # asks through the lookup first, so a logically-expired key -- or one this same call
    # already removed -- reports as already gone rather than as one more key removed
    removed = []
    for key in argv[1:]:
        if store.lookup(key) is not None:
            store.remove(key)
            removed.append(key)
    if not removed:
        return resp.encode_integer(0), []
    return resp.encode_integer(len(removed)), [[b"DEL"] + removed]


@command(b"EXISTS", arity=-2, kind=Kind.READ)
def exists(store, conn, argv: list[bytes]) -> Reply:
    count = sum(1 for key in argv[1:] if store.lookup(key) is not None)
    return resp.encode_integer(count), []


@command(b"TYPE", arity=2, kind=Kind.READ)
def type_(store, conn, argv: list[bytes]) -> Reply:
    value = store.lookup(argv[1])
    if value is None:
        return resp.encode_simple_string(b"none"), []
    return resp.encode_simple_string(store.kind_of(value)), []


# real Redis 7 takes NX/XX/GT/LT on this family and this server does not, so arity is the exact
# 3: a minimum would accept a trailing option and silently ignore it
INVALID_EXPIRE_TIME_EXPIRE = b"ERR invalid expire time in 'expire' command"
INVALID_EXPIRE_TIME_PEXPIRE = b"ERR invalid expire time in 'pexpire' command"
INCR_DECR_OVERFLOW = b"ERR increment or decrement would overflow"

# EXPIRE's bound on its seconds argument, before the multiply: the reference truncates toward
# zero with C semantics, so its own negative bound is exactly -this
MAX_EXPIRE_SECONDS = INT64_MAX // 1000


def _apply_expiry(store, key: bytes, deadline_ms: int) -> Reply:
    # one place for all three, so they cannot drift. a deadline at or before now deletes here, so
    # the DEL rides with this command: a follower may not remove a key on its own initiative
    if store.lookup(key) is None:
        return resp.encode_integer(0), []
    if deadline_ms <= store.now_ms():
        store.remove(key)
        return resp.encode_integer(1), [[b"DEL", key]]
    store.expire_at(key, deadline_ms)
    return resp.encode_integer(1), [[b"PEXPIREAT", key, b"%d" % deadline_ms]]


@command(b"EXPIRE", arity=3, kind=Kind.WRITE)
def expire(store, conn, argv: list[bytes]) -> Reply:
    key = argv[1]
    seconds = parse_int64(argv[2])
    if seconds is None:
        return resp.encode_error(NOT_AN_INTEGER), []
    # rejected before the multiply: a seconds value outside this range carries the millisecond
    # figure past a 64-bit deadline, and a floor would accept the one value the reference refuses
    if not (-MAX_EXPIRE_SECONDS <= seconds <= MAX_EXPIRE_SECONDS):
        return resp.encode_error(INVALID_EXPIRE_TIME_EXPIRE), []
    deadline = store.now_ms() + seconds * 1000
    if deadline > INT64_MAX:
        return resp.encode_error(INVALID_EXPIRE_TIME_EXPIRE), []
    return _apply_expiry(store, key, deadline)


@command(b"PEXPIRE", arity=3, kind=Kind.WRITE)
def pexpire(store, conn, argv: list[bytes]) -> Reply:
    key = argv[1]
    millis = parse_int64(argv[2])
    if millis is None:
        return resp.encode_error(NOT_AN_INTEGER), []
    deadline = store.now_ms() + millis
    if deadline > INT64_MAX:
        return resp.encode_error(INVALID_EXPIRE_TIME_PEXPIRE), []
    return _apply_expiry(store, key, deadline)


@command(b"PEXPIREAT", arity=3, kind=Kind.WRITE)
def pexpireat(store, conn, argv: list[bytes]) -> Reply:
    key = argv[1]
    # already an absolute deadline, and parse_int64 has already bounded it to
    # INT64_MIN..INT64_MAX -- neither of EXPIRE's two overflow checks applies here
    deadline = parse_int64(argv[2])
    if deadline is None:
        return resp.encode_error(NOT_AN_INTEGER), []
    return _apply_expiry(store, key, deadline)


def _remaining_ms(store, key: bytes) -> int:
    # -2 missing -- through the lookup, so a logically-expired key counts as missing --
    # -1 present with no deadline, otherwise the remaining milliseconds
    if store.lookup(key) is None:
        return -2
    deadline = store.deadline(key)
    if deadline is None:
        return -1
    # clamped at zero, as the reference clamps: the clock is read twice, and -1 and -2 are a
    # vocabulary -- no deadline, no key -- that an unclamped remainder would speak by accident
    return max(0, deadline - store.now_ms())


@command(b"TTL", arity=2, kind=Kind.READ)
def ttl(store, conn, argv: list[bytes]) -> Reply:
    remaining = _remaining_ms(store, argv[1])
    if remaining < 0:
        return resp.encode_integer(remaining), []
    # rounds to the nearest second rather than truncating -- measured against
    # redis-server 7.2.7, which rounds: PX 1600 reads back as TTL 2, not 1
    return resp.encode_integer((remaining + 500) // 1000), []


@command(b"PTTL", arity=2, kind=Kind.READ)
def pttl(store, conn, argv: list[bytes]) -> Reply:
    return resp.encode_integer(_remaining_ms(store, argv[1])), []


def _incr_by(store, argv: list[bytes], name: bytes, delta: int) -> Reply:
    key = argv[1]
    current = store.lookup(key, kind=KIND_STRING)
    if current is None:
        base = 0
    else:
        base = parse_int64(current)
        if base is None:
            return resp.encode_error(NOT_AN_INTEGER), []
    new_value = base + delta
    if not (INT64_MIN <= new_value <= INT64_MAX):
        return resp.encode_error(INCR_DECR_OVERFLOW), []
    # keep_ttl=True: INCR/DECR reinterpret the value in place, and clearing the TTL would leave
    # a key outliving a deadline the client set moments earlier
    store.write(key, b"%d" % new_value, keep_ttl=True)
    # the command verbatim, never a computed SET -- the pure effect "SET k <new>" would
    # tell a follower more than this command actually did
    return resp.encode_integer(new_value), [[name, key]]


@command(b"INCR", arity=2, kind=Kind.WRITE)
def incr(store, conn, argv: list[bytes]) -> Reply:
    return _incr_by(store, argv, b"INCR", 1)


@command(b"DECR", arity=2, kind=Kind.WRITE)
def decr(store, conn, argv: list[bytes]) -> Reply:
    return _incr_by(store, argv, b"DECR", -1)
