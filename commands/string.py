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

# SET's option families: EX/PX/EXAT/PXAT/KEEPTTL all name what happens to the deadline,
# NX/XX name a condition, GET stands alone. Within a family, a repeat of the same token is
# ordinary (last value wins); meeting a DIFFERENT member of the same family is a syntax
# error the moment it happens -- two independent checks over the token history, not a dict
# staged and validated once scanning finishes. Staged-and-validated is the natural-looking
# alternative and it is wrong: it accepts "EX 10 PX 20000 EX 30", which real Redis refuses
#
# KEEPTTL sits in the deadline family with the other four but takes no argument, which is
# why the family is two tuples rather than one: the exclusivity check spans both, the
# argument handling does not
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
            # the field is kept, not parsed: a repeat of the same token overrides this
            # one, and the reference validates only whichever occurrence survives. parsing
            # here rejects "EX abc EX 10", which the reference answers +OK. the overflow
            # check below was already deferred for this reason; the grammar check was not
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
        # an absolute deadline is the argument itself; a duration is measured from now.
        # the reference bounds the argument at zero either way, which is why EXAT 0 is an
        # error while PXAT 1 is accepted and expires immediately -- the check is on what
        # was asked for, not on where it lands
        deadline = millis if _TTL_IS_ABSOLUTE[ttl_kind] else store.now_ms() + millis
        if ttl_amount <= 0 or not (INT64_MIN <= deadline <= INT64_MAX):
            return resp.encode_error(INVALID_EXPIRE_TIME), []

    # the expiry-aware lookup, never a raw dict test: a logically-expired key must not
    # block NX, and the DEL that expiry produces reaches the store's own queue from
    # inside lookup(), never this handler's effects. read once, before anything is
    # written, because GET owes the caller the value this command replaced -- and the
    # kind is passed so GET answers WRONGTYPE on a key SET itself would overwrite happily
    previous = store.lookup(key, kind=KIND_STRING) if want_old else store.lookup(key)

    # GET reports what was there whether or not the condition let the write happen, so
    # its reply is decided here and the condition only decides whether to write
    refused = (condition == b"NX" and previous is not None) or (
        condition == b"XX" and previous is None)
    if refused:
        return (resp.encode_bulk_string(previous) if want_old
                else resp.encode_bulk_string(None)), []

    # keep_ttl is False unless KEEPTTL asked otherwise: a plain SET discards whatever TTL
    # was there, and this is the only place that decides -- a deadline resolved above is
    # re-applied right after, and cannot be combined with KEEPTTL in the first place
    store.write(key, value, keep_ttl=keep_ttl)
    # KEEPTTL is carried into the effect rather than dropped. Without it a follower
    # replays a plain SET, whose own keep_ttl=False clears the deadline this command was
    # written to preserve, and the two ends disagree about when the key dies with nothing
    # to notice. _incr_by below re-emits its own command name for the same reason
    effects = [[b"SET", key, value] + ([_KEEPTTL_TOKEN] if keep_ttl else [])]
    if deadline is not None:
        if deadline <= store.now_ms():
            # a deadline already past deletes here rather than being written into the
            # index, for the reason EXPIRE's own path deletes: lazy expiry runs only when
            # something looks the key up again, and with no sweep nothing guarantees that
            # ever happens. EXAT and PXAT reach this from a valid argument by naming a past
            # instant outright, and EX and PX reach it too, rarely: the deadline is computed
            # from one clock read and compared against a second taken after lookup() and
            # write(), so a small enough relative TTL can be overtaken in between -- measured,
            # 6 of 20,000 back-to-back `SET k v PX 1` calls took this branch with nothing
            # injected. Either way it is the same key nothing would otherwise collect
            store.remove(key)
            # the DEL alone, not a SET/DEL pair: the follower needs the end state, and
            # store.remove's own comment gives the reason a redundant pair is worse
            return (resp.encode_bulk_string(previous) if want_old
                    else resp.encode_simple_string(b"OK")), [[b"DEL", key]]
        store.expire_at(key, deadline)
        # the unconditional form, with NX/XX dropped: the leader already evaluated the
        # condition, and re-evaluating it on a follower would make correctness depend on
        # the follower's own notion of existence -- which its expiry rule deliberately
        # makes differ from the leader's
        effects.append([b"PEXPIREAT", key, b"%d" % deadline])
    if want_old:
        return resp.encode_bulk_string(previous), effects
    return resp.encode_simple_string(b"OK"), effects


@command(b"GET", arity=2, kind=Kind.READ)
def get(store, conn, argv: list[bytes]) -> Reply:
    # one of nine commands that unconditionally pass a kind -- INCR and DECR here, and
    # all six list commands in list.py. SET passes one too, but only when its GET option
    # is present, since only then does it need to reject a value it could not return as
    # the old string; DEL, EXISTS and TYPE stay genuinely type-agnostic, on a real server
    # and here
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


# EXPIRE, PEXPIRE and PEXPIREAT: real Redis 7 takes NX/XX/GT/LT on this family and this
# server does not, so arity is the exact 3 rather than a minimum -- a minimum would accept
# a trailing option and silently ignore it instead of raising
INVALID_EXPIRE_TIME_EXPIRE = b"ERR invalid expire time in 'expire' command"
INVALID_EXPIRE_TIME_PEXPIRE = b"ERR invalid expire time in 'pexpire' command"
INCR_DECR_OVERFLOW = b"ERR increment or decrement would overflow"

# EXPIRE's bound on its seconds argument, before the multiply to milliseconds. INT64_MAX
# // 1000 in both directions: the reference divides with C semantics, which truncate
# toward zero, so its own negative bound is exactly -this
MAX_EXPIRE_SECONDS = INT64_MAX // 1000


def _apply_expiry(store, key: bytes, deadline_ms: int) -> Reply:
    # the one place EXPIRE, PEXPIRE and PEXPIREAT turn a resolved absolute deadline into
    # a reply and an effect, so the three cannot drift apart. a deadline at or before now
    # deletes the key here, in the handler, rather than being written into the index and
    # left for the next lookup to clean up: a past-dated deadline on a follower would be
    # an entry the follower has no license to remove on its own initiative, and the leader
    # -- which already considers the key gone -- would never send a DEL to tell it to
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
    # rejected before the multiply, not after: a seconds value outside this range would
    # carry the millisecond figure past where a 64-bit deadline could hold it, even though
    # Python's own int never overflows and so never raises to catch this on its own.
    # symmetric rather than INT64_MIN // 1000, which floors where the reference truncates
    # -- one value apart, and on that one value a floor accepts what the reference refuses
    # and silently deletes the key instead of answering an error
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
    # clamped at zero, as the reference clamps, because the clock is read twice: lookup()
    # read it first and found this deadline still in the future, and the read below can
    # have crossed it. the two negative values here are a vocabulary -- -1 says "no
    # deadline" and -2 says "no key" -- so an unclamped remainder would not report a small
    # number, it would report one of those two facts about a key for which neither is true
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
    # keep_ttl=True: INCR/DECR reinterpret the value in place rather than replacing it the
    # way SET does, and clearing the TTL here would answer a deadline the client set
    # moments earlier with a key that now outlives it
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
