"""Connection and server commands: PING, ECHO, HELLO, DBSIZE, KEYS, FLUSHALL, INFO, CONFIG."""

import resource
import sys

from commands.registry import Kind, Reply, SYNTAX_ERROR, command, wrong_arity
import resp

# a constant and not read from installed metadata, so the reply does not need the project installed
SERVER_VERSION = b"0.1.0"

# named so a follower has one place to change
REPLICATION_ROLE = b"master"


@command(b"PING", arity=-1, kind=Kind.OTHER)
def ping(store, conn, argv: list[bytes]) -> Reply:
    # the arity integer expresses a minimum only, not a range, so the two-token upper bound is checked here
    if len(argv) > 2:
        return wrong_arity(b"PING"), []
    if len(argv) == 1:
        return resp.encode_simple_string(b"PONG"), []
    return resp.encode_bulk_string(argv[1]), []


@command(b"ECHO", arity=2, kind=Kind.OTHER)
def echo(store, conn, argv: list[bytes]) -> Reply:
    return resp.encode_bulk_string(argv[1]), []


@command(b"HELLO", arity=-1, kind=Kind.OTHER)
def hello(store, conn, argv: list[bytes]) -> Reply:
    # same minimum-only arity as ping, hence the explicit upper bound here too
    if len(argv) > 2:
        return wrong_arity(b"HELLO"), []
    if len(argv) == 2 and argv[1] != b"2":
        # every argument but b"2" answers NOPROTO, integer or not: a deliberate divergence from
        # real Redis's own parse error
        return resp.encode_error(b"NOPROTO unsupported protocol version"), []
    # RESP2 has no map type, so this is the flat field/value array a real server returns on RESP2
    return resp.encode_array([
        resp.encode_bulk_string(b"server"),
        resp.encode_bulk_string(b"redis"),
        resp.encode_bulk_string(b"version"),
        resp.encode_bulk_string(SERVER_VERSION),
        resp.encode_bulk_string(b"proto"),
        resp.encode_integer(2),
        resp.encode_bulk_string(b"id"),
        # 0 when dispatch is driven with no socket, the in-process convention: unguarded, the one
        # reply carrying a connection fact fails with an AttributeError instead
        resp.encode_integer(0 if conn is None else conn.id),
        resp.encode_bulk_string(b"mode"),
        resp.encode_bulk_string(b"standalone"),
        resp.encode_bulk_string(b"role"),
        # the replication role, not Connection.role -- the two vocabularies are disjoint
        resp.encode_bulk_string(REPLICATION_ROLE),
        resp.encode_bulk_string(b"modules"),
        resp.encode_array([]),
    ]), []


@command(b"DBSIZE", arity=1, kind=Kind.OTHER)
def dbsize(store, conn, argv: list[bytes]) -> Reply:
    # live_count() and not the shared lookup: routing a count through it would delete every expired
    # key it scanned past and queue a DEL for each, mutating the keyspace to answer a number
    return resp.encode_integer(store.live_count()), []


# the reference's own matcher and not fnmatch: this grammar negates a class with '^' where fnmatch
# takes '!', so a complement would come back as exactly the set it meant to exclude
_STAR = ord("*")
_QUESTION = ord("?")
_LBRACKET = ord("[")
_RBRACKET = ord("]")
_CARET = ord("^")
_DASH = ord("-")
_BACKSLASH = ord("\\")


def _glob_match(pattern: bytes, key: bytes, *, nocase: bool = False) -> bool:
    """True if `key` matches `pattern` in full, under the reference's own glob grammar.

    `*`, `?`, `[abc]`, `[a-z]` and `[^...]` negation behave as the reference's own glob
    does. A `-` inside a class that does not open a `low-high` range is a literal, so
    `a[-]b` matches `a-b`; `!` inside a class is always a literal too, never a negation --
    `[^a]` and `[!a]` are not the same pattern here, only the first excludes `a`. A
    backslash escapes the character after it, including a trailing one, which escapes
    itself.

    `p` and `k` walk `pattern` and `key` together, advancing both on every atom that
    consumes exactly one byte of `key` -- a literal, `?`, a class, or an escape, all
    handled by `_match_atom`. Only `*` can consume zero bytes or many, which is what
    makes backtracking necessary: a `*` that turns out to have absorbed too much, or too
    little, has to be retried against a different split. Retrying that through a fresh
    call on "the rest of the pattern, from here" -- what recursion into the same
    function does -- spends one Python stack frame per `*` group the pattern holds,
    however far apart they fall, and a pattern built from enough of them exhausts the
    stack long before it exhausts any actual budget on the work involved.

    This walk never recurses. It remembers only the *most recent* `*` it has passed and
    how far into `key` it has already tried extending that one, in `star_p`/`star_k`. A
    mismatch anywhere past that point backs off by extending that single `*` one further
    byte into `key` and resuming right after it, rather than unwinding anything -- a
    later `*` can always match everything an earlier one could have, so forgetting the
    earlier one once a later one is seen loses no match a full backtrack would have
    found. Stack depth stays flat regardless of how many `*` groups appear; only the
    running time grows, and only as `len(pattern) * len(key)` in the worst case.

    The main loop runs only while `key` still has bytes left, so a key already empty on
    entry never enters it -- and the trailing-`*` cleanup below is guarded on that,
    because the reference's loop is guarded the same way. Its `while (patternLen &&
    stringLen)` is followed by a match only when both are exhausted, so an empty key
    matches nothing there but the empty pattern, `*` included. `KEYS *` still lists an
    empty-string key, because the reference does not reach its matcher for that pattern
    either: `keysCommand` short-circuits the exact one-byte `*`, and `keys()` below
    carries the same short-circuit for the same reason.
    """
    plen, klen = len(pattern), len(key)
    p = k = 0
    star_p, star_k = -1, -1
    while k < klen:
        if p < plen and pattern[p] == _STAR:
            star_p, star_k = p, k
            p += 1
            continue
        matched, next_p = (
            _match_atom(pattern, p, key, k, nocase) if p < plen else (False, p))
        if matched:
            p, k = next_p, k + 1
        elif star_p != -1:
            star_k += 1
            p, k = star_p + 1, star_k
        else:
            return False
    if klen:
        # only when the key had bytes on entry: unguarded, this consumes stars the reference's
        # loop never reaches, and every run of `*` matches an empty key
        while p < plen and pattern[p] == _STAR:
            p += 1
    return p == plen


def _signed(byte: int) -> int:
    # the reference reads pattern and key bytes as signed `char`, so 0xff is -1 and `[\x00-\xff]`
    # is the inverted range -1..0, matching almost nothing. only a class range orders bytes
    return byte - 256 if byte >= 128 else byte


def _fold(byte: int, nocase: bool) -> int:
    # C tolower() over the ASCII letters, applied inside the comparison and not to the pattern
    # beforehand: the two differ wherever the comparison is order-sensitive
    return byte + 32 if nocase and 65 <= byte <= 90 else byte


def _match_atom(
    pattern: bytes, p: int, key: bytes, k: int, nocase: bool = False
) -> tuple[bool, int]:
    """Match the single non-`*` atom at `pattern[p]` against `key[k]`.

    Returns whether it matched and the pattern index immediately past the atom: one byte
    past a literal, `?`, or escape, and past a class's closing `]` -- or past the end of
    `pattern`, for a class that never finds one. That ending index comes out the same
    whether or not the atom matched, which is what lets `_glob_match` fall back to a
    different `key` position after a mismatch without re-scanning the atom to find where
    it ends.
    """
    plen = len(pattern)
    ch = pattern[p]
    if ch == _QUESTION:
        return True, p + 1
    if ch == _BACKSLASH:
        escaped = p + 1 if p + 1 < plen else p
        return _fold(pattern[escaped], nocase) == _fold(key[k], nocase), escaped + 1
    if ch != _LBRACKET:
        return _fold(ch, nocase) == _fold(key[k], nocase), p + 1

    p += 1
    negate = p < plen and pattern[p] == _CARET
    if negate:
        p += 1
    matched = False
    while True:
        if p < plen and pattern[p] == _BACKSLASH and p + 1 < plen:
            p += 1
            if _fold(pattern[p], nocase) == _fold(key[k], nocase):
                matched = True
        elif p < plen and pattern[p] == _RBRACKET:
            break
        elif p >= plen:
            # an unterminated class: nothing closes it, so what has been seen so far
            # is all there is
            p -= 1
            break
        elif p + 2 < plen and pattern[p + 1] == _DASH:
            # signed, as the reference's comparison is: both endpoints and the key byte take one
            # reading, so only a range straddling 0x80 tells the two apart
            lo, hi = _signed(pattern[p]), _signed(pattern[p + 2])
            if lo > hi:
                lo, hi = hi, lo
            # folded after the swap and never re-checked, the reference's own order: `[Z-a]` is
            # 90..97, folds to 122..97 and matches nothing, where a pre-folded pattern would match
            lo, hi = _fold(lo, nocase), _fold(hi, nocase)
            if lo <= _fold(_signed(key[k]), nocase) <= hi:
                matched = True
            p += 2
        elif _fold(pattern[p], nocase) == _fold(key[k], nocase):
            matched = True
        p += 1
    return (not matched if negate else matched), p + 1


@command(b"KEYS", arity=2, kind=Kind.OTHER)
def keys(store, conn, argv: list[bytes]) -> Reply:
    pattern = argv[1]
    # the reference's own short-circuit: keysCommand never calls its matcher for the one-byte `*`,
    # which is the only reason `KEYS *` lists an empty-string key on either server
    all_keys = pattern == b"*"
    # store.live_keys() rather than the shared lookup, for DBSIZE's own reason: a read
    # command must not delete what it was only asked to list
    matched = [
        key for key in store.live_keys() if all_keys or _glob_match(pattern, key)
    ]
    return resp.encode_array([resp.encode_bulk_string(key) for key in matched]), []


@command(b"FLUSHALL", arity=1, kind=Kind.WRITE)
def flushall(store, conn, argv: list[bytes]) -> Reply:
    store.flush()
    # unconditional, unlike DEL's "changed nothing propagates nothing": a follower's keyspace may
    # not be empty where the leader's is, and skipping this would leave it that way forever
    return resp.encode_simple_string(b"OK"), [[b"FLUSHALL"]]


def _used_memory_bytes() -> int:
    """Process RSS in bytes, for INFO's used_memory field.

    /proc/self/statm where the platform has one, resource.getrusage's ru_maxrss where it
    does not -- a real syscall-backed figure rather than a walk over the keyspace summing
    sys.getsizeof, which would not be comparable to the allocator-tracked number this
    field exists to approximate. No dependency reads it either: psutil is a runtime
    dependency this project carries none of, and shelling out to ps forks a process
    inside a loop with exactly one thread to give away.

    The two fallbacks disagree on units, and both have to be handled rather than one
    trusted blindly: /proc/self/statm's resident field is pages, scaled here by the page
    size, while ru_maxrss is bytes on this platform and documented as kilobytes on Linux
    -- measured here, taken on trust there, since development happens on the platform
    where it is bytes.
    """
    try:
        with open("/proc/self/statm", "rb") as statm:
            resident_pages = int(statm.read().split()[1])
        return resident_pages * resource.getpagesize()
    except FileNotFoundError:
        ru_maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return ru_maxrss if sys.platform == "darwin" else ru_maxrss * 1024


def _info_sections(store, conn) -> list[tuple[bytes, list[bytes]]]:
    # the guard is two levels: conn is None when dispatch is driven with no socket, and conn.server
    # is None for a connection built rather than accepted, so testing server first would raise
    connected = 0 if (conn is None or conn.server is None) else conn.server.connected_clients
    # redis-cli --stat reads these two and blocked_clients, and renders a field it does not find as
    # LLONG_MIN: three of its six columns read -9223372036854775808 while this report left them out
    received = 0 if (conn is None or conn.server is None) else conn.server.total_connections_received
    processed = 0 if (conn is None or conn.server is None) else conn.server.total_commands_processed

    keyspace_fields = []
    live = store.live_count()
    if live:
        # omitted entirely rather than printed as zero: an empty database is not a fact
        # INFO reports about db0, it is the reason db0 goes unmentioned
        keyspace_fields.append(b"db0:keys=%d,expires=%d" % (live, store.expiring_count()))

    return [
        (b"Server", [b"redis_version:" + SERVER_VERSION]),
        # blocked_clients is a constant: no BLPOP, no WAIT, no transaction, so 0 by construction
        # and a true statement about this process, the standard CONFIG GET's table is held to
        (b"Clients", [b"connected_clients:%d" % connected, b"blocked_clients:0"]),
        (b"Memory", [b"used_memory:%d" % _used_memory_bytes()]),
        (b"Stats", [b"total_connections_received:%d" % received,
                    b"total_commands_processed:%d" % processed]),
        (b"Replication", [b"role:" + REPLICATION_ROLE]),
        (b"Keyspace", keyspace_fields),
    ]


def _section_body(name: bytes, fields: list[bytes]) -> bytes:
    # a header, its fields, then a blank line even for a section with no fields: on an empty
    # database Keyspace keeps its header and only the db0 line drops out
    return b"# " + name + b"\r\n" + b"".join(field + b"\r\n" for field in fields) + b"\r\n"


# the reference's three whole-report selectors, not section names: read as names, a client that
# spells the bare form out gets an empty body. every section here is a default section
_INFO_WHOLE_REPORT = frozenset((b"ALL", b"EVERYTHING", b"DEFAULT"))


@command(b"INFO", arity=-1, kind=Kind.OTHER)
def info(store, conn, argv: list[bytes]) -> Reply:
    # named arguments filter, case-insensitively, and an unrecognised one contributes nothing:
    # asking for a section this server lacks should get back less, not a connection-ending reply
    wanted = {name.upper() for name in argv[1:]}
    if wanted & _INFO_WHOLE_REPORT:
        # an empty filter is the bare form, so a selector beside a section name widens rather
        # than intersects, which is the reference's reading of `INFO default Server`
        wanted = set()
    body = b""
    for name, fields in _info_sections(store, conn):
        if wanted and name.upper() not in wanted:
            continue
        body += _section_body(name, fields)
    return resp.encode_bulk_string(body), []


# every value a true statement about this process, and a table rather than an echo: echoing made
# an unknown name a parameter that exists, so `redis-py`'s `CONFIG GET *` got back `*` itself
_CONFIG: dict[bytes, bytes] = {
    b"appendonly": b"no",
    b"databases": b"1",
    b"maxmemory": b"0",
    b"maxmemory-policy": b"noeviction",
}

# the three bytes that make an argument a pattern rather than a name, which is the
# distinction the reference draws before it decides how to answer
_GLOB_METACHARACTERS = b"*?["


def _save_rule(conn) -> bytes:
    # the reference's syntax: a rule fires once the clock is MORE than <seconds> past the last save,
    # so every N seconds is `N-1 0` (7.2.7's `save "1 0"` saves every two), and `""` is off
    server = None if conn is None else conn.server
    interval = 0 if server is None else server.armed_snapshot_interval
    if not interval:
        return b""
    return b"%d 0" % (interval - 1)


@command(b"CONFIG", arity=-2, kind=Kind.OTHER)
def config(store, conn, argv: list[bytes]) -> Reply:
    # one subcommand: a benchmarking client probes CONFIG GET before anything else and treats an
    # error reply as fatal, which is what this has to survive
    if argv[1].upper() != b"GET":
        return resp.encode_error(SYNTAX_ERROR), []
    patterns = argv[2:]
    if not patterns:
        # the reference names the subcommand here and not the command, the only arity error in this
        # server with one to name, so it is spelt out rather than coming from cmd.name
        return wrong_arity(b"CONFIG|GET"), []
    # built per call, since save's value belongs to the accepting server, and both branches answer
    # from this one table or `CONFIG GET save` and `CONFIG GET *` could disagree
    table = {**_CONFIG, b"save": _save_rule(conn)}
    pairs = []
    seen = set()
    for pattern in patterns:
        lowered = pattern.lower()
        if not any(byte in pattern for byte in _GLOB_METACHARACTERS):
            # an exact name is looked up and the reply carries the client's own spelling:
            # `CONFIG GET MAXMEMORY` answers `MAXMEMORY` on the reference, and the lookup folds case
            if lowered in table and lowered not in seen:
                seen.add(lowered)
                pairs.append(resp.encode_bulk_string(pattern))
                pairs.append(resp.encode_bulk_string(table[lowered]))
            continue
        for name, value in table.items():
            # the matcher folds case rather than the pattern being lowered first: a class range is
            # order-sensitive, so a lowered pattern matches ranges the reference refuses
            if name not in seen and _glob_match(pattern, name, nocase=True):
                seen.add(name)
                pairs.append(resp.encode_bulk_string(name))
                pairs.append(resp.encode_bulk_string(value))
    return resp.encode_array(pairs), []
