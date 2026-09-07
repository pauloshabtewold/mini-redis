"""Connection and server commands: PING, ECHO, HELLO, DBSIZE, KEYS, FLUSHALL, INFO, CONFIG."""

import resource
import sys

from commands.registry import Kind, Reply, SYNTAX_ERROR, command, wrong_arity
import resp

# kept as a constant rather than read from installed metadata, so the reply does not depend on the project being installed
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
        # every argument other than b"2" answers NOPROTO, integer or not -- diverges from real Redis's own parse error, deliberately
        return resp.encode_error(b"NOPROTO unsupported protocol version"), []
    # RESP2 has no map type, so this is a flat array of alternating field/value pairs -- what a real server returns on RESP2
    return resp.encode_array([
        resp.encode_bulk_string(b"server"),
        resp.encode_bulk_string(b"redis"),
        resp.encode_bulk_string(b"version"),
        resp.encode_bulk_string(SERVER_VERSION),
        resp.encode_bulk_string(b"proto"),
        resp.encode_integer(2),
        resp.encode_bulk_string(b"id"),
        # 0 when dispatch is driven with no socket at all, which is this project's own
        # convention for in-process use -- the same guard INFO's connected_clients takes,
        # for the same callers. Unguarded, the one command whose reply carries a
        # connection fact is the one command that convention cannot call, and it fails
        # with an AttributeError rather than a reply
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
    # store.live_count() rather than the shared expiry-aware lookup: DBSIZE is one of the
    # named exceptions to that rule, because routing a count through it would delete every
    # expired key the count scanned past and queue a DEL for each -- a read answering a
    # number and mutating the keyspace to do it, with propagation nobody asked for
    return resp.encode_integer(store.live_count()), []


# KEYS's glob grammar, hand-ported from the reference's own matcher rather than fnmatch.
# Measured side by side on the same patterns, the two disagree on which character negates
# a class -- this grammar takes '^', fnmatch takes '!', and each treats the other as an
# ordinary member of the class -- so a client asking for the complement of a set would get
# back exactly the set it meant to exclude, with no error to notice it by. fnmatch also has
# no backslash escape at all, and it operates on str, which the bytes invariant on this
# path forbids, and memoises translated patterns in a cache keyed by whatever a client sends
_STAR = ord("*")
_QUESTION = ord("?")
_LBRACKET = ord("[")
_RBRACKET = ord("]")
_CARET = ord("^")
_DASH = ord("-")
_BACKSLASH = ord("\\")


def _glob_match(pattern: bytes, key: bytes) -> bool:
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
        matched, next_p = _match_atom(pattern, p, key, k) if p < plen else (False, p)
        if matched:
            p, k = next_p, k + 1
        elif star_p != -1:
            star_k += 1
            p, k = star_p + 1, star_k
        else:
            return False
    if klen:
        # only when the key had bytes on entry. Unguarded, this consumes stars the
        # reference's loop never reaches, and every run of nothing but `*` matches an
        # empty key -- which `*` alone appears to get right only because the caller's
        # short-circuit answers that one pattern before this function sees it
        while p < plen and pattern[p] == _STAR:
            p += 1
    return p == plen


def _signed(byte: int) -> int:
    # the reference walks its pattern and its key through `char`, which is signed on
    # every platform this is measured against, so 0xff reads as -1 there and a range's
    # low-high ordering is decided on that reading: `[\x00-\xff]` is the inverted range
    # -1..0 and matches almost nothing, where an unsigned reading makes it match
    # everything. Only the class-range branch orders bytes rather than comparing them for
    # equality, so it is the only place this is needed -- equality survives either reading
    return byte - 256 if byte >= 128 else byte


def _match_atom(pattern: bytes, p: int, key: bytes, k: int) -> tuple[bool, int]:
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
        return pattern[escaped] == key[k], escaped + 1
    if ch != _LBRACKET:
        return ch == key[k], p + 1

    p += 1
    negate = p < plen and pattern[p] == _CARET
    if negate:
        p += 1
    matched = False
    while True:
        if p < plen and pattern[p] == _BACKSLASH and p + 1 < plen:
            p += 1
            if pattern[p] == key[k]:
                matched = True
        elif p < plen and pattern[p] == _RBRACKET:
            break
        elif p >= plen:
            # an unterminated class: nothing closes it, so what has been seen so far
            # is all there is
            p -= 1
            break
        elif p + 2 < plen and pattern[p + 1] == _DASH:
            # signed, because the reference's comparison is -- see _signed. Both
            # endpoints and the key byte take the same reading, so a range entirely
            # below 0x80 or entirely above it behaves exactly as an unsigned one would;
            # only a range straddling 0x80 can tell the two apart
            lo, hi = _signed(pattern[p]), _signed(pattern[p + 2])
            if lo > hi:
                lo, hi = hi, lo
            if lo <= _signed(key[k]) <= hi:
                matched = True
            p += 2
        elif pattern[p] == key[k]:
            matched = True
        p += 1
    return (not matched if negate else matched), p + 1


@command(b"KEYS", arity=2, kind=Kind.OTHER)
def keys(store, conn, argv: list[bytes]) -> Reply:
    pattern = argv[1]
    # the reference's own short-circuit, not an optimisation added on top of it:
    # keysCommand tests for the exact one-byte `*` and never calls its matcher for that
    # pattern. It is the only reason `KEYS *` lists an empty-string key on either server,
    # because the matcher itself answers no to every pattern against an empty key but the
    # empty one. Removing this line silently stops listing that key -- the defect a
    # previous round found, repaired then in the matcher and repaired here instead
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
    # unconditional, unlike DEL's "changed nothing propagates nothing": a leader whose
    # keyspace is already empty still owes its followers the instruction, because a
    # follower's own keyspace may not be -- it holds whatever it has not yet been told to
    # remove, and skipping the effect here would leave that behind forever
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
    # the fixed section order INFO reports in, each paired with the fields it carries.
    # connected_clients is the one field anywhere in this reply that is a fact about
    # sockets rather than about the store or the process, which is why it alone reads the
    # connection's third slot -- and the guard here is two levels, not one: conn is None
    # whenever dispatch is driven with no socket at all, which is this project's own
    # convention for in-process use, and conn.server is None for a connection built
    # directly rather than accepted. checking conn.server first would raise on exactly
    # the callers this guard exists for
    connected = 0 if (conn is None or conn.server is None) else conn.server.connected_clients

    keyspace_fields = []
    live = store.live_count()
    if live:
        # omitted entirely rather than printed as zero: an empty database is not a fact
        # INFO reports about db0, it is the reason db0 goes unmentioned
        keyspace_fields.append(b"db0:keys=%d,expires=%d" % (live, store.expiring_count()))

    return [
        (b"Server", [b"redis_version:" + SERVER_VERSION]),
        (b"Clients", [b"connected_clients:%d" % connected]),
        (b"Memory", [b"used_memory:%d" % _used_memory_bytes()]),
        (b"Replication", [b"role:" + REPLICATION_ROLE]),
        (b"Keyspace", keyspace_fields),
    ]


def _section_body(name: bytes, fields: list[bytes]) -> bytes:
    # a header line, its fields, then a blank line -- even for a section with no fields of
    # its own, which is how Keyspace renders on an empty database: the header stays and
    # only the db0 line drops out
    return b"# " + name + b"\r\n" + b"".join(field + b"\r\n" for field in fields) + b"\r\n"


# the reference's three whole-report selectors, which are not section names: `all` and
# `everything` widen the report past its default sections there, and `default` names
# exactly what a bare INFO already returns. Treating them as unrecognised section names
# answers a client that spells the bare form out with an empty body, which is the one
# shape of "less" that tells it nothing rather than telling it less. Every section here
# is a default section, so all three select the same set
_INFO_WHOLE_REPORT = frozenset((b"ALL", b"EVERYTHING", b"DEFAULT"))


@command(b"INFO", arity=-1, kind=Kind.OTHER)
def info(store, conn, argv: list[bytes]) -> Reply:
    # bare INFO reports every section; named arguments filter to only those, matched
    # case-insensitively. An unrecognised name contributes nothing rather than erroring --
    # a client asking a section this server does not have should get back less, not a
    # connection-ending reply, and an all-unrecognised request is legitimately empty
    wanted = {name.upper() for name in argv[1:]}
    if wanted & _INFO_WHOLE_REPORT:
        # an empty filter is what the bare form already uses, so a selector alongside a
        # section name widens rather than intersects -- which is the reference's own
        # reading of `INFO default Server`
        wanted = set()
    body = b""
    for name, fields in _info_sections(store, conn):
        if wanted and name.upper() not in wanted:
            continue
        body += _section_body(name, fields)
    return resp.encode_bulk_string(body), []


# The configuration this server actually has. Every value is a true statement about this
# process -- nothing is written to disk, nothing evicts, nothing bounds the keyspace, and
# there is one keyspace rather than sixteen -- and three of the five happen to equal the
# reference's own default because those defaults are "off" as well. `save` is not one of
# them: a default-configured reference answers `3600 1 300 100 60 10000` there, and `""`
# is what a server with no snapshots honestly reports.
#
# A table rather than an echo of whatever was asked for. Echoing meant a name this server
# has never heard of came back as a parameter that exists with an empty value, which is a
# reply a client acts on -- `redis-py`'s config_get() sends `CONFIG GET *` and got back a
# single parameter named `*`. Answering from a table makes an unknown name answer the
# empty array the reference answers, and makes the values that are returned true
_CONFIG: dict[bytes, bytes] = {
    b"appendonly": b"no",
    b"databases": b"1",
    b"maxmemory": b"0",
    b"maxmemory-policy": b"noeviction",
    b"save": b"",
}

# the three bytes that make an argument a pattern rather than a name, which is the
# distinction the reference draws before it decides how to answer
_GLOB_METACHARACTERS = b"*?["


@command(b"CONFIG", arity=-2, kind=Kind.OTHER)
def config(store, conn, argv: list[bytes]) -> Reply:
    # one subcommand implemented. A benchmarking or profiling client probes CONFIG GET for
    # a handful of parameters before doing anything else and treats an error reply as
    # fatal, which is what this has to survive; real configurability is not on offer
    if argv[1].upper() != b"GET":
        return resp.encode_error(SYNTAX_ERROR), []
    patterns = argv[2:]
    if not patterns:
        # the reference names the subcommand in this one message rather than the command,
        # and it is the only arity error in this server that has a subcommand to name --
        # so the name is spelt out here instead of coming from cmd.name like every other
        return wrong_arity(b"CONFIG|GET"), []
    pairs = []
    seen = set()
    for pattern in patterns:
        lowered = pattern.lower()
        if not any(byte in pattern for byte in _GLOB_METACHARACTERS):
            # an exact name is looked up, not matched, and the reply carries the spelling
            # the client used rather than the canonical one -- `CONFIG GET MAXMEMORY`
            # answers `MAXMEMORY` on the reference, and the lookup itself ignores case
            if lowered in _CONFIG and lowered not in seen:
                seen.add(lowered)
                pairs.append(resp.encode_bulk_string(pattern))
                pairs.append(resp.encode_bulk_string(_CONFIG[lowered]))
            continue
        for name, value in _CONFIG.items():
            # the pattern is lowered rather than the matcher being taught to ignore case:
            # every name in the table is already lower, so for the ASCII these names are
            # drawn from the two are the same thing. KEYS passes no such flag and must not
            # -- a key is arbitrary bytes and its case is the client's business
            if name not in seen and _glob_match(lowered, name):
                seen.add(name)
                pairs.append(resp.encode_bulk_string(name))
                pairs.append(resp.encode_bulk_string(value))
    return resp.encode_array(pairs), []
