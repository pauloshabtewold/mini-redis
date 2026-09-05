"""List commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN."""

from collections import deque
from itertools import islice

from commands.registry import Kind, NOT_AN_INTEGER, Reply, command, parse_int64
import resp
from store import KIND_LIST


def _push(store, argv: list[bytes], name: bytes, *, prepend: bool) -> Reply:
    key = argv[1]
    elements = argv[2:]
    container = store.lookup(key, kind=KIND_LIST)
    if container is None:
        # bytes is immutable, which is the whole reason Store.write() exists for a
        # string -- INCR cannot alter a stored value without replacing it outright. A
        # deque has no such problem: it can be altered in place, so there is nothing
        # left for a store method to do that holding the object and mutating it does
        # not already do. Written once, here, on the miss branch only -- a second
        # write after pushing, or a fresh deque laid over one just mutated, would
        # silently clear a TTL set between the two writes
        container = deque()
        store.write(key, container, keep_ttl=False)
    if prepend:
        for element in elements:
            container.appendleft(element)
    else:
        for element in elements:
            container.append(element)
    return resp.encode_integer(len(container)), [[name, key] + elements]


@command(b"LPUSH", arity=-3, kind=Kind.WRITE)
def lpush(store, conn, argv: list[bytes]) -> Reply:
    return _push(store, argv, b"LPUSH", prepend=True)


@command(b"RPUSH", arity=-3, kind=Kind.WRITE)
def rpush(store, conn, argv: list[bytes]) -> Reply:
    return _push(store, argv, b"RPUSH", prepend=False)


def _pop(store, argv: list[bytes], name: bytes, *, from_left: bool) -> Reply:
    key = argv[1]
    container = store.lookup(key, kind=KIND_LIST)
    if container is None:
        return resp.encode_bulk_string(None), []
    element = container.popleft() if from_left else container.pop()
    if not container:
        # store.remove() is the only thing that may delete a key, and it feeds the
        # effects queue nothing -- so the pop's own effect below travels alone. A
        # follower applies this same pop through its own dispatcher and runs the
        # identical cleanup against its own copy; joining a DEL to this effect would
        # only double the traffic on every last-element pop for no reason the
        # follower's own dispatch does not already cover
        store.remove(key)
    return resp.encode_bulk_string(element), [[name, key]]


@command(b"LPOP", arity=2, kind=Kind.WRITE)
def lpop(store, conn, argv: list[bytes]) -> Reply:
    return _pop(store, argv, b"LPOP", from_left=True)


@command(b"RPOP", arity=2, kind=Kind.WRITE)
def rpop(store, conn, argv: list[bytes]) -> Reply:
    return _pop(store, argv, b"RPOP", from_left=False)


@command(b"LRANGE", arity=4, kind=Kind.READ)
def lrange(store, conn, argv: list[bytes]) -> Reply:
    key = argv[1]
    # both indices are parsed before the key is looked up at all. Argument validation
    # outranks both the type check and the existence check -- measured against
    # redis-server 7.2.7, a bad index against a wrong-kind key answers the index error
    # rather than WRONGTYPE, and the same bad index against a missing key answers it
    # too. Looked up first, either the WRONGTYPE or the empty array would arrive
    # instead and hide this
    start = parse_int64(argv[2])
    stop = parse_int64(argv[3])
    if start is None or stop is None:
        return resp.encode_error(NOT_AN_INTEGER), []

    container = store.lookup(key, kind=KIND_LIST)
    if container is None:
        return resp.encode_array([]), []

    n = len(container)
    if start < 0:
        start += n
    if stop < 0:
        stop += n
    start = max(start, 0)
    stop = min(stop, n - 1)
    if start > stop or start >= n:
        return resp.encode_array([]), []
    # deque has no slice syntax of its own; islice walks the run in place instead of
    # copying the whole container into a list just to throw most of it away
    elements = islice(container, start, stop + 1)
    return resp.encode_array([resp.encode_bulk_string(e) for e in elements]), []


@command(b"LLEN", arity=2, kind=Kind.READ)
def llen(store, conn, argv: list[bytes]) -> Reply:
    container = store.lookup(argv[1], kind=KIND_LIST)
    if container is None:
        return resp.encode_integer(0), []
    return resp.encode_integer(len(container)), []
