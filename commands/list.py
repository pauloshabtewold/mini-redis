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
        # a deque is mutable where bytes is not, so nothing is left for a store method to do here.
        # written once on the miss branch: a second write would clear a TTL set between the two
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
        # the pop's own effect travels alone: a follower applies the same pop and runs the identical
        # cleanup against its own copy, so a DEL beside it would only double the traffic
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
    # both indices are parsed before the key is looked up: measured against redis-server 7.2.7, a bad
    # index answers the index error against a wrong-kind key and against a missing key alike
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
    # islice walks in place rather than copying the container, and only ever from the head, so the
    # half nearer the tail is read off reversed(): `LRANGE k -1 -1` is otherwise linear in the list
    if start <= n - 1 - stop:
        elements = islice(container, start, stop + 1)
    else:
        elements = reversed(list(islice(reversed(container), n - 1 - stop, n - start)))
    return resp.encode_array([resp.encode_bulk_string(e) for e in elements]), []


@command(b"LLEN", arity=2, kind=Kind.READ)
def llen(store, conn, argv: list[bytes]) -> Reply:
    container = store.lookup(argv[1], kind=KIND_LIST)
    if container is None:
        return resp.encode_integer(0), []
    return resp.encode_integer(len(container)), []
