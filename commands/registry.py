"""Command registration: name, arity, read/write/other kind tag, and dispatch."""

import enum
from collections.abc import Callable
from typing import Any, NamedTuple

import resp
from store import Store, WrongTypeError

# the second element is a list and never None, so the drain loop can append to it
# unconditionally and the contract test has exactly one shape to assert -- every handler
# returns this pair, not only the ones that change something
Effects = list[list[bytes]]
Reply = tuple[bytes, Effects]
Handler = Callable[[Store, Any, list[bytes]], Reply]


class Kind(enum.StrEnum):
    """What a command IS, not what it does to replication.

    READ, WRITE and OTHER describe the command itself -- for the rate
    limiter's exemption and for documentation. They never decide what
    replicates: propagation is driven by the effect list, because a read
    that finds an expired key must emit a DEL and the sweep emits DELs
    with no command in sight.

    StrEnum so the value renders into introspection output and log lines
    with no conversion. An enum rather than a plain string so a typo is
    an AttributeError at the line that has it.
    """

    READ = "read"
    WRITE = "write"
    OTHER = "other"


class Command(NamedTuple):
    name: bytes
    arity: int
    kind: Kind
    handler: Handler


COMMANDS: dict[bytes, Command] = {}


def command(name: bytes, arity: int, kind: Kind) -> Callable[[Handler], Handler]:
    canonical = name.upper()

    def register(handler: Handler) -> Handler:
        if canonical in COMMANDS:
            # a silent replacement would produce a command that exists, answers, and answers from the wrong handler -- no arity check and no tag check can see it
            raise ValueError(f"command {canonical!r} already registered")
        COMMANDS[canonical] = Command(canonical, arity, kind, handler)
        return handler

    return register


def wrong_arity(name: bytes) -> bytes:
    # bytes, not a pair: the second element is added exactly once, at the point of
    # return, so `return wrong_arity(name), []` cannot nest a pair inside a pair
    return resp.encode_error(b"ERR wrong number of arguments for '" + name.lower() + b"' command")


def dispatch(store: Store, conn: Any, argv: list[bytes]) -> Reply:
    canonical = argv[0].upper()
    cmd = COMMANDS.get(canonical)
    if cmd is None:
        # argv[0] is client-controlled bytes and a multibulk name may legally contain \r\n; an unmapped name would otherwise split one error frame into two and desynchronise every later reply on this connection
        name = argv[0].replace(b"\r", b" ").replace(b"\n", b" ")
        return resp.encode_error(b"ERR unknown command '" + name + b"'"), []

    # positive arity is exact, negative is a minimum, both counting the command name -- an upper bound is the handler's own check
    ok = len(argv) == cmd.arity if cmd.arity >= 0 else len(argv) >= -cmd.arity
    if not ok:
        return wrong_arity(cmd.name), []

    # WRONGTYPE is the store's fact and the dispatcher's reply, so no handler is given the
    # chance to forget it -- a forgotten check surfaces instead as a TypeError the
    # per-connection boundary turns into a closed connection, a client error reported as a
    # server fault
    try:
        return cmd.handler(store, conn, argv)
    except WrongTypeError as exc:
        return resp.encode_error(exc.message), []


# the shared integer grammar every numeric command argument uses
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
NOT_AN_INTEGER = b"ERR value is not an integer or out of range"
SYNTAX_ERROR = b"ERR syntax error"


def parse_int64(field: bytes) -> int | None:
    """Parse a wire-protocol integer field, or return None if it is not one.

    Stricter than Python's int(), which accepts "011", "+5", "-0", leading and trailing
    whitespace, and digit-group underscores -- none of which the protocol allows. The
    divergence matters on the path where a stored value is reinterpreted as an integer:
    left to int(), a value nobody wrote could come back from INCR.
    """
    if not field or len(field) > 20:
        return None
    if field == b"0":
        # "0" alone is a property of the whole field, not of the digits after a leading
        # "-": checked that way instead, it would accept b"-0", which real Redis rejects
        return 0
    rest = field[1:] if field[0:1] == b"-" else field
    if not rest or rest[0:1] not in b"123456789" or not rest.isdigit():
        return None
    value = int(field)
    if value < INT64_MIN or value > INT64_MAX:
        return None
    return value
