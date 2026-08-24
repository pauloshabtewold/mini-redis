"""Command registration: name, arity, read/write/other kind tag, and dispatch."""

import enum
from collections.abc import Callable
from typing import Any, NamedTuple

import resp

Handler = Callable[[Any, list[bytes]], bytes]


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
    return resp.encode_error(b"ERR wrong number of arguments for '" + name.lower() + b"' command")


def dispatch(conn, argv: list[bytes]) -> bytes:
    canonical = argv[0].upper()
    cmd = COMMANDS.get(canonical)
    if cmd is None:
        # argv[0] is client-controlled bytes and a multibulk name may legally contain \r\n; an unmapped name would otherwise split one error frame into two and desynchronise every later reply on this connection
        name = argv[0].replace(b"\r", b" ").replace(b"\n", b" ")
        return resp.encode_error(b"ERR unknown command '" + name + b"'")

    # positive arity is exact, negative is a minimum, both counting the command name -- an upper bound is the handler's own check
    ok = len(argv) == cmd.arity if cmd.arity >= 0 else len(argv) >= -cmd.arity
    if not ok:
        return wrong_arity(cmd.name)

    return cmd.handler(conn, argv)
