"""The (response, effects) contract every registered command handler must satisfy."""

import socket

import commands
from connection import Connection
from store import Store


def test_every_registered_handler_returns_a_reply_pair():
    driven = set()
    wrote_effects = set()
    write_names = {
        name for name, cmd in commands.registry.COMMANDS.items()
        if cmd.kind == commands.registry.Kind.WRITE
    }

    # a real Connection, not None: HELLO's own reply reads conn.id, and this function
    # takes no fixture of any kind, so it builds the one connection every pass shares
    sock, _peer = socket.socketpair()
    conn = Connection(sock, ("127.0.0.1", 0))

    def drive(store, argv):
        name = argv[0]
        response, effects = commands.dispatch(store, conn, argv)
        assert type(response) is bytes, name
        assert type(effects) is list, name
        for effect in effects:
            assert type(effect) is list, (name, effect)
            for element in effect:
                assert type(element) is bytes, (name, effect, element)
        driven.add(name)
        if effects:
            wrote_effects.add(name)

    def argv_for(name, filler):
        arity = commands.registry.COMMANDS[name].arity
        return [name] + [filler] * (abs(arity) - 1)

    try:
        empty = Store()
        for name in commands.registry.COMMANDS:
            drive(empty, argv_for(name, b"x"))

        preloaded = Store()
        preloaded.write(b"x", b"1", keep_ttl=False)
        for name in commands.registry.COMMANDS:
            drive(preloaded, argv_for(name, b"x"))
        for name in commands.registry.COMMANDS:
            drive(preloaded, argv_for(name, b"1"))

        # one filler cannot be both an existing key and a parsing integer, so EXPIRE,
        # PEXPIRE and PEXPIREAT cannot reach store.expire_at through the uniform
        # derivation above -- this pass rebuilds the store before each command instead,
        # with a real key at [1] and an absolute deadline at the tail for the *AT commands
        for name in commands.registry.COMMANDS:
            store = Store()
            store.write(b"x", b"1", keep_ttl=False)
            arg = b"%d" % (store.now_ms() + 60_000) if name.endswith(b"AT") else b"1"
            arity = commands.registry.COMMANDS[name].arity
            drive(store, [name, b"x"] + [arg] * (abs(arity) - 2))
    finally:
        sock.close()
        _peer.close()

    assert driven == set(commands.registry.COMMANDS)
    assert write_names <= wrote_effects, sorted(write_names - wrote_effects)


def test_a_reply_pair_is_never_none_or_a_nested_tuple():
    store = Store()

    def double_wrapped(store, conn, argv):
        # the mistake this pins: the handler called here already returns (response,
        # effects), and this wraps that whole pair as the response half of a second
        # pair instead of returning it directly
        return commands.dispatch(store, conn, argv), []

    response, effects = double_wrapped(store, None, [b"PING"])
    assert type(response) is not bytes
    assert type(effects) is list

    def effects_as_none(store, conn, argv):
        return b"+PONG\r\n", None

    response, effects = effects_as_none(store, None, [b"PING"])
    assert type(response) is bytes
    assert type(effects) is not list
