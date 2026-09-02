"""The (response, effects) contract every registered command handler must satisfy."""

import socket

import pytest

import commands
from connection import Connection
from store import Store


def assert_is_a_reply_pair(name, pair):
    """Raise unless `pair` is the (bytes, list-of-lists-of-bytes) every handler owes.

    A function rather than four asserts inlined below, because the test that proves
    this check would catch a malformed pair has to run the same code the sweep runs.
    Written out separately, the two drift and the proof stops being about the check.
    """
    assert type(pair) is tuple and len(pair) == 2, (name, pair)
    response, effects = pair
    assert type(response) is bytes, (name, response)
    assert type(effects) is list, (name, effects)
    for effect in effects:
        assert type(effect) is list, (name, effect)
        for element in effect:
            assert type(element) is bytes, (name, effect, element)


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
        pair = commands.dispatch(store, conn, argv)
        assert_is_a_reply_pair(name, pair)
        _response, effects = pair
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


def test_the_contract_check_rejects_a_nested_pair_and_a_none_effects_list():
    """The sweep above is only worth its runtime if its check can fail.

    Both shapes here are mistakes a handler can really make -- returning the pair it
    got from dispatch as the *response* half of a second pair, and returning None
    where the effects list belongs. Each is fed to the same assert_is_a_reply_pair
    the sweep uses, and the test is that it raises. Asserting on the malformed values
    directly instead would prove only that they are what this file just wrote down.
    """
    store = Store()
    sock, peer = socket.socketpair()
    conn = Connection(sock, ("127.0.0.1", 0))
    try:
        nested = (commands.dispatch(store, conn, [b"PING"]), [])
        with pytest.raises(AssertionError):
            assert_is_a_reply_pair(b"PING", nested)

        with pytest.raises(AssertionError):
            assert_is_a_reply_pair(b"PING", (b"+PONG\r\n", None))

        with pytest.raises(AssertionError):
            assert_is_a_reply_pair(b"PING", (b"+PONG\r\n", [[b"SET", "k"]]))

        # and it passes the real thing, so the three above fail for their own reason
        # rather than because the check rejects everything
        assert_is_a_reply_pair(b"PING", commands.dispatch(store, conn, [b"PING"]))
    finally:
        sock.close()
        peer.close()
