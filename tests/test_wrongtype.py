"""WRONGTYPE against real values on both sides: every type-specific command run
against a key of the other kind, the commands that must not raise it against either
kind, and the one ordering nothing else here pins -- a key that is both logically
expired and of the wrong kind reads as missing, not as WRONGTYPE.
"""

import pytest

import commands
from tests.conftest import FROZEN, FrozenStore

WRONGTYPE = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"


@pytest.fixture
def store():
    s = FrozenStore()
    commands.dispatch(s, None, [b"SET", b"s", b"v"])
    commands.dispatch(s, None, [b"RPUSH", b"l", b"a"])
    s.take_effects()
    return s


# --- normal cases -----------------------------------------------------------------------


@pytest.mark.parametrize("argv", [
    [b"GET", b"l"], [b"INCR", b"l"], [b"DECR", b"l"], [b"SET", b"l", b"v", b"GET"],
], ids=["get", "incr", "decr", "set-get"])
def test_string_commands_raise_wrongtype_against_a_list(store, argv):
    assert commands.dispatch(store, None, argv) == (WRONGTYPE, [])


@pytest.mark.parametrize("argv", [
    [b"LPUSH", b"s", b"x"], [b"RPUSH", b"s", b"x"], [b"LPOP", b"s"], [b"RPOP", b"s"],
    [b"LRANGE", b"s", b"0", b"-1"], [b"LLEN", b"s"],
], ids=["lpush", "rpush", "lpop", "rpop", "lrange", "llen"])
def test_list_commands_raise_wrongtype_against_a_string(store, argv):
    assert commands.dispatch(store, None, argv) == (WRONGTYPE, [])


@pytest.mark.parametrize("key", [b"s", b"l"], ids=["string", "list"])
def test_set_without_get_is_type_agnostic(store, key):
    # measured against redis-server 7.2.7: SET on a list key answers +OK, overwriting the
    # list with a string outright -- it does not consult the key's existing kind at all
    assert commands.dispatch(store, None, [b"SET", key, b"new"])[0] == b"+OK\r\n"


@pytest.mark.parametrize("key", [b"s", b"l"], ids=["string", "list"])
def test_del_is_type_agnostic(store, key):
    assert commands.dispatch(store, None, [b"DEL", key])[0] == b":1\r\n"


@pytest.mark.parametrize("key", [b"s", b"l"], ids=["string", "list"])
def test_exists_is_type_agnostic(store, key):
    assert commands.dispatch(store, None, [b"EXISTS", key]) == (b":1\r\n", [])


def test_type_answers_each_key_s_own_kind(store):
    assert commands.dispatch(store, None, [b"TYPE", b"s"]) == (b"+string\r\n", [])
    assert commands.dispatch(store, None, [b"TYPE", b"l"]) == (b"+list\r\n", [])


@pytest.mark.parametrize("key", [b"s", b"l"], ids=["string", "list"])
def test_expire_is_type_agnostic(store, key):
    assert commands.dispatch(store, None, [b"EXPIRE", key, b"100"]) == (
        b":1\r\n", [[b"PEXPIREAT", key, b"%d" % (FROZEN + 100_000)]])


@pytest.mark.parametrize("key", [b"s", b"l"], ids=["string", "list"])
def test_pexpire_is_type_agnostic(store, key):
    assert commands.dispatch(store, None, [b"PEXPIRE", key, b"100000"]) == (
        b":1\r\n", [[b"PEXPIREAT", key, b"%d" % (FROZEN + 100_000)]])


@pytest.mark.parametrize("key", [b"s", b"l"], ids=["string", "list"])
def test_pexpireat_is_type_agnostic(store, key):
    deadline = FROZEN + 100_000
    assert commands.dispatch(store, None, [b"PEXPIREAT", key, b"%d" % deadline]) == (
        b":1\r\n", [[b"PEXPIREAT", key, b"%d" % deadline]])


@pytest.mark.parametrize("key", [b"s", b"l"], ids=["string", "list"])
def test_ttl_and_pttl_are_type_agnostic(store, key):
    commands.dispatch(store, None, [b"EXPIRE", key, b"100"])
    assert commands.dispatch(store, None, [b"TTL", key]) == (b":100\r\n", [])
    assert commands.dispatch(store, None, [b"PTTL", key]) == (b":100000\r\n", [])


# --- edge cases --------------------------------------------------------------------------


def test_an_expired_key_reads_as_missing_rather_than_wrongtype(store):
    # Store.lookup() checks the deadline before the kind, so a key that is both logically
    # expired and of the wrong kind reads as missing. test_store.py pins this ordering for
    # a key that was never there; this pins it for one that expired
    store.expire_at(b"l", FROZEN - 1)
    assert commands.dispatch(store, None, [b"GET", b"l"]) == (b"$-1\r\n", [])
    assert store.take_effects() == [[b"DEL", b"l"]], (
        "the lazy DEL goes to the queue, not the handler")
