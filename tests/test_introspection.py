"""DBSIZE, KEYS, FLUSHALL, INFO, CONFIG: the introspection and admin commands, KEYS's
hand-written glob grammar, and the connected-client count INFO reads through the
connection's third slot.
"""

import resource
import socket
import sys
import time

import pytest
from redis._parsers.helpers import parse_info

import commands
from connection import Connection
from store import Store
from tests.conftest import FROZEN, FrozenStore
from tests.test_server_lifecycle import listening, pump


@pytest.fixture
def store():
    return Store()


# --- DBSIZE and KEYS: normal cases ------------------------------------------------------


def test_dbsize_on_an_empty_store(store, conn):
    assert commands.dispatch(store, conn, [b"DBSIZE"]) == (b":0\r\n", [])


def test_dbsize_counts_every_live_key_regardless_of_kind(store, conn):
    commands.dispatch(store, conn, [b"SET", b"s", b"v"])
    commands.dispatch(store, conn, [b"RPUSH", b"l", b"a"])
    assert commands.dispatch(store, conn, [b"DBSIZE"]) == (b":2\r\n", [])


def test_keys_on_an_empty_store(store, conn):
    assert commands.dispatch(store, conn, [b"KEYS", b"*"]) == (b"*0\r\n", [])


def test_keys_star_lists_every_key_regardless_of_kind(store, conn):
    commands.dispatch(store, conn, [b"SET", b"s", b"v"])
    commands.dispatch(store, conn, [b"RPUSH", b"l", b"a"])
    reply, effects = commands.dispatch(store, conn, [b"KEYS", b"*"])
    assert effects == []
    assert sorted(reply.split(b"\r\n")[2::2]) == [b"l", b"s"]


# --- DBSIZE and KEYS: filtered, not removed ---------------------------------------------


def test_dbsize_excludes_an_expired_key_without_deleting_it():
    # routed through live_count() rather than the shared expiry-aware lookup: DBSIZE and
    # KEYS are the deliberate exception, because a count or a listing that deletes what
    # it scanned past would be a read command mutating the keyspace
    s = FrozenStore()
    s.write(b"gone", b"v", keep_ttl=False)
    s.expire_at(b"gone", FROZEN - 1)
    assert commands.dispatch(s, None, [b"DBSIZE"]) == (b":0\r\n", [])
    assert b"gone" in s._data, "an expired key counted out must not be deleted"
    assert s.take_effects() == [], "a count must not propagate a DEL"


def test_keys_excludes_an_expired_key_without_deleting_it():
    s = FrozenStore()
    s.write(b"gone", b"v", keep_ttl=False)
    s.expire_at(b"gone", FROZEN - 1)
    assert commands.dispatch(s, None, [b"KEYS", b"*"]) == (b"*0\r\n", [])
    assert b"gone" in s._data, "an expired key filtered out must not be deleted"
    assert s.take_effects() == [], "a listing must not propagate a DEL"


# --- KEYS: the glob grammar ---------------------------------------------------------------


@pytest.mark.parametrize("pattern, preload, expected", [
    # a proper a-z range accepts a lowercase letter and rejects an uppercase one and a digit
    (b"a[a-z]b", [b"axb", b"aXb", b"a1b"], [b"axb"]),
    # a lone '-' that does not open a low-high range is a literal
    (b"a[-]b", [b"a-b", b"axb"], [b"a-b"]),
    # '*' has no special meaning inside a class
    (b"a[*]b", [b"a*b", b"axb"], [b"a*b"]),
    # a class of ordinary alternatives
    (b"[aL]*", [b"a1", b"L9", b"zz"], [b"L9", b"a1"]),
    # '?' is exactly one character, no more and no fewer
    (b"a?", [b"a1", b"ab", b"abc"], [b"a1", b"ab"]),
    # '^' negates a class
    (b"[^a]*", [b"a1", b"L"], [b"L"]),
    # '!' does not -- it is a literal, and the two spellings are not the same pattern
    (b"[!a]*", [b"a1", b"L"], [b"a1"]),
    # a backslash escapes the character after it, including the grammar's own wildcards
    (b"a\\*b", [b"a*b", b"axb"], [b"a*b"]),
    # a trailing lone backslash matches a literal backslash
    (b"a\\", [b"a\\", b"ab"], [b"a\\"]),
    # an empty pattern matches nothing among these non-empty keys
    (b"", [b"a1", b"ab"], []),
], ids=[
    "range", "literal-dash", "literal-star-in-class", "class-of-alternatives",
    "question-mark", "caret-negates", "bang-is-literal", "backslash-escape",
    "trailing-backslash", "empty-pattern",
])
def test_keys_glob_grammar(store, conn, pattern, preload, expected):
    for key in preload:
        commands.dispatch(store, conn, [b"SET", key, b"v"])
    reply, effects = commands.dispatch(store, conn, [b"KEYS", pattern])
    assert effects == []
    assert sorted(reply.split(b"\r\n")[2::2]) == sorted(expected)


def test_keys_star_matches_an_empty_string_key(store, conn):
    # the "a trailing run of stars matches the rest, including nothing" case only ran as
    # a loop-exit condition, which a key that is already empty on entry never reaches --
    # so this used to answer *0 even though the empty key was live and DBSIZE counted it
    commands.dispatch(store, conn, [b"SET", b"", b"v"])
    assert commands.dispatch(store, conn, [b"DBSIZE"]) == (b":1\r\n", [])
    reply, effects = commands.dispatch(store, conn, [b"KEYS", b"*"])
    assert effects == []
    assert sorted(reply.split(b"\r\n")[2::2]) == [b""]


@pytest.mark.parametrize("pattern, expected", [
    (b"*", [b""]),
    (b"**", [b""]),
    (b"*a", []),
], ids=["single-star", "double-star", "star-plus-content"])
def test_keys_star_variants_against_an_empty_string_key(store, conn, pattern, expected):
    # pins the boundary: any run of nothing but '*' matches an empty key, but a '*'
    # followed by further pattern content still needs a byte of key that isn't there
    commands.dispatch(store, conn, [b"SET", b"", b"v"])
    reply, effects = commands.dispatch(store, conn, [b"KEYS", pattern])
    assert effects == []
    assert sorted(reply.split(b"\r\n")[2::2]) == expected


def test_keys_matching_against_many_non_consecutive_stars_stays_fast(store, conn):
    # a naive backtracking matcher retries every split point at every '*', which is
    # exponential in the number of stars rather than in pattern length -- 20 stars
    # against a key with no trailing 'y' took 10.8s unmemoised on the machine this was
    # measured on, next to single-digit milliseconds for a matcher that remembers a
    # (pattern index, key index) pair it has already resolved. The budget below sits
    # far under that 10.8s so a real regression still fails it, with enough room above
    # a healthy run that a loaded machine does not flake it. A second key that the same
    # pattern does match sits in the same store, so a matcher that is fast only because
    # it is also wrong -- for instance one that always answers "no match" -- cannot pass
    # by returning a coincidentally cheap answer
    commands.dispatch(store, conn, [b"SET", b"x" * 25, b"v"])
    commands.dispatch(store, conn, [b"SET", b"x" * 24 + b"y", b"v"])
    pattern = (b"*x" * 20) + b"y"
    started = time.monotonic()
    reply, effects = commands.dispatch(store, conn, [b"KEYS", pattern])
    elapsed = time.monotonic() - started
    assert sorted(reply.split(b"\r\n")[2::2]) == [b"x" * 24 + b"y"], (
        "only the key with a trailing 'y' must match"
    )
    assert effects == []
    assert elapsed < 3.0, "KEYS took %.2fs against an adversarial pattern" % elapsed


def test_keys_matching_against_thousands_of_non_consecutive_stars_does_not_recurse(store, conn):
    # each non-consecutive '*' cost one Python stack frame in an earlier version of this
    # matcher, because retrying the rest of the pattern after a star went through a
    # fresh recursive call rather than a loop -- a pattern built from enough of them
    # raised RecursionError well below any pattern length a client could be stopped
    # from sending. 2000 star groups sits well past where that used to break
    star_groups = 2000
    matching = b"x" * (star_groups + 5) + b"y"
    other = b"x" * (star_groups + 5) + b"z"
    commands.dispatch(store, conn, [b"SET", matching, b"v"])
    commands.dispatch(store, conn, [b"SET", other, b"v"])
    pattern = (b"*x" * star_groups) + b"y"
    reply, effects = commands.dispatch(store, conn, [b"KEYS", pattern])
    assert effects == []
    assert sorted(reply.split(b"\r\n")[2::2]) == [matching]


# --- FLUSHALL ------------------------------------------------------------------------------


def test_flushall_empties_every_kind_of_value(store, conn):
    commands.dispatch(store, conn, [b"SET", b"s", b"v"])
    commands.dispatch(store, conn, [b"RPUSH", b"l", b"a"])
    assert commands.dispatch(store, conn, [b"FLUSHALL"]) == (b"+OK\r\n", [[b"FLUSHALL"]])
    assert store._data == {} and store._expiry == {}


def test_flushall_on_an_already_empty_keyspace_still_propagates(store, conn):
    # unlike DEL's "changed nothing propagates nothing": a follower's keyspace may not be
    # empty even when the leader's is, and it still needs the instruction to catch up
    assert commands.dispatch(store, conn, [b"FLUSHALL"]) == (b"+OK\r\n", [[b"FLUSHALL"]])


# --- INFO ----------------------------------------------------------------------------------


def test_info_with_no_arguments_reports_every_section(store, conn):
    commands.dispatch(store, conn, [b"SET", b"k", b"v"])
    commands.dispatch(store, conn, [b"EXPIRE", b"k", b"100"])
    response, effects = commands.dispatch(store, conn, [b"INFO"])
    assert effects == []
    assert response.startswith(b"$"), "INFO answers a bulk string"
    body = response.split(b"\r\n", 1)[1][:-2]
    info = parse_info(body.decode("ascii"))

    assert info["redis_version"] == "0.1.0"
    assert info["role"] == "master"
    assert info["connected_clients"] == 0, "no server is attached to this connection"
    assert info["db0"] == {"keys": 1, "expires": 1}
    assert isinstance(info["used_memory"], int)
    # exactly this field set: no uptime, which would need time.monotonic(), and no
    # persistence fields, which would anticipate a feature this one does not build
    assert set(info) == {
        "redis_version", "connected_clients", "used_memory", "role", "db0",
    }, info


def test_info_omits_the_db0_line_on_an_empty_keyspace(store, conn):
    response, _effects = commands.dispatch(store, conn, [b"INFO", b"keyspace"])
    body = response.split(b"\r\n", 1)[1][:-2]
    assert b"# Keyspace" in body
    assert b"db0" not in body


def test_info_filters_to_the_named_section_only(store, conn):
    response, _effects = commands.dispatch(store, conn, [b"INFO", b"replication"])
    body = response.split(b"\r\n", 1)[1][:-2]
    assert b"role:master" in body
    assert b"redis_version" not in body
    assert b"connected_clients" not in body


def test_info_section_names_are_matched_case_insensitively(store, conn):
    lower, _ = commands.dispatch(store, conn, [b"INFO", b"replication"])
    upper, _ = commands.dispatch(store, conn, [b"INFO", b"REPLICATION"])
    mixed, _ = commands.dispatch(store, conn, [b"INFO", b"Replication"])
    assert lower == upper == mixed


def test_info_on_an_unrecognised_section_is_an_empty_bulk_string(store, conn):
    assert commands.dispatch(store, conn, [b"INFO", b"nosuchsection"]) == (b"$0\r\n\r\n", [])


def test_info_takes_more_than_one_section_name(store, conn):
    response, _effects = commands.dispatch(store, conn, [b"INFO", b"server", b"replication"])
    body = response.split(b"\r\n", 1)[1][:-2]
    assert b"redis_version" in body and b"role:master" in body
    assert b"connected_clients" not in body


def test_used_memory_agrees_with_an_independent_reading_of_the_same_process():
    response, _effects = commands.dispatch(Store(), None, [b"INFO", b"memory"])
    body = response.split(b"\r\n", 1)[1][:-2]
    info = parse_info(body.decode("ascii"))
    assert isinstance(info["used_memory"], int)

    ru_maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    independent = ru_maxrss if sys.platform == "darwin" else ru_maxrss * 1024
    ratio = info["used_memory"] / independent
    assert 0.1 < ratio < 10, (info["used_memory"], independent, ratio)


# --- the connection's third slot ------------------------------------------------------------


def test_the_server_slot_is_filled_on_accept_and_cleared_on_close():
    with listening() as (server, connect, _listener):
        connect()
        pump(server)
        assert len(server._connections) == 1
        conn = next(iter(server._connections))
        assert conn.server is server

        server._close(conn)
        assert conn.server is None


def test_connected_clients_counts_every_tracked_connection():
    with listening() as (server, connect, _listener):
        connect()
        connect()
        for _ in range(10):
            server._loop.run_once()
            if len(server._connections) == 2:
                break
        assert len(server._connections) == 2
        assert server.connected_clients == 2


def test_info_reads_connected_clients_through_the_slot():
    with listening() as (server, connect, _listener):
        connect()
        pump(server)
        conn = next(iter(server._connections))
        response, _effects = commands.dispatch(server._store, conn, [b"INFO", b"clients"])
        info = parse_info(response.split(b"\r\n", 1)[1].decode("ascii"))
        assert info["connected_clients"] == 1


def test_info_reports_zero_connected_clients_with_no_server_attached():
    # conn=None is this project's own convention for driving a handler with no socket at
    # all -- tests/conftest.py's r() helper does exactly this -- and a bare Connection
    # built directly rather than accepted carries the same None in its third slot
    response, _effects = commands.dispatch(Store(), None, [b"INFO", b"clients"])
    info = parse_info(response.split(b"\r\n", 1)[1].decode("ascii"))
    assert info["connected_clients"] == 0

    a, b = socket.socketpair()
    bare = Connection(a, ("127.0.0.1", 0))
    try:
        response, _effects = commands.dispatch(Store(), bare, [b"INFO", b"clients"])
        info = parse_info(response.split(b"\r\n", 1)[1].decode("ascii"))
        assert info["connected_clients"] == 0
    finally:
        a.close()
        b.close()


# --- CONFIG --------------------------------------------------------------------------------


def test_config_get_echoes_the_parameter_with_an_empty_value(store, conn):
    assert commands.dispatch(store, conn, [b"CONFIG", b"GET", b"save"]) == (
        b"*2\r\n$4\r\nsave\r\n$0\r\n\r\n", [])


def test_config_get_answers_the_same_empty_value_for_any_parameter_name(store, conn):
    assert commands.dispatch(store, conn, [b"CONFIG", b"GET", b"nosuchparam"]) == (
        b"*2\r\n$11\r\nnosuchparam\r\n$0\r\n\r\n", [])


def test_config_subcommand_is_matched_case_insensitively(store, conn):
    assert commands.dispatch(store, conn, [b"CONFIG", b"get", b"save"])[0] == (
        b"*2\r\n$4\r\nsave\r\n$0\r\n\r\n")


# --- error cases -----------------------------------------------------------------------


@pytest.mark.parametrize("argv, expected", [
    ([b"DBSIZE", b"x"], b"-ERR wrong number of arguments for 'dbsize' command\r\n"),
    ([b"KEYS"], b"-ERR wrong number of arguments for 'keys' command\r\n"),
    ([b"KEYS", b"*", b"extra"], b"-ERR wrong number of arguments for 'keys' command\r\n"),
    ([b"FLUSHALL", b"ASYNC"], b"-ERR wrong number of arguments for 'flushall' command\r\n"),
    ([b"CONFIG"], b"-ERR wrong number of arguments for 'config' command\r\n"),
])
def test_arity_errors(store, conn, argv, expected):
    assert commands.dispatch(store, conn, argv) == (expected, [])


@pytest.mark.parametrize("argv", [
    [b"CONFIG", b"GET"],
    [b"CONFIG", b"GET", b"a", b"b"],
    [b"CONFIG", b"SET", b"save", b""],
    [b"CONFIG", b"BOGUS"],
])
def test_config_arity_and_syntax_errors_all_start_with_err(store, conn, argv):
    response, effects = commands.dispatch(store, conn, argv)
    assert response.startswith(b"-ERR ")
    assert effects == []
