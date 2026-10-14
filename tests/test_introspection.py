"""DBSIZE, KEYS, FLUSHALL, INFO, CONFIG: the introspection and admin commands, KEYS's
hand-written glob grammar, and the two things read through the connection's third slot:
the connected-client count INFO reports and the save rule CONFIG GET answers.
"""

import resource
import socket
import sys
import time

import pytest
from redis._parsers.helpers import parse_info

import commands
from commands.server import _glob_match
from connection import Connection
from server import Server
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
    # routed through live_count() rather than the expiry-aware lookup: DBSIZE and KEYS are the deliberate
    # exception, because a count or listing that deleted what it scanned would be a read that mutates
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


@pytest.mark.parametrize("pattern, preload, expected", [
    # every expectation is redis-server 7.2.7's answer. an unclosed class runs the matcher off the end,
    # and the three guards in _match_atom that step back are uncovered otherwise: IndexError, or nothing
    (b"[", [b"a", b"[", b"[a", b""], []),
    (b"[^", [b"a", b"^", b"[^", b""], [b"^", b"a"]),
    (b"[a", [b"a", b"b", b"[a", b"[", b""], [b"a"]),
    # a dash as the last byte before the end: the range branch would read one past the
    # pattern if its length test admitted this
    (b"[a-", [b"a", b"-", b"b", b"[", b""], [b"-", b"a"]),
    (b"[ab-", [b"a", b"b", b"-", b"c", b""], [b"-", b"a", b"b"]),
    (b"a[b-", [b"ab", b"a-", b"ac", b"a"], [b"a-", b"ab"]),
    # a backslash as the last byte: the escape branch would read one past it too
    (b"[\\", [b"\\", b"a", b"[", b""], [b"\\"]),
    # closed, and not the class it looks like: `a-]` is a range whose endpoints swap to
    # ']'..'a', so it matches ']' as well -- not 'a' plus a literal dash
    (b"[a-]", [b"a", b"-", b"b", b"]"], [b"]", b"a"]),
], ids=[
    "bare-bracket", "bracket-caret", "unclosed-member", "unclosed-trailing-dash",
    "unclosed-two-members-and-dash", "unclosed-after-a-literal", "unclosed-trailing-backslash",
    "closed-range-ending-in-bracket",
])
def test_keys_matches_a_character_class_that_is_never_closed(
    store, conn, pattern, preload, expected
):
    for key in preload:
        commands.dispatch(store, conn, [b"SET", key, b"v"])
    reply, effects = commands.dispatch(store, conn, [b"KEYS", pattern])
    assert effects == []
    assert sorted(reply.split(b"\r\n")[2::2]) == sorted(expected)


@pytest.mark.parametrize("pattern, preload, expected", [
    # again 7.2.7's own answers. every case needs the `*` retried more than one byte along, which the ten
    # grammar cases never reach: `star_k = k + 1` leaves each answering only its shortest match
    (b"a*b", [b"ab", b"axb", b"axyb", b"axyzb", b"ab_", b"a", b"b"],
     [b"ab", b"axb", b"axyb", b"axyzb"]),
    (b"a**b", [b"ab", b"axb", b"axyzb", b"a", b"b"], [b"ab", b"axb", b"axyzb"]),
    (b"*a*b*", [b"ab", b"xaybz", b"aXXb", b"ba", b"a", b"b"], [b"aXXb", b"ab", b"xaybz"]),
    (b"*abc", [b"abc", b"xabc", b"xxabc", b"abcx", b"ab"], [b"abc", b"xabc", b"xxabc"]),
    (b"a*b*c", [b"abc", b"axbyc", b"axxbyyc", b"abc_", b"acb"],
     [b"abc", b"axbyc", b"axxbyyc"]),
    (b"*[a-c]*", [b"a", b"xbx", b"zzz", b"c", b"d"], [b"a", b"c", b"xbx"]),
    (b"?*?", [b"ab", b"abc", b"a", b"abcd"], [b"ab", b"abc", b"abcd"]),
], ids=[
    "star-spans-one-to-three", "two-stars-span", "three-stars", "leading-star",
    "two-separated-stars", "star-around-a-class", "star-between-two-single-bytes",
])
def test_keys_extends_a_star_across_several_bytes_before_the_rest_matches(
    store, conn, pattern, preload, expected
):
    for key in preload:
        commands.dispatch(store, conn, [b"SET", key, b"v"])
    reply, effects = commands.dispatch(store, conn, [b"KEYS", pattern])
    assert effects == []
    assert sorted(reply.split(b"\r\n")[2::2]) == sorted(expected)


def test_keys_star_matches_an_empty_string_key(store, conn):
    # this used to answer *0 for a live empty key DBSIZE counted. the answer now comes from keys()'s
    # one-byte-'*' short-circuit, which is where the reference's comes from, and the test below pins both
    commands.dispatch(store, conn, [b"SET", b"", b"v"])
    assert commands.dispatch(store, conn, [b"DBSIZE"]) == (b":1\r\n", [])
    reply, effects = commands.dispatch(store, conn, [b"KEYS", b"*"])
    assert effects == []
    assert sorted(reply.split(b"\r\n")[2::2]) == [b""]


@pytest.mark.parametrize("pattern, expected", [
    (b"*", [b""]),
    (b"**", []),
    (b"***", []),
    (b"*a", []),
    (b"", [b""]),
], ids=["single-star", "double-star", "triple-star", "star-plus-content", "empty"])
def test_keys_star_variants_against_an_empty_string_key(store, conn, pattern, expected):
    # measured against redis-server 7.2.7: an empty key matches only the empty pattern and the one-byte
    # '*' -- the first through the matcher, the second through keys()'s short-circuit; longer runs match none
    commands.dispatch(store, conn, [b"SET", b"", b"v"])
    reply, effects = commands.dispatch(store, conn, [b"KEYS", pattern])
    assert effects == []
    assert sorted(reply.split(b"\r\n")[2::2]) == expected


def test_the_matcher_itself_refuses_every_star_pattern_against_an_empty_key(store, conn):
    # the layer below: keys()'s short-circuit is the only reason '*' lists an empty key, so a repair that
    # moved the answer into the matcher would pass every KEYS assertion and still diverge on '**'
    assert _glob_match(b"", b"") is True
    for pattern in (b"*", b"**", b"***", b"*a", b"?"):
        assert _glob_match(pattern, b"") is False, pattern


@pytest.mark.parametrize("pattern, preload, expected", [
    # a range straddling 0x80 is read signed, as the reference reads it: 'a' is 97 and \xff is -1, so the
    # swap fires and the range is -1..97 -- every byte at or below 'a' plus \xff, not 'b', 'c' or \x80
    (b"[a-\xff]", [b" ", b"0", b"a", b"b", b"c", b"\x7f", b"\x80", b"\xff"],
     [b" ", b"0", b"a", b"\xff"]),
    # written the other way round it is the same range, because the swap normalises it
    (b"[\xff-a]", [b" ", b"a", b"b", b"\xff"], [b" ", b"a", b"\xff"]),
    # the widest-looking range in the grammar matches almost nothing: 0 and -1, swapped
    (b"[\x00-\xff]", [b" ", b"a", b"\x80", b"\xff"], [b"\xff"]),
    # both endpoints above 0x80: signed and unsigned readings agree, so this is unchanged
    (b"[\x80-\xff]", [b"a", b"\x80", b"\xc3", b"\xff"], [b"\x80", b"\xc3", b"\xff"]),
    # both endpoints below it: likewise unchanged
    (b"[a-c]", [b"a", b"b", b"c", b"d", b"\xff"], [b"a", b"b", b"c"]),
    # a high byte as a plain class member is an equality test, which no reading changes
    (b"[\xff]", [b"a", b"\x80", b"\xff"], [b"\xff"]),
], ids=[
    "straddles-0x80", "straddles-reversed", "widest-range-is-inverted",
    "both-endpoints-high", "both-endpoints-low", "high-byte-literal-member",
])
def test_keys_class_ranges_read_bytes_the_way_the_reference_does(
    store, conn, pattern, preload, expected
):
    # measured against redis-server 7.2.7, which walks pattern and key through a signed `char`: a range
    # with exactly one endpoint at or above 0x80 orders its endpoints differently. equality is unaffected
    for key in preload:
        commands.dispatch(store, conn, [b"SET", key, b"v"])
    reply, effects = commands.dispatch(store, conn, [b"KEYS", pattern])
    assert effects == []
    assert sorted(reply.split(b"\r\n")[2::2]) == sorted(expected)


def test_keys_matching_against_many_non_consecutive_stars_stays_fast(store, conn):
    # naive backtracking is exponential in the number of stars: 20 stars against a key with no trailing
    # 'y' took 10.8s where the shipped matcher took single-digit ms. a key that does match is the control
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
    # each non-consecutive '*' cost one Python stack frame when the retry recursed rather than looped, so
    # enough of them raised RecursionError below any length a client can be stopped at; 2000 is well past
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
    assert info["blocked_clients"] == 0, "nothing in this server blocks a client, ever"
    assert info["total_connections_received"] == 0, "no server is attached to this connection"
    assert info["total_commands_processed"] == 0, "no server is attached to this connection"
    assert info["db0"] == {"keys": 1, "expires": 1}
    assert isinstance(info["used_memory"], int)
    # exactly this field set: no uptime, which needs a monotonic clock, and no persistence fields. the three
    # beyond the original five are what redis-cli --stat renders as LLONG_MIN when missing, three of six
    assert set(info) == {
        "redis_version", "connected_clients", "blocked_clients", "used_memory",
        "total_connections_received", "total_commands_processed", "role", "db0",
    }, info


def test_the_three_counters_stat_reads_are_zero_for_a_connection_no_server_accepted(store, conn):
    # the same two-level guard connected_clients has: conn is None for in-process dispatch, conn.server is
    # None for a connection built rather than accepted, and a counter that raised would take INFO down
    for probe in (conn, None):
        response, _effects = commands.dispatch(store, probe, [b"INFO"])
        info = parse_info(response.split(b"\r\n", 1)[1][:-2].decode("ascii"))
        assert info["total_connections_received"] == 0, probe
        assert info["total_commands_processed"] == 0, probe
        assert info["blocked_clients"] == 0, probe


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


def _info_fields(store, conn, argv):
    # the field names only, never their values: used_memory is a resident-memory high-water mark, so two
    # INFO calls a moment apart can legitimately disagree and whole-body comparison would flake
    response, effects = commands.dispatch(store, conn, argv)
    assert effects == []
    body = response.split(b"\r\n", 1)[1][:-2]
    return set(parse_info(body.decode("ascii")))


@pytest.mark.parametrize("selector", [b"all", b"everything", b"default", b"ALL", b"Default"])
def test_info_whole_report_selectors_report_every_section(store, conn, selector):
    # 'all', 'everything' and 'default' are the reference's whole-report selectors, not section names --
    # and a client spelling out the bare form's own meaning used to get a zero-byte body back
    commands.dispatch(store, conn, [b"SET", b"k", b"v"])
    assert _info_fields(store, conn, [b"INFO", selector]) == _info_fields(
        store, conn, [b"INFO"]
    )


def test_a_whole_report_selector_beside_a_section_name_widens_rather_than_narrows(store, conn):
    # the reference's reading of `INFO default Server`: the selector wins, so the reply
    # is the whole report and not the one section named beside it
    commands.dispatch(store, conn, [b"SET", b"k", b"v"])
    assert _info_fields(store, conn, [b"INFO", b"default", b"Server"]) == _info_fields(
        store, conn, [b"INFO"]
    )


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
    # conn=None is this project's convention for driving a handler with no socket -- conftest's r() does
    # it -- and a bare Connection built rather than accepted carries the same None in its third slot
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


def test_config_get_answers_a_real_parameter_from_the_table(store, conn):
    # every value is a true statement about the server answering it: conn was built rather than accepted,
    # so nothing saves and save's "" is the true answer. three of the five equal 7.2.7's own default
    assert commands.dispatch(store, conn, [b"CONFIG", b"GET", b"save"]) == (
        b"*2\r\n$4\r\nsave\r\n$0\r\n\r\n", [])
    assert commands.dispatch(store, conn, [b"CONFIG", b"GET", b"appendonly"]) == (
        b"*2\r\n$10\r\nappendonly\r\n$2\r\nno\r\n", [])
    assert commands.dispatch(store, conn, [b"CONFIG", b"GET", b"maxmemory"]) == (
        b"*2\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n", [])


def test_config_get_answers_an_unknown_parameter_with_an_empty_array(store, conn):
    # not the name back with an empty value, which is what an echo would do: a client cannot tell an
    # unknown parameter from an unset one, and the reference answers the empty array here too
    assert commands.dispatch(store, conn, [b"CONFIG", b"GET", b"nosuchparam"]) == (
        b"*0\r\n", [])
    assert commands.dispatch(store, conn, [b"CONFIG", b"GET", b""]) == (b"*0\r\n", [])


def test_config_get_looks_an_exact_name_up_without_regard_to_case(store, conn):
    # and answers in the spelling the client used, not the canonical one -- measured
    # against redis-server 7.2.7, which answers `MAXMEMORY` to `CONFIG GET MAXMEMORY`
    assert commands.dispatch(store, conn, [b"CONFIG", b"GET", b"MAXMEMORY"]) == (
        b"*2\r\n$9\r\nMAXMEMORY\r\n$1\r\n0\r\n", [])
    assert commands.dispatch(store, conn, [b"CONFIG", b"GET", b"MaxMemory-Policy"]) == (
        b"*2\r\n$16\r\nMaxMemory-Policy\r\n$10\r\nnoeviction\r\n", [])


@pytest.mark.parametrize("pattern, expected", [
    # a pattern is glob-matched over the table and answers canonical names, where an exact name answers
    # the client's spelling -- the reference's own line, and `*` is what redis-py's config_get() sends
    (b"*", [b"appendonly", b"no", b"databases", b"1", b"maxmemory", b"0",
            b"maxmemory-policy", b"noeviction", b"save", b""]),
    (b"maxmemory*", [b"maxmemory", b"0", b"maxmemory-policy", b"noeviction"]),
    (b"ma?memory", [b"maxmemory", b"0"]),
    (b"[m]axmemory", [b"maxmemory", b"0"]),
    (b"MAXMEMORY*", [b"maxmemory", b"0", b"maxmemory-policy", b"noeviction"]),
    (b"nosuch*", []),
], ids=["star", "prefix", "question-mark", "class", "upper-case-pattern", "matches-nothing"])
def test_config_get_glob_matches_a_pattern_over_the_table(store, conn, pattern, expected):
    reply, effects = commands.dispatch(store, conn, [b"CONFIG", b"GET", pattern])
    assert effects == []
    assert reply.split(b"\r\n")[2:-1:2] == expected


@pytest.mark.parametrize("pattern, expected", [
    # the fold happens inside the comparison and, for a range, AFTER the swap -- the reference's order:
    # `[Z-a]` is 90..97 and folds to 122..97, matching nothing, where lowering first would match maxmemory
    (b"[Z-a]axmemory", []),
    (b"[a-Z]axmemory", []),
    (b"[M-a]axmemory", []),
    # ranges that do not straddle a case boundary are unaffected by the order
    (b"[A-z]axmemory", [b"maxmemory", b"0"]),
    (b"[L-N]axmemory", [b"maxmemory", b"0"]),
    (b"[l-n]axmemory", [b"maxmemory", b"0"]),
    (b"[m-m]axmemory", [b"maxmemory", b"0"]),
    # and the ordinary folding a client would expect from the flag
    (b"MAXMEMORY*", [b"maxmemory", b"0", b"maxmemory-policy", b"noeviction"]),
    (b"SaVe", [b"SaVe", b""]),
], ids=[
    "upper-lower-range", "lower-upper-range", "mid-upper-lower-range",
    "spans-both-cases", "both-upper", "both-lower", "single-byte",
    "upper-prefix-glob", "mixed-case-exact",
])
def test_config_get_folds_case_inside_the_comparison_not_before_it(
    store, conn, pattern, expected
):
    reply, effects = commands.dispatch(store, conn, [b"CONFIG", b"GET", pattern])
    assert effects == []
    assert reply.split(b"\r\n")[2:-1:2] == expected


def test_keys_never_folds_case_however_config_get_matches(store, conn):
    # the flag belongs to CONFIG GET's caller alone: a key is arbitrary bytes and its case is the client's
    # business, so the pattern that folds over the parameter table must not fold over the keyspace
    for key in (b"maxmemory", b"MAXMEMORY", b"MaxMemory"):
        commands.dispatch(store, conn, [b"SET", key, b"v"])
    reply, _ = commands.dispatch(store, conn, [b"KEYS", b"MAXMEMORY"])
    assert sorted(reply.split(b"\r\n")[2::2]) == [b"MAXMEMORY"]
    reply, _ = commands.dispatch(store, conn, [b"KEYS", b"[Z-a]axmemory"])
    assert reply == b"*0\r\n"


def test_config_get_answers_every_pattern_it_is_given_and_never_twice(store, conn):
    # the reference deduplicates across patterns -- `CONFIG GET * maxmemory` answers 195
    # pairs there, not 196 -- so a parameter two patterns both reach is answered once
    assert commands.dispatch(store, conn, [b"CONFIG", b"GET", b"save", b"appendonly"]) == (
        b"*4\r\n$4\r\nsave\r\n$0\r\n\r\n$10\r\nappendonly\r\n$2\r\nno\r\n", [])
    assert commands.dispatch(store, conn, [b"CONFIG", b"GET", b"maxmemory", b"maxmemory"]) == (
        b"*2\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n", [])
    # [2:-1:2] is the flat name/value run; every other element of that is a name
    def names_of(reply):
        return reply.split(b"\r\n")[2:-1:2][::2]

    # a glob followed by an exact name the glob already reached
    reply, _ = commands.dispatch(store, conn, [b"CONFIG", b"GET", b"*", b"maxmemory"])
    assert names_of(reply) == [
        b"appendonly", b"databases", b"maxmemory", b"maxmemory-policy", b"save"]
    # and two globs that overlap, which the exact-name case never reaches: on 7.2.7 `CONFIG GET maxmemory*
    # ma*` answers eight names with no repeat, and `CONFIG GET * *` answers its whole table once
    reply, _ = commands.dispatch(store, conn, [b"CONFIG", b"GET", b"maxmemory*", b"ma*"])
    assert names_of(reply) == [b"maxmemory", b"maxmemory-policy"], names_of(reply)
    reply, _ = commands.dispatch(store, conn, [b"CONFIG", b"GET", b"*", b"*"])
    assert len(names_of(reply)) == len(set(names_of(reply))) == 5, names_of(reply)


def test_config_subcommand_is_matched_case_insensitively(store, conn):
    assert commands.dispatch(store, conn, [b"CONFIG", b"get", b"save"])[0] == (
        b"*2\r\n$4\r\nsave\r\n$0\r\n\r\n")


def _config_get_through(server, *names):
    # a connection holding server in its third slot, which is what _on_accept leaves, so these tests need
    # no listener; run() arms the save before the listener, and _arm_periodic_tasks() arms it the same way
    a, b = socket.socketpair()
    try:
        conn = Connection(a, ("127.0.0.1", 0))
        conn.server = server
        return commands.dispatch(Store(), conn, [b"CONFIG", b"GET", *names])
    finally:
        a.close()
        b.close()


@pytest.mark.parametrize("interval, with_path, expected", [
    # a reference rule fires once its whole-second clock is MORE than the rule's seconds past the last save,
    # so every N seconds is `N-1 0`: on 7.2.7 `save "1 0"` saved every 2 seconds and `save "3 0"` every 4
    (60, True, b"*2\r\n$4\r\nsave\r\n$4\r\n59 0\r\n"),
    (3, True, b"*2\r\n$4\r\nsave\r\n$3\r\n2 0\r\n"),
    # a rule 7.2.7's own parser refuses -- its seconds must be at least 1 -- and still the
    # one spelling of a save every second; `1 0` would claim every two
    (1, True, b"*2\r\n$4\r\nsave\r\n$3\r\n0 0\r\n"),
    # the two servers _arm_periodic_tasks() arms no save for, both answered with the
    # reference's own snapshotting-off value
    (0, True, b"*2\r\n$4\r\nsave\r\n$0\r\n\r\n"),
    (60, False, b"*2\r\n$4\r\nsave\r\n$0\r\n\r\n"),
], ids=["cli-default", "three-seconds", "one-second", "interval-zero", "no-path"])
def test_config_get_save_spells_the_attached_servers_own_rule(
    tmp_path, interval, with_path, expected
):
    path = str(tmp_path / "dump.mrdb") if with_path else None
    server = Server(0, snapshot_path=path, snapshot_interval=interval)
    try:
        server._arm_periodic_tasks()
        assert _config_get_through(server, b"save") == (expected, [])
    finally:
        server._loop.close()


def test_config_get_save_is_the_off_value_until_the_save_is_armed(tmp_path):
    # the reply follows the save arm and not the two settings that feed it: a Server driven without run()
    # has a path and an interval and will never save, and `59 0` there would promise saves that never come
    server = Server(0, snapshot_path=str(tmp_path / "dump.mrdb"), snapshot_interval=60)
    try:
        assert _config_get_through(server, b"save") == (
            b"*2\r\n$4\r\nsave\r\n$0\r\n\r\n", [])
        server._arm_periodic_tasks()
        assert _config_get_through(server, b"save") == (
            b"*2\r\n$4\r\nsave\r\n$4\r\n59 0\r\n", [])
    finally:
        server._loop.close()


def test_config_get_save_follows_a_run_from_armed_to_returned(tmp_path):
    # the other end: run() arms the save before the listener and disarms it on the way out, so a deadline
    # left set at shutdown went on answering `59 0` for a server that would never save again
    during = []

    class StopsAfterOneTick(Server):
        def _tick(self):
            during.append(_config_get_through(self, b"save"))
            self._running = False
            super()._tick()

    server = StopsAfterOneTick(0, snapshot_path=str(tmp_path / "dump.mrdb"),
                               snapshot_interval=60)
    server.run()
    assert during == [(b"*2\r\n$4\r\nsave\r\n$4\r\n59 0\r\n", [])], during
    assert _config_get_through(server, b"save") == (b"*2\r\n$4\r\nsave\r\n$0\r\n\r\n", [])


def test_config_get_answers_save_alike_through_a_pattern_and_an_exact_name(tmp_path):
    # both branches of the handler have to read the per-call table: redis-py sends `CONFIG GET *`, so a
    # glob branch on a fixed table would go on telling that client persistence is off
    server = Server(0, snapshot_path=str(tmp_path / "dump.mrdb"), snapshot_interval=60)
    try:
        server._arm_periodic_tasks()
        for names, name in (([b"*"], b"save"), ([b"sa?e"], b"save"), ([b"[s]ave"], b"save"),
                            ([b"SAVE"], b"SAVE"), ([b"save", b"*"], b"save")):
            reply, effects = _config_get_through(server, *names)
            flat = reply.split(b"\r\n")[2:-1:2]
            assert effects == []
            assert dict(zip(flat[::2], flat[1::2])).get(name) == b"59 0", (names, flat)
            # and the name is answered once, however many of the patterns reach it
            assert flat[::2].count(name) == 1, (names, flat)
    finally:
        server._loop.close()


def test_config_get_save_is_the_off_value_with_no_server_attached():
    # conn=None is this project's convention for a handler driven with no socket -- conftest's r() does it
    # -- and there is no slot to read: a guard reaching conn.server before testing conn raises here
    assert commands.dispatch(Store(), None, [b"CONFIG", b"GET", b"save"]) == (
        b"*2\r\n$4\r\nsave\r\n$0\r\n\r\n", [])


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


@pytest.mark.parametrize("argv, expected", [
    # the bare form is the one arity error in this server that has a subcommand to name,
    # and the reference names it -- so this asserts `config|get`, not `config`
    ([b"CONFIG", b"GET"], b"-ERR wrong number of arguments for 'config|get' command\r\n"),
    # every subcommand but GET, including the two the reference answers `+OK` to, asserted byte for byte:
    # these four shapes are exactly the ones that diverge and a startswith() would pin none of them
    ([b"CONFIG", b"SET", b"save", b""], b"-ERR syntax error\r\n"),
    ([b"CONFIG", b"RESETSTAT"], b"-ERR syntax error\r\n"),
    ([b"CONFIG", b"BOGUS"], b"-ERR syntax error\r\n"),
    ([b"CONFIG", b"BOGUS", b"x", b"y"], b"-ERR syntax error\r\n"),
])
def test_config_subcommand_and_arity_errors(store, conn, argv, expected):
    assert commands.dispatch(store, conn, argv) == (expected, [])
