"""The list half: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, and the introspection and
admin commands -- DBSIZE, KEYS, FLUSHALL, INFO, CONFIG GET -- against a real server
subprocess, driven through redis-py rather than through dispatch() directly.
"""

import redis


# --- normal cases -----------------------------------------------------------------------


def test_rpush_and_lrange_round_trip_in_push_order(redis_client):
    r = redis_client
    assert r.rpush("l", "a", "b", "c") == 3
    assert r.lrange("l", 0, -1) == [b"a", b"b", b"c"]


def test_lpush_prepends_so_the_range_comes_back_reversed(redis_client):
    r = redis_client
    assert r.lpush("l", "a", "b", "c") == 3
    assert r.lrange("l", 0, -1) == [b"c", b"b", b"a"]


def test_llen_reports_the_element_count(redis_client):
    r = redis_client
    r.rpush("l", "a", "b")
    assert r.llen("l") == 2


def test_lpop_and_rpop_take_the_element_from_the_matching_end(redis_client):
    r = redis_client
    r.rpush("l", "a", "b", "c")
    assert r.lpop("l") == b"a"
    assert r.rpop("l") == b"c"
    assert r.lrange("l", 0, -1) == [b"b"]


def test_push_and_pop_round_trip_a_non_utf8_element(redis_client):
    r = redis_client
    value = b"\xff\x00\xfe\x80binary"
    r.rpush("bin", value)
    assert r.lrange("bin", 0, -1) == [value]
    assert r.lpop("bin") == value


def test_dbsize_counts_keys_of_every_kind(redis_client):
    r = redis_client
    r.set("s", "v")
    r.rpush("l", "a")
    assert r.dbsize() == 2


def test_keys_lists_every_key_matching_the_pattern(redis_client):
    r = redis_client
    r.rpush("alpha", "a")
    r.rpush("beta", "b")
    r.set("gamma", "v")
    assert sorted(r.keys("*")) == [b"alpha", b"beta", b"gamma"]


def test_flushall_empties_every_kind_of_value(redis_client):
    r = redis_client
    r.set("s", "v")
    r.rpush("l", "a")
    assert r.flushall() is True
    assert r.dbsize() == 0


def test_info_with_no_section_reports_the_full_set_of_fields(redis_client):
    r = redis_client
    r.set("k", "v")
    info = r.info()
    assert info["redis_version"] == "0.1.0"
    assert info["role"] == "master"
    assert info["connected_clients"] == 1
    assert info["db0"] == {"keys": 1, "expires": 0}
    assert isinstance(info["used_memory"], int)
    assert set(info) == {
        "redis_version", "connected_clients", "used_memory", "role", "db0",
    }, info


def test_info_with_a_section_name_filters_to_it(redis_client):
    r = redis_client
    assert r.info("replication") == {"role": "master"}


def test_config_get_echoes_the_parameter_with_an_empty_value(redis_client):
    r = redis_client
    assert r.config_get("save") == {"save": ""}


def test_config_get_answers_the_same_empty_value_for_any_parameter_name(redis_client):
    r = redis_client
    assert r.config_get("nosuchparam") == {"nosuchparam": ""}


# --- edge cases --------------------------------------------------------------------------


def test_lpop_and_rpop_on_a_missing_key_answer_none(redis_client):
    r = redis_client
    assert r.lpop("nosuchlist") is None
    assert r.rpop("nosuchlist") is None


def test_lrange_on_a_missing_key_is_an_empty_list(redis_client):
    r = redis_client
    assert r.lrange("nosuchlist", 0, -1) == []


def test_llen_on_a_missing_key_is_zero(redis_client):
    r = redis_client
    assert r.llen("nosuchlist") == 0


def test_push_onto_an_existing_list_appends_rather_than_replacing(redis_client):
    r = redis_client
    r.rpush("l", "a")
    assert r.rpush("l", "b") == 2
    assert r.lrange("l", 0, -1) == [b"a", b"b"]


def test_popping_the_last_element_removes_the_key_entirely(redis_client):
    r = redis_client
    r.rpush("l", "a")
    assert r.lpop("l") == b"a"
    assert r.exists("l") == 0
    assert r.type("l") == b"none"


def test_flushall_on_an_already_empty_keyspace_still_answers_ok(redis_client):
    r = redis_client
    assert r.flushall() is True


# --- error cases -------------------------------------------------------------------------


def test_lpop_with_a_count_is_a_wrong_arity_error(redis_client):
    # redis-py's lpop(name, count) sends "LPOP name count" -- a count this server does
    # not implement. arity is exactly 2, so the trailing count is rejected outright
    # rather than accepted and silently ignored, and this pins that divergence instead
    # of avoiding the call that would surface it
    r = redis_client
    r.rpush("l", "a", "b")
    try:
        r.lpop("l", 2)
    except redis.ResponseError as exc:
        assert str(exc) == "wrong number of arguments for 'lpop' command", exc
    else:
        raise AssertionError("lpop(name, count) answered instead of erroring")


def test_rpop_with_a_count_is_a_wrong_arity_error(redis_client):
    r = redis_client
    r.rpush("l", "a", "b")
    try:
        r.rpop("l", 2)
    except redis.ResponseError as exc:
        assert str(exc) == "wrong number of arguments for 'rpop' command", exc
    else:
        raise AssertionError("rpop(name, count) answered instead of erroring")
