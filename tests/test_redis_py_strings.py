"""The string half: SET, GET, DEL, EXISTS, TYPE, the EXPIRE family, TTL, PTTL, INCR
and DECR against a real server subprocess, driven through redis-py rather than
through dispatch() directly.
"""

import time

import redis


# --- normal cases -----------------------------------------------------------------------


def test_set_get_round_trip_including_binary_keys_and_values(redis_client):
    r = redis_client
    assert r.set("k", "v") is True
    assert r.get("k") == b"v"
    assert r.set("b", b"\x00\xff\r\nkey") is True
    assert r.get("b") == b"\x00\xff\r\nkey"
    assert r.set(b"\x00\xffk", b"v") is True
    assert r.get(b"\x00\xffk") == b"v"


def test_type_exists_del(redis_client):
    r = redis_client
    r.set("k", "v")
    assert r.type("k") == b"string"
    assert r.exists("k", "nope") == 1
    assert r.delete("k", "nope") == 1


def test_set_with_ex_reports_the_ttl(redis_client):
    r = redis_client
    assert r.set("t", "v", ex=100) is True
    assert r.ttl("t") == 100


def test_expire_and_pexpire(redis_client):
    r = redis_client
    r.set("e1", "v")
    assert r.expire("e1", 50) is True
    assert r.ttl("e1") == 50
    r.set("e2", "v")
    assert r.pexpire("e2", 50_000) is True
    assert r.ttl("e2") == 50


def test_pexpireat_sets_an_absolute_deadline(redis_client):
    r = redis_client
    r.set("e3", "v")
    deadline_ms = int(time.time() * 1000) + 60_000
    assert r.pexpireat("e3", deadline_ms) is True
    assert 59 <= r.ttl("e3") <= 60


def test_hundred_command_pipeline_of_incr(redis_client):
    r = redis_client
    pipe = r.pipeline(transaction=False)
    for _ in range(100):
        pipe.execute_command("INCR", "p")
    assert pipe.execute()[-1] == 100


# --- edge cases --------------------------------------------------------------------------


def test_nx_and_xx_return_none_on_condition_failure(redis_client):
    r = redis_client
    assert r.set("c", "v1") is True
    assert r.set("c", "v2", nx=True) is None
    assert r.get("c") == b"v1"
    assert r.set("missing", "v", xx=True) is None
    assert r.exists("missing") == 0


def test_ttl_and_pttl_on_a_missing_key(redis_client):
    r = redis_client
    assert r.ttl("gone") == -2
    assert r.pttl("gone") == -2


def test_past_dated_pexpireat_removes_the_key(redis_client):
    r = redis_client
    r.set("doomed", "v")
    assert r.pexpireat("doomed", 1) is True
    assert r.exists("doomed") == 0


# --- error cases -------------------------------------------------------------------------


def test_incr_on_a_non_integer_value_raises_with_the_exact_message(redis_client):
    r = redis_client
    r.set("s", "abc")
    try:
        r.execute_command("INCR", "s")
    except redis.ResponseError as exc:
        assert str(exc) == "value is not an integer or out of range", exc
    else:
        raise AssertionError("INCR on a non-integer value answered instead of erroring")


def test_default_protocol_client_fails_with_noproto(mini_redis_server):
    bad = redis.Redis(host="127.0.0.1", port=mini_redis_server)
    try:
        bad.ping()
    except redis.ResponseError as exc:
        assert "NOPROTO" in str(exc), exc
    else:
        raise AssertionError("the default client connected; the protocol=2 pin is untested")


def test_execute_command_incr_and_decr_work_while_the_helper_methods_do_not(redis_client):
    r = redis_client
    assert r.execute_command("INCR", "cnt") == 1
    assert r.execute_command("DECR", "cnt") == 0
    try:
        r.incr("cnt")
    except redis.ResponseError as exc:
        assert "INCRBY" in str(exc), exc
    else:
        raise AssertionError("r.incr() stopped sending INCRBY; re-check the mapping")
