"""redis-cli compliance across the full registered command set: one subprocess.run per
command against a single real server, with the driven command list computed from
tests/test_command_registry.py's EXPECTED map rather than duplicated by hand.
"""

import shutil
import subprocess
import time

import pytest

from tests.test_command_registry import EXPECTED

NO_REDIS_CLI_REASON = "redis-cli not found on PATH"


def commands_under_test() -> set[bytes]:
    # recomputed from EXPECTED on every call rather than cached at import time, so a
    # command added to the map later is picked up the next time this is called
    return set(EXPECTED)


def _cli(port, *args):
    return subprocess.run(
        ["redis-cli", "-p", str(port), *args],
        capture_output=True, text=True, timeout=5,
    )


# --- one case per registered command, each against its own dedicated key. EXPECTED's
# keys are driven as a set, not a sequence, so no case here may depend on another
# having already run against the shared server -----------------------------------------


def _case_ping(port):
    assert _cli(port, "PING").stdout.strip() == "PONG"


def _case_echo(port):
    assert _cli(port, "ECHO", "compliance").stdout.strip() == "compliance"


def _case_hello(port):
    fields = _cli(port, "HELLO", "2").stdout.splitlines()
    pairs = dict(zip(fields[0::2], fields[1::2]))
    assert pairs["server"] == "redis", fields
    assert pairs["version"] == "0.1.0", fields
    assert pairs["proto"] == "2", fields
    assert pairs["role"] == "master", fields


def _case_set(port):
    assert _cli(port, "SET", "cli:set", "hello").stdout.strip() == "OK"


def _case_get(port):
    _cli(port, "SET", "cli:get", "value")
    assert _cli(port, "GET", "cli:get").stdout.strip() == "value"


def _case_del(port):
    _cli(port, "SET", "cli:del", "value")
    assert _cli(port, "DEL", "cli:del").stdout.strip() == "1"


def _case_exists(port):
    _cli(port, "SET", "cli:exists", "value")
    result = _cli(port, "EXISTS", "cli:exists", "cli:exists:missing")
    assert result.stdout.strip() == "1"


def _case_type(port):
    _cli(port, "SET", "cli:type", "value")
    assert _cli(port, "TYPE", "cli:type").stdout.strip() == "string"


def _case_expire(port):
    _cli(port, "SET", "cli:expire", "value")
    assert _cli(port, "EXPIRE", "cli:expire", "100").stdout.strip() == "1"


def _case_pexpire(port):
    _cli(port, "SET", "cli:pexpire", "value")
    assert _cli(port, "PEXPIRE", "cli:pexpire", "100000").stdout.strip() == "1"


def _case_pexpireat(port):
    _cli(port, "SET", "cli:pexpireat", "value")
    deadline_ms = int(time.time() * 1000) + 100_000
    result = _cli(port, "PEXPIREAT", "cli:pexpireat", str(deadline_ms))
    assert result.stdout.strip() == "1"


def _case_ttl(port):
    _cli(port, "SET", "cli:ttl", "value")
    _cli(port, "EXPIRE", "cli:ttl", "100")
    assert _cli(port, "TTL", "cli:ttl").stdout.strip() == "100"


def _case_pttl(port):
    _cli(port, "SET", "cli:pttl", "value")
    _cli(port, "EXPIRE", "cli:pttl", "100")
    remaining = int(_cli(port, "PTTL", "cli:pttl").stdout.strip())
    assert 0 < remaining <= 100_000, remaining


def _case_incr(port):
    assert _cli(port, "INCR", "cli:incr").stdout.strip() == "1"


def _case_decr(port):
    assert _cli(port, "DECR", "cli:decr").stdout.strip() == "-1"


def _case_lpush(port):
    assert _cli(port, "LPUSH", "cli:lpush", "a", "b").stdout.strip() == "2"


def _case_rpush(port):
    assert _cli(port, "RPUSH", "cli:rpush", "a", "b").stdout.strip() == "2"


def _case_lpop(port):
    _cli(port, "RPUSH", "cli:lpop", "a", "b")
    assert _cli(port, "LPOP", "cli:lpop").stdout.strip() == "a"


def _case_rpop(port):
    _cli(port, "RPUSH", "cli:rpop", "a", "b")
    assert _cli(port, "RPOP", "cli:rpop").stdout.strip() == "b"


def _case_lrange(port):
    _cli(port, "RPUSH", "cli:lrange", "a", "b", "c")
    result = _cli(port, "LRANGE", "cli:lrange", "0", "-1")
    assert result.stdout.splitlines() == ["a", "b", "c"]


def _case_llen(port):
    _cli(port, "RPUSH", "cli:llen", "a", "b", "c")
    assert _cli(port, "LLEN", "cli:llen").stdout.strip() == "3"


def _case_dbsize(port):
    before = int(_cli(port, "DBSIZE").stdout.strip())
    _cli(port, "SET", "cli:dbsize", "value")
    after = int(_cli(port, "DBSIZE").stdout.strip())
    assert after == before + 1, (before, after)


def _case_keys(port):
    _cli(port, "SET", "cli:keys:only", "value")
    assert _cli(port, "KEYS", "cli:keys:*").stdout.splitlines() == ["cli:keys:only"]


def _case_flushall(port):
    _cli(port, "SET", "cli:flushall", "value")
    assert _cli(port, "FLUSHALL").stdout.strip() == "OK"
    assert _cli(port, "DBSIZE").stdout.strip() == "0"


def _case_info(port):
    body = _cli(port, "INFO").stdout
    assert "redis_version:0.1.0" in body, body
    assert "role:master" in body, body


def _case_config(port):
    result = _cli(port, "CONFIG", "GET", "save")
    assert result.stdout.splitlines() == ["save", ""]


CASES = {
    b"PING": _case_ping,
    b"ECHO": _case_echo,
    b"HELLO": _case_hello,
    b"SET": _case_set,
    b"GET": _case_get,
    b"DEL": _case_del,
    b"EXISTS": _case_exists,
    b"TYPE": _case_type,
    b"EXPIRE": _case_expire,
    b"PEXPIRE": _case_pexpire,
    b"PEXPIREAT": _case_pexpireat,
    b"TTL": _case_ttl,
    b"PTTL": _case_pttl,
    b"INCR": _case_incr,
    b"DECR": _case_decr,
    b"LPUSH": _case_lpush,
    b"RPUSH": _case_rpush,
    b"LPOP": _case_lpop,
    b"RPOP": _case_rpop,
    b"LRANGE": _case_lrange,
    b"LLEN": _case_llen,
    b"DBSIZE": _case_dbsize,
    b"KEYS": _case_keys,
    b"FLUSHALL": _case_flushall,
    b"INFO": _case_info,
    b"CONFIG": _case_config,
}


@pytest.mark.skipif(shutil.which("redis-cli") is None, reason=NO_REDIS_CLI_REASON)
def test_redis_cli_compliance_across_the_full_command_set(mini_redis_server):
    port = mini_redis_server
    driven = set()
    for name in commands_under_test():
        CASES[name](port)
        driven.add(name)
    # a command present in EXPECTED with no entry in CASES raises KeyError above rather
    # than reaching this line, so it fails the test instead of being silently absent
    assert driven == set(EXPECTED)
