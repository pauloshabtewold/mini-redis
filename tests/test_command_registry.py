import commands

Kind = commands.registry.Kind

# set-equality on (name, kind) pairs, not presence: a mistagged command fails outright, and a command this map does not carry fails here until the map is edited deliberately
EXPECTED = {
    b"PING": Kind.OTHER,
    b"ECHO": Kind.OTHER,
    b"HELLO": Kind.OTHER,
    b"SET": Kind.WRITE,
    b"GET": Kind.READ,
    b"DEL": Kind.WRITE,
    b"EXISTS": Kind.READ,
    b"TYPE": Kind.READ,
    b"EXPIRE": Kind.WRITE,
    b"PEXPIRE": Kind.WRITE,
    b"PEXPIREAT": Kind.WRITE,
    b"TTL": Kind.READ,
    b"PTTL": Kind.READ,
    b"INCR": Kind.WRITE,
    b"DECR": Kind.WRITE,
}


def test_every_registered_command_carries_its_expected_kind():
    actual = {(name, entry.kind) for name, entry in commands.registry.COMMANDS.items()}
    assert actual == set(EXPECTED.items())


def test_every_kind_is_a_distinct_value():
    # StrEnum makes a duplicated value an alias rather than an error, and the map above compares
    # tags that would then be equal: a WRITE tagged "read" is a write the rate limiter exempts,
    # with every tag in this file still matching
    assert len(list(Kind)) == 3
    assert len({kind.value for kind in Kind}) == 3
