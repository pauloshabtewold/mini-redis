import commands

Kind = commands.registry.Kind

# set-equality on (name, kind) pairs, not presence: a mistagged command fails outright, and a command this map does not carry fails here until the map is edited deliberately
EXPECTED = {
    b"PING": Kind.OTHER,
    b"ECHO": Kind.OTHER,
    b"HELLO": Kind.OTHER,
}


def test_every_registered_command_carries_its_expected_kind():
    actual = {(name, entry.kind) for name, entry in commands.registry.COMMANDS.items()}
    assert actual == set(EXPECTED.items())
