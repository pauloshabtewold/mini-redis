"""Every registered command's reply, byte for byte, at every arity it accepts and refuses.

Every one of these was checked by hand when the commands were written, and by nothing that runs
again. Twenty mutations of the command layer -- ECHO returning the wrong bytes, HELLO's whole
reply replaced, the CR/LF sanitiser deleted -- left the suite green, and commands/server.py
executed 14 of its 37 lines. This module is what makes those lines run.
"""

import socket

import pytest

import commands
from commands import registry
from connection import Connection, Role
from store import Store


@pytest.fixture
def store():
    return Store()


@pytest.mark.parametrize(
    "argv, expected",
    [
        ([b"PING"], b"+PONG\r\n"),
        ([b"ping"], b"+PONG\r\n"),
        ([b"pInG"], b"+PONG\r\n"),
        ([b"PING", b"hi"], b"$2\r\nhi\r\n"),
        ([b"PING", b""], b"$0\r\n\r\n"),
        ([b"PING", b"\x00\xff\r\n"], b"$4\r\n\x00\xff\r\n\r\n"),
        ([b"PING", b"a", b"b"], b"-ERR wrong number of arguments for 'ping' command\r\n"),
        ([b"ECHO", b"hi"], b"$2\r\nhi\r\n"),
        ([b"ECHO", b""], b"$0\r\n\r\n"),
        ([b"ECHO", b"\x00\xff\r\n"], b"$4\r\n\x00\xff\r\n\r\n"),
        ([b"ECHO"], b"-ERR wrong number of arguments for 'echo' command\r\n"),
        ([b"eChO", b"a", b"b"], b"-ERR wrong number of arguments for 'echo' command\r\n"),
        ([b"HELLO", b"2", b"foo"], b"-ERR wrong number of arguments for 'hello' command\r\n"),
    ],
)
def test_reply_bytes(store, conn, argv, expected):
    response, effects = commands.dispatch(store, conn, argv)
    assert response == expected
    assert effects == []


@pytest.mark.parametrize("bad", [b"1", b"3", b"9", b"0", b"-1", b"abc", b"2.0", b"", b"02"])
def test_hello_refuses_every_protocol_version_but_two(store, conn, bad):
    response, effects = commands.dispatch(store, conn, [b"HELLO", bad])
    assert response == b"-NOPROTO unsupported protocol version\r\n"
    assert effects == []


@pytest.mark.parametrize("argv", [[b"HELLO"], [b"HELLO", b"2"]])
def test_hello_answers_the_fourteen_element_array(store, conn, argv):
    expected = (
        b"*14\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n0.1.0\r\n"
        b"$5\r\nproto\r\n:2\r\n$2\r\nid\r\n:%d\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n"
        b"$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n" % conn.id
    )
    response, effects = commands.dispatch(store, conn, argv)
    assert response == expected
    assert effects == []


def test_hello_reports_the_replication_role_not_the_connection_role():
    # the two vocabularies are disjoint: a follower link still answers master here
    store = Store()
    a, _b = socket.socketpair()
    follower = Connection(a, ("127.0.0.1", 0), role=Role.FOLLOWER)
    try:
        response, effects = commands.dispatch(store, follower, [b"HELLO"])
        assert b"$4\r\nrole\r\n$6\r\nmaster\r\n" in response
        assert effects == []
        assert follower.role is Role.FOLLOWER
    finally:
        a.close()


def test_hello_reports_this_connection_s_own_id():
    store = Store()
    a, _b = socket.socketpair()
    c, _d = socket.socketpair()
    first, second = Connection(a, ("h", 0)), Connection(c, ("h", 0))
    try:
        first_reply, first_effects = commands.dispatch(store, first, [b"HELLO"])
        second_reply, second_effects = commands.dispatch(store, second, [b"HELLO"])
        assert first_reply != second_reply
        assert first_effects == []
        assert second_effects == []
        third_reply, third_effects = commands.dispatch(store, first, [b"HELLO"])
        assert b"$2\r\nid\r\n:%d\r\n" % first.id in third_reply
        assert third_effects == []
    finally:
        a.close()
        c.close()


@pytest.mark.parametrize(
    "name, expected",
    [
        (b"NOSUCHCMD", b"-ERR unknown command 'NOSUCHCMD'\r\n"),
        (b"", b"-ERR unknown command ''\r\n"),
        # a multibulk name may legally carry CR or LF, and an unmapped one would split a single
        # error frame into two and desynchronise every later reply on that connection
        (b"AB\r\nCD", b"-ERR unknown command 'AB  CD'\r\n"),
        (b"A\rB", b"-ERR unknown command 'A B'\r\n"),
        (b"A\nB", b"-ERR unknown command 'A B'\r\n"),
        (b"\r\n", b"-ERR unknown command '  '\r\n"),
    ],
)
def test_an_unknown_command_is_one_frame_carrying_the_name_as_sent(store, conn, name, expected):
    reply, effects = commands.dispatch(store, conn, [name])
    assert reply == expected
    assert reply.count(b"\r\n") == 1
    assert effects == []


def test_an_unknown_command_ignores_its_arguments(store, conn):
    # the short form is pinned; real Redis appends the first few arguments and this never does
    reply, effects = commands.dispatch(store, conn, [b"NOSUCHCMD", b"foo", b"bar"])
    assert reply == b"-ERR unknown command 'NOSUCHCMD'\r\n"
    assert effects == []


def test_arity_is_exact_when_positive_and_a_minimum_when_negative():
    # both counts include the command name; an upper bound is the handler's own check
    assert registry.COMMANDS[b"ECHO"].arity == 2
    assert registry.COMMANDS[b"PING"].arity == -1
    assert registry.COMMANDS[b"HELLO"].arity == -1

    @registry.command(b"XARITY", arity=-2, kind=registry.Kind.READ)
    def xarity(store, conn, argv):
        return b"+XOK\r\n", []

    store = Store()
    try:
        assert registry.dispatch(store, None, [b"XARITY"]) == (
            b"-ERR wrong number of arguments for 'xarity' command\r\n", [])
        assert registry.dispatch(store, None, [b"XARITY", b"a"]) == (b"+XOK\r\n", [])
        assert registry.dispatch(store, None, [b"XARITY", b"a", b"b", b"c"]) == (b"+XOK\r\n", [])
        # the canonical name is the registry's own, lower-cased, never the client's spelling
        assert registry.dispatch(store, None, [b"xArItY"]) == (
            b"-ERR wrong number of arguments for 'xarity' command\r\n", [])
    finally:
        del registry.COMMANDS[b"XARITY"]


def test_a_duplicate_registration_is_refused_rather_than_shadowing():
    # a silent replacement produces a command that exists, answers, and answers from the wrong
    # handler, which no arity check and no tag check can see
    with pytest.raises(ValueError) as exc_info:
        registry.command(b"PING", arity=-1, kind=registry.Kind.OTHER)(
            lambda store, conn, argv: (b"+WRONG\r\n", []))
    assert b"PING" in str(exc_info.value).encode()
    store = Store()
    assert commands.dispatch(store, None, [b"PING"]) == (b"+PONG\r\n", [])


def test_every_handler_returns_bytes_in_the_reply_slot_and_writes_nothing_to_the_socket():
    # the command layer never touches a socket: a handler that wrote to one would make a follower
    # replaying its leader's stream through this same dispatcher, producing no replies, impossible
    class Recording:
        def __init__(self):
            self.writes = []

        def fileno(self):
            return -1

        def send(self, data):
            self.writes.append(bytes(data))
            return len(data)

        def recv(self, size):
            return b""

        def close(self):
            pass

    recorder = Recording()
    c = Connection(recorder, ("127.0.0.1", 0))
    store = Store()
    for argv in ([b"PING"], [b"PING", b"x"], [b"ECHO", b"x"], [b"HELLO"], [b"HELLO", b"9"], [b"NOSUCHCMD"]):
        response, effects = commands.dispatch(store, c, argv)
        assert type(response) is bytes, argv
        assert effects == [], argv
    assert recorder.writes == []
    assert c.write_buffer == bytearray()
