import inspect
import logging
import selectors
import socket

import pytest

import resp
from connection import Connection
from server import MAX_SCHEDULABLE_INTERVAL, Server, build_arg_parser, main
from tests.test_server_lifecycle import listening, pump


def _drive(wire, **caps):
    # a real Connection over a socketpair, driven through take_commands() rather than
    # resp.parse_command(): the live server reaches parse_multibulk_header and
    # parse_bulk_element straight from connection.py, never through parse_command for a
    # multibulk, so a cap threaded only into parse_command would leave this path
    # uncapped while the natural unit test stayed green
    a, b = socket.socketpair()
    a.setblocking(False)
    conn = Connection(a, ("stub", 0))
    try:
        b.sendall(wire)
        conn.receive()
        try:
            return conn, conn.take_commands(**caps)
        except resp.ProtocolError as exc:
            return conn, exc.message
    finally:
        a.close()
        b.close()


def test_both_flags_default_to_the_documented_values():
    args = build_arg_parser().parse_args([])
    assert args.max_value_size == 67108864, args.max_value_size
    assert args.max_multibulk == 1048576, args.max_multibulk
    server = Server(0)
    try:
        assert (server.max_value_size, server.max_multibulk) == (67108864, 1048576)
    finally:
        # Server.__init__ opens a selector that only run() would otherwise dispose of
        server._loop.close()


@pytest.mark.parametrize(
    "flag", ["--max-value-size", "--max-multibulk", "--write-buffer-limit"])
def test_a_negative_limit_is_refused_at_the_cli_for_all_three_limits(flag):
    # the shared rule covers all three -- including --write-buffer-limit,
    # whose own observable behaviour at this door must not have changed
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args([flag, "-1"])


def test_a_negative_limit_is_refused_by_the_constructor_for_all_three_limits():
    # the CLI is not the only door: Server is constructed directly by tests and by
    # anything embedding it. every limit is validated before self._loop is built, so a
    # refused construction opens no selector and leaves nothing to close
    for kwargs in (
        {"max_value_size": -1}, {"max_multibulk": -1}, {"write_buffer_limit": -1}
    ):
        with pytest.raises(ValueError):
            Server(0, **kwargs)


@pytest.mark.parametrize("flag", ["--max-value-size", "--max-multibulk"])
def test_a_non_numeric_limit_is_refused_at_the_cli(flag):
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args([flag, "notanumber"])


def test_zero_is_accepted_by_both_doors_and_disables_each_cap():
    args = build_arg_parser().parse_args(
        ["--max-value-size", "0", "--max-multibulk", "0"])
    assert (args.max_value_size, args.max_multibulk) == (0, 0)
    server = Server(0, max_value_size=0, max_multibulk=0)
    try:
        assert (server.max_value_size, server.max_multibulk) == (0, 0)
    finally:
        server._loop.close()


def _main_with_run_stubbed(monkeypatch, tmp_path, *flags):
    # main() up to and including constructing the server, and no further: run() is replaced
    # by a recorder, so a refusal is what stops it or the test fails by name instead of by
    # starting a server that never returns. logging is left alone, and the snapshot is a
    # path under tmp_path that does not exist, with saving off
    started = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(Server, "run", lambda self: started.append(self))
    main([*flags, "--port", "0", "--snapshot-interval", "0",
          "--snapshot-path", str(tmp_path / "dump.mrdb")])
    assert len(started) == 1, "main() returned without constructing and running a server"
    return started[0]


# the mirror of the rule that every numeric flag is refused at both doors: a flag accepted at both
# doors can still be read by nothing. the validator tests build a Server directly and the parser
# tests check what is refused, so neither touches the one wire between them, the keyword list in
# main(), and a flag dropped from it leaves the parser, the constructor and every test of either
# green while the setting never leaves its default. each flag main() hands to the constructor is
# listed here with a value that is not its default, beside the attribute that has to carry it.
# --log-level is not among them: it never reaches Server, and test_logging.py reads what main()
# does with it
_FLAGS_THAT_REACH_THE_SERVER = [
    (("--write-buffer-limit", "12345"), "write_buffer_limit", 12345),
    (("--max-value-size", "1000"), "max_value_size", 1000),
    (("--max-multibulk", "100"), "max_multibulk", 100),
    (("--expiry-sweep-interval", "250"), "expiry_sweep_interval", 250),
    (("--ignore-snapshot",), "ignore_snapshot", True),
    (("--shutdown-drain-timeout", "3"), "shutdown_drain_timeout", 3),
    (("--max-connections", "77"), "max_connections", 77),
    (("--write-buffer-high-water", "4194304"), "write_buffer_high_water", 4194304),
    (("--write-buffer-low-water", "1000"), "write_buffer_low_water", 1000),
    (("--incomplete-command-timeout", "7"), "incomplete_command_timeout", 7),
    # a name and not an address: it resolves to the loopback address the default is, so
    # nothing is exposed by the value, and it is not the string the default is spelt as
    (("--host", "localhost"), "host", "localhost"),
]


@pytest.mark.parametrize(
    "flags, attribute, wanted", _FLAGS_THAT_REACH_THE_SERVER,
    ids=[flags[0] for flags, _attribute, _wanted in _FLAGS_THAT_REACH_THE_SERVER])
def test_a_parsed_flag_arrives_on_the_server_main_constructs(
        monkeypatch, tmp_path, flags, attribute, wanted):
    # the value has to differ from the default for the assertion below to mean anything:
    # a main() that handed the constructor the default would otherwise pass for every flag
    assert getattr(build_arg_parser().parse_args([]), attribute) != wanted, (
        "%s: the value chosen here is the flag's default" % flags[0])
    server = _main_with_run_stubbed(monkeypatch, tmp_path, *flags)
    try:
        assert getattr(server, attribute) == wanted, (
            "%s was parsed and the Server main() built does not hold it: %s is %r"
            % (flags[0], attribute, getattr(server, attribute)))
    finally:
        server._loop.close()


# the three settings _main_with_run_stubbed gives every call, which the parametrised cases
# above cannot vary. they are covered by the test below, one element at a time
_FLAGS_THE_HELPER_ALWAYS_PASSES = ("port", "snapshot_interval", "snapshot_path")


def test_the_flags_the_helper_always_passes_arrive_on_the_server_too(monkeypatch, tmp_path):
    # --port, --snapshot-interval and --snapshot-path are given by _main_with_run_stubbed to
    # every call, so the parametrised test above cannot vary them. each one differs from its
    # default and each is handed over by main() like the rest. every element is compared on its
    # own: a tuple comparison with != passes when any one of the three differs, so it would not
    # notice the other two matching their defaults
    wanted = {"port": 0, "snapshot_interval": 0, "snapshot_path": str(tmp_path / "dump.mrdb")}
    assert tuple(wanted) == _FLAGS_THE_HELPER_ALWAYS_PASSES
    parsed = build_arg_parser().parse_args([])
    server = _main_with_run_stubbed(monkeypatch, tmp_path)
    try:
        for attribute, value in wanted.items():
            assert getattr(parsed, attribute) != value, (
                "--%s: the value the helper passes is the flag's default" % attribute.replace("_", "-"))
            assert getattr(server, attribute) == value, (
                "--%s was parsed and the Server main() built does not hold it: %s is %r"
                % (attribute.replace("_", "-"), attribute, getattr(server, attribute)))
    finally:
        server._loop.close()


def test_every_flag_the_parser_defines_has_a_case_that_follows_it_to_the_server():
    # the two tests above are only as complete as the lists they read, and a flag added to the
    # parser and to neither list would be read by nothing the suite can see and pass them both.
    # the parser's own destinations, from a parse of no arguments, have to equal what is
    # covered here plus --log-level, which reaches no Server parameter on purpose and is
    # checked to still have none: the day Server takes a level, this exemption is stale
    defined = set(vars(build_arg_parser().parse_args([])))
    covered = (
        {attribute for _flags, attribute, _wanted in _FLAGS_THAT_REACH_THE_SERVER}
        | set(_FLAGS_THE_HELPER_ALWAYS_PASSES)
        | {"log_level"})
    assert defined == covered, (
        "flags with no case: %s; cases for no flag: %s"
        % (sorted(defined - covered), sorted(covered - defined)))
    assert "log_level" not in inspect.signature(Server.__init__).parameters


def test_an_empty_host_is_refused_at_both_doors(monkeypatch, tmp_path, capsys):
    # an empty host is what bind() reads as every interface, on a server with no authentication
    # at all, which is the exposure the loopback default exists to prevent, and it is what
    # `--host "$HOST"` becomes when HOST is unset. no one means it, so it is refused rather than
    # honoured. the parser is one door and Server the other: a flag refused only at the CLI is
    # refused only for the people who use the CLI
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(
        Server, "run", lambda self: pytest.fail("main() ran a server over an empty host"))
    for spelling in (["--host", ""], ["--host="]):
        with pytest.raises(SystemExit) as exit_info:
            build_arg_parser().parse_args(spelling)
        assert exit_info.value.code == 2, exit_info.value.code
        message = capsys.readouterr().err
        assert "--host" in message and "host cannot be empty" in message, message
        assert "_check" not in message and "Traceback" not in message, message

    with pytest.raises(SystemExit) as exit_info:
        main(["--host", "", "--port", "0", "--snapshot-interval", "0",
              "--snapshot-path", str(tmp_path / "dump.mrdb")])
    assert exit_info.value.code == 2, exit_info.value.code
    capsys.readouterr()

    built = []
    try:
        with pytest.raises(ValueError) as refusal:
            built.append(Server(0, host=""))
    finally:
        for server in built:
            server._loop.close()
    assert "host cannot be empty" in str(refusal.value), refusal.value

    # what is refused is the empty string and not the wildcard: 0.0.0.0 says on purpose what
    # the empty string says by accident, and an operator who types it has chosen it. nothing
    # listens here, because the bind happens in run() and neither door reaches it
    assert build_arg_parser().parse_args(["--host", "0.0.0.0"]).host == "0.0.0.0"
    server = Server(0, host="0.0.0.0")
    try:
        assert server.host == "0.0.0.0"
    finally:
        server._loop.close()


def test_a_low_water_mark_above_the_high_water_mark_is_refused_at_both_doors(
        monkeypatch, tmp_path, capsys):
    # the one rule that spans two settings, so argparse cannot hold it: each flag is
    # checked alone as it is parsed. main() refuses the pair as a usage error, exit 2, and
    # Server refuses it for a caller that never went through main(). equal marks are
    # refused as well as inverted ones, because a band of zero width is the flapping the
    # two settings exist to avoid
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(
        Server, "run", lambda self: pytest.fail("main() ran a server over a contradictory pair"))
    for high, low in ((1024, 2048), (1024, 1024)):
        with pytest.raises(SystemExit) as exit_info:
            main(["--write-buffer-high-water", str(high), "--write-buffer-low-water", str(low),
                  "--port", "0", "--snapshot-interval", "0",
                  "--snapshot-path", str(tmp_path / "dump.mrdb")])
        assert exit_info.value.code == 2, exit_info.value.code
        message = capsys.readouterr().err
        assert "write buffer low water" in message and "write buffer high water" in message, message
        assert "write_buffer" not in message and "_check" not in message, message

        with pytest.raises(ValueError) as refusal:
            Server(0, write_buffer_high_water=high, write_buffer_low_water=low)
        assert "write_buffer_low_water" in str(refusal.value), refusal.value
        assert "write_buffer_high_water" in str(refusal.value), refusal.value

    # one byte inside the band is accepted through both doors
    server = _main_with_run_stubbed(
        monkeypatch, tmp_path, "--write-buffer-high-water", "1024", "--write-buffer-low-water", "1023")
    try:
        assert (server.write_buffer_high_water, server.write_buffer_low_water) == (1024, 1023)
    finally:
        server._loop.close()
    server = Server(0, write_buffer_high_water=1024, write_buffer_low_water=1023)
    server._loop.close()


def test_a_zero_high_water_mark_disables_the_pause_and_is_not_refused(monkeypatch, tmp_path):
    # the pair rule is guarded by the high-water mark: 0 for it switches the pause off, a
    # pause that is off has no band to be inconsistent with, and without the guard the one
    # way to switch it off would be refused against the default low-water mark. the
    # control inside the same test is the same connection with the pause on
    server = _main_with_run_stubbed(monkeypatch, tmp_path, "--write-buffer-high-water", "0")
    try:
        assert server.write_buffer_high_water == 0
        assert server.write_buffer_low_water == 262144, "the default low-water mark is still in force"
    finally:
        server._loop.close()
    Server(0, write_buffer_high_water=0)._loop.close()

    with listening() as (server, connect, _listener):
        client = connect()
        pump(server)
        conn, = server._connections
        selector = server._loop._selector

        server.write_buffer_high_water = 0
        conn.queue(b"x" * (16 * 1024 * 1024))
        server._flush(conn)
        assert len(conn.write_buffer) > 1024 * 1024, "the kernel took more than the test assumed"
        assert not conn.closed
        assert selector.get_key(conn).events == selectors.EVENT_READ | selectors.EVENT_WRITE, (
            "a disabled pause must leave a connection with a long queue being read")

        server.write_buffer_high_water = 1024 * 1024
        server._flush(conn)
        assert selector.get_key(conn).events == selectors.EVENT_WRITE, (
            "the same queue with the pause on is read no more: the control for the assertion above")
        client.close()


def test_each_new_numeric_flag_is_refused_when_negative_at_both_doors(capsys):
    # the parser is one door and Server is the other, and a flag refused at only one is
    # refused only for the people who use it. 0 is accepted at both, as the value that
    # switches the setting off, and the one duration among them is also refused past
    # MAX_SCHEDULABLE_INTERVAL, the ceiling every duration flag shares. this flag would not
    # fail without it: the stalled-command sweep only compares an elapsed time against the
    # value, and comparing an int with a float cannot overflow however large the int is. the
    # ceiling is there so that one rule covers every duration at both doors
    flags = (
        ("--write-buffer-high-water", "write buffer high water", "write_buffer_high_water"),
        ("--write-buffer-low-water", "write buffer low water", "write_buffer_low_water"),
        ("--incomplete-command-timeout", "incomplete command timeout", "incomplete_command_timeout"),
    )
    for flag, label, name in flags:
        with pytest.raises(SystemExit):
            build_arg_parser().parse_args([flag, "-1"])
        message = capsys.readouterr().err
        assert flag in message and label in message and "_" + name not in message, message
        with pytest.raises(ValueError):
            Server(0, **{name: -1})

        assert getattr(build_arg_parser().parse_args([flag, "0"]), name) == 0
        server = Server(0, **{name: 0})
        try:
            assert getattr(server, name) == 0
        finally:
            server._loop.close()

    too_big = MAX_SCHEDULABLE_INTERVAL + 1
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args(["--incomplete-command-timeout", str(too_big)])
    capsys.readouterr()
    with pytest.raises(ValueError):
        Server(0, incomplete_command_timeout=too_big)
    ceiling = str(MAX_SCHEDULABLE_INTERVAL)
    assert build_arg_parser().parse_args(
        ["--incomplete-command-timeout", ceiling]).incomplete_command_timeout == MAX_SCHEDULABLE_INTERVAL
    Server(0, incomplete_command_timeout=MAX_SCHEDULABLE_INTERVAL)._loop.close()


def test_a_declared_length_exactly_at_the_cap_is_accepted_through_a_connection():
    # the comparison is `>`, never `>=` -- a length exactly at the cap is kept,
    # driven through the same Connection path a live server uses
    wire = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\nabcd\r\n"
    conn, result = _drive(wire, max_value_size=4, max_multibulk=8)
    assert result == [[b"SET", b"k", b"abcd"]], result


def test_an_over_cap_bulk_length_is_refused_before_the_body_is_buffered():
    # the check sits ahead of body_end being computed, so the
    # connection never commits to a buffer-growth hint for the oversized body --
    # _parse_needed == 0 after the refusal is what proves it, and a criterion that
    # only reads the error body passes over a check placed one line too late
    header = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$68157440\r\n"    # 65 MiB declared, no body
    conn, message = _drive(header, max_value_size=67108864, max_multibulk=1048576)
    assert message == b"ERR Protocol error: invalid bulk length", message
    assert conn._parse_needed == 0, (
        "the connection recorded a buffer-growth commitment of %d before refusing -- "
        "the cap fired after body_end was computed" % conn._parse_needed)
    assert len(conn.read_buffer) <= len(header), len(conn.read_buffer)


def test_an_over_cap_multibulk_count_is_refused_through_a_connection():
    # the live path drives parse_multibulk_header directly out of connection.py,
    # never through parse_command -- the header-side twin of the test above
    conn, message = _drive(
        b"*1000000000\r\n", max_value_size=67108864, max_multibulk=1048576)
    assert message == b"ERR Protocol error: invalid multibulk length", message
    assert conn._elements_remaining == 0, "the header was accepted before it was refused"
    assert conn._argv is None


def test_a_live_server_refuses_an_over_cap_bulk_length_and_closes_the_sender():
    with listening() as (server, connect, _listener):
        server.max_value_size = 64
        server.max_multibulk = 8
        client = connect()
        pump(server)
        client.sendall(b"*1\r\n$65\r\n")
        pump(server)
        client.settimeout(0.5)
        assert client.recv(256) == b"-ERR Protocol error: invalid bulk length\r\n"
        assert len(server._connections) == 0, "a protocol error must close the sender"


def test_a_live_server_refuses_an_over_cap_multibulk_count_and_closes_the_sender():
    with listening() as (server, connect, _listener):
        server.max_value_size = 64
        server.max_multibulk = 8
        client = connect()
        pump(server)
        client.sendall(b"*9\r\n")
        pump(server)
        client.settimeout(0.5)
        assert client.recv(256) == b"-ERR Protocol error: invalid multibulk length\r\n"
        assert len(server._connections) == 0


def test_a_command_under_both_caps_dispatches_normally_through_a_live_server():
    # the caps must not over-refuse: an ordinary command well inside both limits is
    # dispatched and answered exactly as it would be with no cap at all
    with listening() as (server, connect, _listener):
        server.max_value_size = 64
        server.max_multibulk = 8
        client = connect()
        pump(server)
        client.sendall(b"*1\r\n$4\r\nPING\r\n")
        pump(server)
        client.settimeout(2)
        assert client.recv(64) == b"+PONG\r\n"
        assert len(server._connections) == 1


def test_a_value_inside_the_inbound_cap_can_exceed_the_outbound_limit():
    # the two limits sit on opposite sides and neither bounds the other: a value well
    # under --max-value-size can still be large enough, repeated across one pipelined
    # batch, to exceed --write-buffer-limit -- which then disconnects the very client
    # that stored it rather than throttling its read
    with listening() as (server, connect, _listener):
        server.max_value_size = 1024 * 1024
        server.write_buffer_limit = 64 * 1024
        client = connect()
        pump(server)
        value = b"v" * 32768
        client.sendall(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$%d\r\n" % len(value) + value + b"\r\n")
        pump(server)
        client.settimeout(2)
        assert client.recv(100) == b"+OK\r\n", \
            "storing it must succeed: it is under the inbound cap"

        # 200 GETs in one write and the client never reads, so nothing drains between
        # them -- the same shape test_server_lifecycle.py's own limit test uses
        client.sendall(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n" * 200)
        pump(server)
        assert len(server._connections) == 0, (
            "a value under the inbound cap must still respect the outbound limit "
            "once it is read back at enough volume")
