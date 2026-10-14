import inspect
import logging
import selectors
import socket

import pytest

import resp
from connection import Connection
import server as server_mod
from server import MAX_SCHEDULABLE_INTERVAL, Server, build_arg_parser, main
from tests.test_server_lifecycle import listening, pump


def _drive(wire, **caps):
    # a real Connection driven through take_commands(), not resp.parse_command(): the live server reaches
    # parse_multibulk_header and parse_bulk_element directly, so a cap only in parse_command misses this path
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
    # the CLI is not the only door: Server is built directly by tests and by anything embedding it. every
    # limit is validated before self._loop, so a refused construction opens no selector
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
    # main() up to constructing the server and no further: run() is a recorder, so a refusal fails by name
    # rather than by starting a server that never returns. the snapshot path does not exist, saving off
    started = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(Server, "run", lambda self: started.append(self))
    main([*flags, "--port", "0", "--snapshot-interval", "0",
          "--snapshot-path", str(tmp_path / "dump.mrdb")])
    assert len(started) == 1, "main() returned without constructing and running a server"
    return started[0]


# the one wire neither the parser tests nor the constructor tests touch: the keyword list in main(). a flag
# dropped from it leaves both green while the setting never leaves its default. --log-level reaches no Server
_FLAGS_THAT_REACH_THE_SERVER = [
    # above the default high-water mark, which a limit now has to be: this case is about the
    # flag reaching the attribute and not about the figure, so the figure moved rather than the case
    (("--write-buffer-limit", "2097152"), "write_buffer_limit", 2097152),
    (("--max-value-size", "1000"), "max_value_size", 1000),
    (("--max-multibulk", "100"), "max_multibulk", 100),
    (("--expiry-sweep-interval", "250"), "expiry_sweep_interval", 250),
    (("--ignore-snapshot",), "ignore_snapshot", True),
    (("--shutdown-drain-timeout", "3"), "shutdown_drain_timeout", 3),
    (("--max-connections", "77"), "max_connections", 77),
    (("--write-buffer-high-water", "4194304"), "write_buffer_high_water", 4194304),
    (("--write-buffer-low-water", "1000"), "write_buffer_low_water", 1000),
    (("--incomplete-command-timeout", "7"), "incomplete_command_timeout", 7),
    (("--rate-limit", "100"), "rate_limit", 100),
    # 2 and not 1: 1 is the default, and the test above refuses a case whose value is one
    (("--rate-limit-window", "2"), "rate_limit_window", 2),
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
    # the three settings the helper gives every call, which the parametrised test cannot vary. each is
    # compared on its own: a tuple != passes when any one differs and would miss the other two
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
    # the two tests above are only as complete as their lists: a flag in the parser and in neither list
    # passes both while nothing reads it. --log-level is the exemption, checked to still reach no Server
    defined = set(vars(build_arg_parser().parse_args([])))
    covered = (
        {attribute for _flags, attribute, _wanted in _FLAGS_THAT_REACH_THE_SERVER}
        | set(_FLAGS_THE_HELPER_ALWAYS_PASSES)
        | {"log_level"})
    assert defined == covered, (
        "flags with no case: %s; cases for no flag: %s"
        % (sorted(defined - covered), sorted(covered - defined)))
    assert "log_level" not in inspect.signature(Server.__init__).parameters


# every numeric setting, named here rather than derived, so a flag added without a case shows up
# in the coverage test below and not as a silent gap in this one
_EVERY_NUMERIC_SETTING = [
    "write_buffer_limit", "max_value_size", "max_multibulk", "snapshot_interval",
    "expiry_sweep_interval", "shutdown_drain_timeout", "max_connections",
    "write_buffer_high_water", "write_buffer_low_water", "incomplete_command_timeout",
    "rate_limit", "rate_limit_window",
]


@pytest.mark.parametrize("setting", _EVERY_NUMERIC_SETTING)
@pytest.mark.parametrize("value", [float("nan"), float("inf"), True, 2.5],
                         ids=["nan", "inf", "bool", "float"])
def test_a_value_that_is_not_a_whole_number_is_refused_at_the_constructor(setting, value):
    # every check these settings pass is an ordering and `nan < 0` is False, so nan, inf and True (bool
    # subclasses int) got past all of them: rate_limit=nan refuses nobody, and the hole spans twelve flags
    with pytest.raises(ValueError) as refusal:
        Server(0, **{setting: value})
    assert "must be a whole number" in str(refusal.value), str(refusal.value)
    assert setting in str(refusal.value), (
        "the refusal does not name the setting that was wrong", str(refusal.value))


def test_every_numeric_setting_is_covered_by_the_whole_number_case():
    # the list above is written out, so this is what keeps it honest: a numeric parameter added to
    # Server.__init__ and not to the list would otherwise never be tested for the hole above
    import inspect
    numeric = {
        name for name, p in inspect.signature(Server.__init__).parameters.items()
        if p.default is not inspect.Parameter.empty and isinstance(p.default, int)
        and not isinstance(p.default, bool) and name != "port"
    }
    assert numeric == set(_EVERY_NUMERIC_SETTING), (
        "numeric settings with no whole-number case: %s; cases for no setting: %s"
        % (sorted(numeric - set(_EVERY_NUMERIC_SETTING)),
           sorted(set(_EVERY_NUMERIC_SETTING) - numeric)))


def test_an_empty_host_is_refused_at_both_doors(monkeypatch, tmp_path, capsys):
    # an empty host is what bind() reads as every interface on a server with no authentication, and it is
    # what `--host "$HOST"` becomes when HOST is unset. the parser is one door and Server the other
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

    # what is refused is the empty string and not the wildcard: 0.0.0.0 says on purpose what the empty
    # string says by accident. nothing listens here, because the bind happens in run()
    assert build_arg_parser().parse_args(["--host", "0.0.0.0"]).host == "0.0.0.0"
    server = Server(0, host="0.0.0.0")
    try:
        assert server.host == "0.0.0.0"
    finally:
        server._loop.close()


def test_a_limit_at_or_below_the_high_water_mark_is_refused_at_both_doors(
        monkeypatch, tmp_path, capsys):
    # a non-zero limit at or below a non-zero high-water mark leaves both marks unreachable, since _flush
    # tests the limit before it decides read interest and returns on the close; equality is refused too
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(
        Server, "run", lambda self: pytest.fail("main() ran a server over a contradictory pair"))
    for limit, high in ((65536, 1048576), (1048576, 1048576), (1024, 1025)):
        with pytest.raises(SystemExit) as exit_info:
            main(["--write-buffer-limit", str(limit), "--write-buffer-high-water", str(high),
                  "--write-buffer-low-water", "0",
                  "--port", "0", "--snapshot-interval", "0",
                  "--snapshot-path", str(tmp_path / "dump.mrdb")])
        assert exit_info.value.code == 2, exit_info.value.code
        message = capsys.readouterr().err
        assert "write buffer limit" in message and "write buffer high water" in message, message
        assert "write_buffer" not in message and "_check" not in message, message
        # its own ending, and not the shared "0 disables the check": an operator who typed a small
        # limit on purpose has to be told that 0 is what removes it, not that 0 is what it meant
        assert "both water marks would do nothing" in message, message

        with pytest.raises(ValueError) as refusal:
            Server(0, write_buffer_limit=limit, write_buffer_high_water=high,
                   write_buffer_low_water=0)
        assert "write_buffer_limit" in str(refusal.value), refusal.value
        assert "write_buffer_high_water" in str(refusal.value), refusal.value

    # the three pairings that must still be accepted, each for its own reason: no limit at all, no
    # pause at all, and a limit one byte above the mark, which is the smallest band that works
    for settings in ({"write_buffer_limit": 0, "write_buffer_high_water": 1048576},
                     {"write_buffer_limit": 1024, "write_buffer_high_water": 0},
                     {"write_buffer_limit": 1026, "write_buffer_high_water": 1025,
                      "write_buffer_low_water": 1024}):
        server = Server(0, **settings)
        try:
            for name, value in settings.items():
                assert getattr(server, name) == value, (name, settings)
        finally:
            server._loop.close()
    server = _main_with_run_stubbed(
        monkeypatch, tmp_path, "--write-buffer-limit", "1026",
        "--write-buffer-high-water", "1025", "--write-buffer-low-water", "1024")
    try:
        assert server.write_buffer_limit == 1026
    finally:
        server._loop.close()


def test_a_low_water_mark_above_the_high_water_mark_is_refused_at_both_doors(
        monkeypatch, tmp_path, capsys):
    # the one rule spanning two settings, so argparse cannot hold it: main() refuses the pair as a usage
    # error, exit 2, and Server for a caller that skipped main(); equal marks are refused as well as inverted
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
    # the pair rule is guarded by the high-water mark: 0 switches the pause off, and without the guard the
    # one way to switch it off would be refused against the default low-water mark
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


def test_the_high_water_mark_and_the_command_timeout_default_to_the_documented_values():
    # the tests that exercise these read each default out of the parser and size their queues relative to
    # it, so the figures themselves are pinned nowhere else: a published number needs its own literal
    args = build_arg_parser().parse_args([])
    assert args.write_buffer_high_water == 1048576, args.write_buffer_high_water
    assert args.incomplete_command_timeout == 30, args.incomplete_command_timeout
    server = Server(0)
    try:
        assert (server.write_buffer_high_water, server.incomplete_command_timeout) == (1048576, 30)
    finally:
        server._loop.close()


def test_each_new_numeric_flag_is_refused_when_negative_at_both_doors(capsys):
    # both doors, with the one duration also refused past MAX_SCHEDULABLE_INTERVAL, and each flag's own
    # ending in full: "cannot be negative" alone passes for another flag's message, and two endings exist
    flags = (
        ("--write-buffer-high-water", "write buffer high water", "write_buffer_high_water",
         "0 disables the check, not -1"),
        ("--write-buffer-low-water", "write buffer low water", "write_buffer_low_water",
         "-1 does not turn anything off, and 0 means resume only when the queue is empty"),
        ("--incomplete-command-timeout", "incomplete command timeout", "incomplete_command_timeout",
         "0 disables the check, not -1"),
    )
    endings = {ending for _flag, _label, _name, ending in flags}
    assert len(endings) == 2, endings
    for flag, label, name, ending in flags:
        with pytest.raises(SystemExit):
            build_arg_parser().parse_args([flag, "-1"])
        message = capsys.readouterr().err
        assert flag in message and label in message and "_" + name not in message, message
        # through the end of the line, so the ending is exactly this flag's and nothing follows it
        assert "cannot be negative; %s\n" % ending in message, message
        with pytest.raises(ValueError) as refusal:
            Server(0, **{name: -1})
        refused = str(refusal.value)
        assert name in refused and refused.endswith("cannot be negative; %s" % ending), refused
        for other in endings - {ending}:
            assert other not in message and other not in refused, (flag, other)

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
    # the check sits ahead of body_end, so the connection never commits to a buffer hint for the oversized
    # body -- _parse_needed == 0 after the refusal is the proof, where reading the error alone passes
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
    # the two limits sit on opposite sides and neither bounds the other: a value well under --max-value-size
    # can still exceed --write-buffer-limit across one pipelined batch, which disconnects the client
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


def test_every_flag_that_takes_a_value_states_its_default_in_help():
    # thirteen flags said nothing about their default, so the LITERAL clause is asserted: asking whether
    # str(value) appeared anywhere passed over eight mutated defaults -- "1" is inside "100 requests"
    import argparse
    parser = build_arg_parser()
    wrong = []
    checked = 0
    for action in parser._actions:
        if isinstance(action, argparse._HelpAction):
            continue
        if action.nargs == 0 or action.const is not None:
            # a store_true takes no value, so it has no default to state: passed or not passed is
            # the whole of it, and --ignore-snapshot's help already says what each means
            continue
        checked += 1
        clause = "the default is %s" % (action.default,)
        # the help strings are implicit concatenations that wrap, so a clause can arrive with a
        # newline or a run of spaces inside it; the comparison is over collapsed whitespace
        if clause not in " ".join((action.help or "").split()):
            wrong.append((action.option_strings, clause, action.help))
    assert wrong == [], (
        "every flag that takes a value has to say, in --help, what it is if you leave it off, "
        "and name the figure the parser actually uses", wrong)
    assert checked == 16, (
        "the count is pinned so that a flag dropped from the parser cannot leave this test "
        "passing over fifteen", checked)


def test_the_unit_renderings_beside_a_default_are_the_right_unit():
    # the byte figures carry a second rendering, "33554432 (32 MiB)", and nothing checked the parenthesis:
    # a mutation to "(64 MiB)" passed; derived here from the same constant so the halves cannot disagree
    parser = build_arg_parser()
    for option, divisor, unit in (
        ("--write-buffer-limit", 1024 * 1024, "MiB"),
        ("--max-value-size", 1024 * 1024, "MiB"),
        ("--write-buffer-high-water", 1024, "KiB"),
        ("--write-buffer-low-water", 1024, "KiB"),
    ):
        action = next(a for a in parser._actions if option in a.option_strings)
        clause = "the default is %d (%d %s)" % (action.default, action.default // divisor, unit)
        assert clause in " ".join(action.help.split()), (
            "the figure and its rendering have to be one clause derived from one constant",
            option, clause, action.help)
