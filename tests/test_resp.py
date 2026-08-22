import pytest

import resp


def _feed_one_byte_at_a_time(wire, expected_argv):
    # Append wire into a bytearray one byte at a time, calling parse command after every byte and deleting the consumed prefix
    buf = bytearray()
    last_index = len(wire) - 1
    for index, byte in enumerate(wire):
        buf.append(byte)
        argv, consumed = resp.parse_command(buf)
        if index == last_index:
            assert argv == expected_argv, argv
            assert consumed == len(wire), (consumed, len(wire))
        else:
            assert (argv, consumed) == (None, 0), (index, argv, consumed)
        if consumed:
            del buf[:consumed]
    assert buf == bytearray()


# literal wire strings


def test_literal_wire_multibulk_ping():
    assert resp.parse_command(b"*1\r\n$4\r\nPING\r\n") == ([b"PING"], 14)


def test_response_encoders_match_literal_wire_bytes():
    assert resp.encode_simple_string(b"OK") == b"+OK\r\n"
    assert resp.encode_error(b"ERR nope") == b"-ERR nope\r\n"
    assert resp.encode_integer(0) == b":0\r\n"
    assert resp.encode_integer(-1) == b":-1\r\n"
    assert resp.encode_bulk_string(None) == b"$-1\r\n"


# round trips

def test_multi_argument_command_round_trips_through_encoder():
    argv_in = [b"SET", b"k", b"v"]
    wire = resp.encode_array([resp.encode_bulk_string(a) for a in argv_in])
    argv_out, consumed = resp.parse_command(wire)
    assert argv_out == argv_in
    assert consumed == len(wire)


def test_nested_array_matches_literal_encoding():
    wire = resp.encode_array([
        resp.encode_array([resp.encode_integer(1)]),
        resp.encode_bulk_string(b"a"),
    ])
    assert wire == b"*2\r\n*1\r\n:1\r\n$1\r\na\r\n"


# buffer boundaries


def test_bulk_body_containing_crlf_is_one_value():
    # the body is itself valid RESP, so a parser that scans for CRLF produces a plausible wrong answer rather than an obvious one
    body = b"*2\r\n$3\r\nfoo\r\n"
    wire = b"*1\r\n$%d\r\n" % len(body) + body + b"\r\n"

    argv, consumed = resp.parse_command(wire)

    assert argv == [body], argv
    assert consumed == len(wire), (consumed, len(wire))
    assert type(argv[0]) is bytes, type(argv[0])

    buf = bytearray(wire)
    del buf[:consumed]
    assert buf == bytearray()


def test_command_split_one_byte_at_a_time():
    body = b"*2\r\n$3\r\nfoo\r\n"
    wire = b"*1\r\n$%d\r\n" % len(body) + body + b"\r\n"
    _feed_one_byte_at_a_time(wire, [body])


def test_multi_argument_command_split_one_byte_at_a_time():
    argv_in = [b"SET", b"k", b"v"]
    wire = resp.encode_array([resp.encode_bulk_string(a) for a in argv_in])
    _feed_one_byte_at_a_time(wire, argv_in)


def test_blank_inline_line_and_empty_multibulk_consume_with_no_command():
    assert resp.parse_command(b"\r\n") == (None, 2)
    assert resp.parse_command(b"*0\r\n") == (None, 4)


def test_trailing_bytes_after_command_left_buffered():
    wire = b"*1\r\n$4\r\nPING\r\n"
    extra = b"extra"

    argv, consumed = resp.parse_command(wire + extra)

    assert argv == [b"PING"]
    assert consumed == len(wire)
    assert (wire + extra)[consumed:] == extra


# pipelining


def test_multiple_commands_in_one_buffer():
    first = b"*1\r\n$4\r\nPING\r\n"
    second = resp.encode_array([resp.encode_bulk_string(a) for a in [b"SET", b"k", b"v"]])
    buf = bytearray(first + second)

    commands = []
    while True:
        argv, consumed = resp.parse_command(buf)
        if consumed == 0:
            break
        del buf[:consumed]
        if argv is not None:
            commands.append(argv)

    assert commands == [[b"PING"], [b"SET", b"k", b"v"]]
    assert buf == bytearray()


# binary safety


def test_bulk_body_binary_safe_full_byte_range():
    body = bytes(range(256))
    wire = b"*1\r\n$%d\r\n" % len(body) + body + b"\r\n"

    argv, consumed = resp.parse_command(wire)

    assert argv == [body]
    assert consumed == len(wire)


def test_empty_bulk_string():
    assert resp.parse_command(b"*1\r\n$0\r\n\r\n") == ([b""], 10)
    assert resp.encode_bulk_string(b"") == b"$0\r\n\r\n"


def test_every_buffer_type_parses_and_always_yields_bytes():
    # memoryview has no .find(), so the parser normalises it first; nothing else in the suite exercises that branch
    wire = b"*1\r\n$1\r\nx\r\n"
    for buf in (wire, bytearray(wire), memoryview(wire)):
        argv, consumed = resp.parse_command(buf)
        assert argv == [b"x"] and consumed == len(wire), (type(buf), argv, consumed)
        assert type(argv[0]) is bytes, type(argv[0])


def test_parsed_argv_elements_are_bytes():
    argv, _ = resp.parse_command(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
    assert type(argv[0]) is bytes, type(argv[0])
    assert type(argv[1]) is bytes, type(argv[1])


# inline commands


def test_inline_command_without_framing():
    assert resp.parse_command(b"PING\r\n") == ([b"PING"], 6)
    assert resp.parse_command(b"SET k v\n") == ([b"SET", b"k", b"v"], 8)


# protocol errors


def test_non_dollar_element_inside_multibulk_is_rejected():
    cases = [
        (b"*1\r\n+OK\r\n", b"ERR Protocol error: expected '$', got '+'"),
        (b"*1\r\n*1\r\n", b"ERR Protocol error: expected '$', got '*'"),
    ]
    for wire, message in cases:
        with pytest.raises(resp.ProtocolError) as exc_info:
            resp.parse_command(wire)
        assert exc_info.value.message == message, (wire, exc_info.value.message)


def test_non_numeric_length_and_count_are_rejected():
    cases = [
        (b"*abc\r\n", b"ERR Protocol error: invalid multibulk length"),
        (b"*1\r\n$abc\r\n", b"ERR Protocol error: invalid bulk length"),
    ]
    for wire, message in cases:
        with pytest.raises(resp.ProtocolError) as exc_info:
            resp.parse_command(wire)
        assert exc_info.value.message == message, (wire, exc_info.value.message)


def test_bulk_body_not_terminated_by_crlf_is_rejected():
    with pytest.raises(resp.ProtocolError) as exc_info:
        resp.parse_command(b"*1\r\n$3\r\nfooXX\r\n")
    assert exc_info.value.message == b"ERR Protocol error: unterminated bulk string"
