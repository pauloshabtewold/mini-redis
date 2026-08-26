import pytest

import resp


def _feed_one_byte_at_a_time(wire, expected_argv):
    # Append wire into a bytearray one byte at a time, calling parse command after every byte and deleting the consumed prefix
    buf = bytearray()
    last_index = len(wire) - 1
    for index, byte in enumerate(wire):
        buf.append(byte)
        argv, consumed, _ = resp.parse_command(buf)
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
    assert resp.parse_command(b"*1\r\n$4\r\nPING\r\n") == ([b"PING"], 14, 0)


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
    argv_out, consumed, _ = resp.parse_command(wire)
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

    argv, consumed, _ = resp.parse_command(wire)

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
    assert resp.parse_command(b"\r\n") == (None, 2, 0)
    assert resp.parse_command(b"*0\r\n") == (None, 4, 0)


def test_zero_multibulk_count_consumes_header_with_no_command():
    # zero is RESP's empty array and is consumed. the negative counts beside it are refused instead,
    # pinned in tests/test_resp_limits.py -- the two live apart because only one is a rejection
    assert resp.parse_command(b"*0\r\n") == (None, 4, 0)


def test_trailing_bytes_after_command_left_buffered():
    wire = b"*1\r\n$4\r\nPING\r\n"
    extra = b"extra"

    argv, consumed, _ = resp.parse_command(wire + extra)

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
        argv, consumed, _ = resp.parse_command(buf)
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

    argv, consumed, _ = resp.parse_command(wire)

    assert argv == [body]
    assert consumed == len(wire)


def test_empty_bulk_string():
    assert resp.parse_command(b"*1\r\n$0\r\n\r\n") == ([b""], 10, 0)
    assert resp.encode_bulk_string(b"") == b"$0\r\n\r\n"


def test_every_buffer_type_parses_and_always_yields_bytes():
    # memoryview has no .find(), so the parser normalises it first; nothing else in the suite exercises that branch
    wire = b"*1\r\n$1\r\nx\r\n"
    for buf in (wire, bytearray(wire), memoryview(wire)):
        argv, consumed, _ = resp.parse_command(buf)
        assert argv == [b"x"] and consumed == len(wire), (type(buf), argv, consumed)
        assert type(argv[0]) is bytes, type(argv[0])


def test_parsed_argv_elements_are_bytes():
    argv, _, _ = resp.parse_command(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
    assert type(argv[0]) is bytes, type(argv[0])
    assert type(argv[1]) is bytes, type(argv[1])


# inline commands


def test_inline_command_without_framing():
    assert resp.parse_command(b"PING\r\n") == ([b"PING"], 6, 0)
    assert resp.parse_command(b"SET k v\n") == ([b"SET", b"k", b"v"], 8, 0)


INLINE_QUOTING_CASES = [
    pytest.param(b'ECHO "a b"\r\n', [b"ECHO", b"a b"], 12, id="double quotes keep an embedded space"),
    pytest.param(b"ECHO 'a b'\r\n", [b"ECHO", b"a b"], 12, id="single quotes keep an embedded space"),
    pytest.param(b'ECHO "a\\x41b"\r\n', [b"ECHO", b"aAb"], 15, id="hex escape in double quotes"),
    pytest.param(b'ECHO "a\\nb"\r\n', [b"ECHO", b"a\nb"], 13, id="newline escape in double quotes"),
    pytest.param(b'ECHO "a\\tb"\r\n', [b"ECHO", b"a\tb"], 13, id="tab escape in double quotes"),
    pytest.param(b"ECHO 'a\\'b'\r\n", [b"ECHO", b"a'b"], 13, id="escaped quote in single quotes"),
]


@pytest.mark.parametrize("wire, expected_argv, expected_consumed", INLINE_QUOTING_CASES)
def test_inline_quoting_table(wire, expected_argv, expected_consumed):
    assert resp.parse_command(wire) == (expected_argv, expected_consumed, 0)


INLINE_UNBALANCED_QUOTE_CASES = [
    pytest.param(b'ECHO "abc\r\n', id="missing closing quote"),
    pytest.param(b'ECHO "abc"def\r\n', id="trailing bytes after a closing quote"),
]


@pytest.mark.parametrize("wire", INLINE_UNBALANCED_QUOTE_CASES)
def test_inline_unbalanced_quote_is_rejected(wire):
    with pytest.raises(resp.ProtocolError) as exc_info:
        resp.parse_command(wire)
    assert exc_info.value.message == resp.UNBALANCED_QUOTES


# sdssplitargs ends an unquoted token on exactly ' ', \n, \r, \t and \0. isspace() is the wrong
# instrument: it admits \v and \f, which real Redis keeps inside a token, and rejects \0, which real
# Redis splits on. \n is absent below because it ends the line before the splitter is reached
@pytest.mark.parametrize("sep", [b" ", b"\r", b"\t", b"\x00"])
def test_inline_token_ends_on_every_sdssplitargs_separator(sep):
    argv, _consumed, _needed = resp.parse_command(b"ECHO" + sep + b"x\n")
    assert argv == [b"ECHO", b"x"], (sep, argv)


@pytest.mark.parametrize("inner", [b"\x0b", b"\x0c"])
def test_vertical_tab_and_form_feed_stay_inside_an_inline_token(inner):
    # measured against redis-server 7.2.7: PING\x0bX is one unknown command there, not PING with an
    # argument, because sdssplitargs's terminator switch does not name them
    argv, _consumed, _needed = resp.parse_command(b"ECHO" + inner + b"x\n")
    assert argv == [b"ECHO" + inner + b"x"], (inner, argv)


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


def test_control_bytes_in_error_frame_are_sanitized_but_printable_bytes_still_render():
    # got is interpolated straight into the RESP error frame; a raw \r or \n there would split one frame into two on the wire, the way an unsanitized name once did in registry.dispatch
    for wire in (b"*1\r\n\r", b"*1\r\n\n"):
        with pytest.raises(resp.ProtocolError) as exc_info:
            resp.parse_command(wire)
        frame = resp.encode_error(exc_info.value.message)
        assert frame.count(b"\r\n") == 1, (wire, frame)
        body = frame[1:-2]
        assert b"\r" not in body and b"\n" not in body, (wire, frame)

    with pytest.raises(resp.ProtocolError) as exc_info:
        resp.parse_command(b"*1\r\n+OK\r\n")
    assert exc_info.value.message == b"ERR Protocol error: expected '$', got '+'"


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
