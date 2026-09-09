import pytest

import resp
from tests.int_ceiling import (
    NO_CEILING_REASON,
    NO_CONVERSION_CEILING,
    OVERSIZED_DIGIT_RUN,
)

# one row per input shape the parser must refuse from the header alone, with no body bytes following it
REJECTED_HEADERS = [
    pytest.param(b"*1\r\n$-1\r\n", b"ERR Protocol error: invalid bulk length", id="bulk -1"),
    pytest.param(b"*1\r\n$-2\r\n", b"ERR Protocol error: invalid bulk length", id="bulk -2"),
    # real Redis consumes a negative count silently and this server refuses it. these two rows are
    # that divergence rather than a description of it -- deleting them deletes its only guard
    pytest.param(b"*-1\r\n", b"ERR Protocol error: invalid multibulk length", id="multibulk -1"),
    pytest.param(b"*-5\r\n", b"ERR Protocol error: invalid multibulk length", id="multibulk -5"),
    # a digit run longer than the interpreter converts: isdigit() passes it and int() raises, so an
    # unguarded parser dies here rather than rejecting. Skipped rather than silently dropped where
    # the ceiling is disabled, since the shape has no witness there at all
    pytest.param(
        b"*1\r\n$" + OVERSIZED_DIGIT_RUN + b"\r\n",
        b"ERR Protocol error: invalid bulk length",
        id="bulk length past the conversion ceiling",
        marks=pytest.mark.skipif(NO_CONVERSION_CEILING, reason=NO_CEILING_REASON),
    ),
    pytest.param(
        b"*" + OVERSIZED_DIGIT_RUN + b"\r\n",
        b"ERR Protocol error: invalid multibulk length",
        id="multibulk count past the conversion ceiling",
        marks=pytest.mark.skipif(NO_CONVERSION_CEILING, reason=NO_CEILING_REASON),
    ),
]


@pytest.mark.parametrize("wire, message", REJECTED_HEADERS)
def test_rejected_from_header_alone(wire, message):
    with pytest.raises(resp.ProtocolError) as exc_info:
        resp.parse_command(wire)
    assert exc_info.value.message == message, (wire[:32], exc_info.value.message)


def test_oversized_digit_run_raises_protocol_error_not_value_error():
    # the guard is the interpreter's, so pin the assumption it rests on rather than a constant.
    # Both arms assert, so no interpreter setting leaves this test running and checking nothing
    if NO_CONVERSION_CEILING:
        # nothing isdigit() admits can make int() raise here, so the contract is not that the
        # header is refused but that it is still incomplete -- waiting, never crashing
        huge_multibulk = b"*" + b"9" * 5000 + b"\r\n"
        huge_bulk = b"*1\r\n$" + b"9" * 5000 + b"\r\n"
        assert resp.parse_command(huge_multibulk) == (None, 0, len(huge_multibulk) + 1)
        assert resp.parse_command(huge_bulk) == (None, 0, len(huge_bulk) + int(b"9" * 5000) + 2)
        return
    with pytest.raises(ValueError):
        int(OVERSIZED_DIGIT_RUN)              # the raw conversion the parser must never let escape
    assert OVERSIZED_DIGIT_RUN.isdigit()      # and which the isdigit() guard alone does not stop
    with pytest.raises(resp.ProtocolError):
        resp.parse_command(b"*" + OVERSIZED_DIGIT_RUN + b"\r\n")


# rows needing a cap have nowhere to go in REJECTED_HEADERS above: test_rejected_from_header_alone
# calls resp.parse_command(wire) with no caps at all, so a row that needs one gets its own table.
# rows are (wire, caps, message); the test passes **caps through to parse_command unchanged
REJECTED_UNDER_A_CAP = [
    pytest.param(
        b"*1\r\n$65\r\n", {"max_value_size": 64},
        b"ERR Protocol error: invalid bulk length",
        id="bulk length one over the cap",
    ),
    pytest.param(
        b"*65\r\n", {"max_multibulk": 64},
        b"ERR Protocol error: invalid multibulk length",
        id="multibulk count one over the cap",
    ),
    # --max-value-size bounds every element, not only the one a human would call a value --
    # each position gets its own row, so a fix that skips element zero still fails two of
    # these three
    pytest.param(
        b"*3\r\n$99\r\n", {"max_value_size": 8},
        b"ERR Protocol error: invalid bulk length",
        id="over-cap element in the command-name slot",
    ),
    pytest.param(
        b"*3\r\n$3\r\nSET\r\n$99\r\n", {"max_value_size": 8},
        b"ERR Protocol error: invalid bulk length",
        id="over-cap element in the key slot",
    ),
    pytest.param(
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$99\r\n", {"max_value_size": 8},
        b"ERR Protocol error: invalid bulk length",
        id="over-cap element in the value slot",
    ),
    pytest.param(
        b"*1\r\n$68157440\r\n", {"max_value_size": 67108864},
        b"ERR Protocol error: invalid bulk length",
        id="65 MiB over the 64 MiB CLI default",
    ),
    pytest.param(
        b"*1000000000\r\n", {"max_multibulk": 1048576},
        b"ERR Protocol error: invalid multibulk length",
        id="over the 1,048,576 CLI default",
    ),
]


@pytest.mark.parametrize("wire, caps, message", REJECTED_UNDER_A_CAP)
def test_rejected_under_a_cap(wire, caps, message):
    with pytest.raises(resp.ProtocolError) as exc_info:
        resp.parse_command(wire, **caps)
    assert exc_info.value.message == message, (wire[:32], caps, exc_info.value.message)


def test_a_declared_size_exactly_at_the_cap_is_accepted():
    # the comparison is `>`, never `>=` -- a declared length or count exactly
    # equal to the cap is accepted, and only one byte or one element more is refused
    resp.parse_command(b"*1\r\n$64\r\n", max_value_size=64)
    resp.parse_command(b"*64\r\n", max_multibulk=64)
    resp.parse_command(b"*1048576\r\n", max_multibulk=1048576)  # exactly 1024 * 1024
    # nothing above raised: an uncaught ProtocolError would have failed this test


def test_a_zero_cap_disables_and_is_the_default():
    # 0 disables each cap and is every parameter's default, so every existing
    # call site that passes no cap -- 48 parse_command sites, 3 parse_bulk_element,
    # 3 parse_multibulk_header -- keeps its present, uncapped behaviour with no edit
    whole = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
    expected = ([b"SET", b"k", b"v"], len(whole), 0)
    assert resp.parse_command(whole) == expected
    assert resp.parse_command(whole, max_value_size=0, max_multibulk=0) == expected, \
        "an explicit 0 must disable, not refuse"
    # the subtler half: an empty bulk is not over any cap, however small
    wire = b"*1\r\n$0\r\n\r\n"
    assert resp.parse_command(wire, max_value_size=1) == ([b""], len(wire), 0)


def test_the_int_max_ceiling_holds_with_the_configurable_cap_off():
    # MAX_MULTIBULK_COUNT is the reference's own unconditional INT_MAX ceiling and
    # must hold whether the configurable cap is off (0) or merely set above it -- folding
    # the two into one comparison would make the hard ceiling disappear exactly when the
    # configurable one is switched off
    over_int_max = b"*2147483648\r\n"
    for caps in ({"max_multibulk": 0}, {"max_multibulk": 4000000000}):
        with pytest.raises(resp.ProtocolError) as exc_info:
            resp.parse_command(over_int_max, **caps)
        assert exc_info.value.message == b"ERR Protocol error: invalid multibulk length", caps


def test_the_caps_reach_the_primitives_directly_not_only_parse_command():
    # the live server drives parse_multibulk_header and parse_bulk_element directly
    # (connection.py), never through parse_command -- a cap threaded only into parse_command
    # would leave the live path uncapped while every test that only calls parse_command
    # stayed green
    for call, wire, caps, want in [
        (resp.parse_bulk_element, b"$65\r\n", {"max_value_size": 64},
         b"ERR Protocol error: invalid bulk length"),
        (resp.parse_multibulk_header, b"*65\r\n", {"max_multibulk": 64},
         b"ERR Protocol error: invalid multibulk length"),
    ]:
        with pytest.raises(resp.ProtocolError) as exc_info:
            call(wire, **caps)
        assert exc_info.value.message == want, (call.__name__, exc_info.value.message)

    # and the whole-command form forwards max_value_size into the element loop, not only
    # into the header: the parse_bulk_element(buf[pos:]) call inside _parse_multibulk is
    # the one call in the file with no search_from, and so the easiest to leave unforwarded
    with pytest.raises(resp.ProtocolError) as exc_info:
        resp.parse_command(b"*2\r\n$1\r\na\r\n$65\r\n", max_value_size=64)
    assert exc_info.value.message == b"ERR Protocol error: invalid bulk length"


def test_a_terminated_inline_line_over_the_ceiling_is_refused():
    # a terminated inline line over MAX_INLINE_SIZE is refused under either
    # terminator the inline grammar accepts -- a bare \n ends a line too
    over = b"ECHO " + b"A" * (resp.MAX_INLINE_SIZE + 1 - 5)
    for terminator in (b"\r\n", b"\n"):
        with pytest.raises(resp.ProtocolError) as exc_info:
            resp.parse_command(over + terminator)
        assert exc_info.value.message == resp.TOO_BIG_INLINE, terminator


def test_an_inline_line_exactly_at_the_ceiling_is_accepted():
    # a line's \r\n terminator is framing, not content, and must not be counted -- a
    # line measured with it included would refuse a legal MAX_INLINE_SIZE-byte line as one
    # or two bytes over. The assertion names the quantity it is about
    assert resp.MAX_INLINE_SIZE == 64 * 1024
    line = b"ECHO " + b"A" * (resp.MAX_INLINE_SIZE - 5)
    assert len(line) == resp.MAX_INLINE_SIZE
    argv, consumed, needed = resp.parse_command(line + b"\r\n")
    assert argv == [b"ECHO", b"A" * (resp.MAX_INLINE_SIZE - 5)]
    assert (consumed, needed) == (len(line) + 2, 0)


def test_an_unterminated_inline_line_is_bounded_by_no_size():
    # this test's whole job is to fail if that deferral is closed by accident.
    # A line that has not ended has no declared length to compare against anything, so it
    # stays unbounded here, deferred to the future --incomplete-command-timeout flag
    # -- a pinned absence rather than a pinned behaviour, and it reads as deletable
    # without this comment
    line = b"ECHO " + b"A" * (resp.MAX_INLINE_SIZE * 3)
    assert resp.parse_command(line) == (None, 0, 0)
