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
