import sys

import pytest

import resp

# one row per input shape the parser must refuse from the header alone, with no body bytes following it
REJECTED_HEADERS = [
    (b"*1\r\n$-1\r\n", b"ERR Protocol error: invalid bulk length"),
    (b"*1\r\n$-2\r\n", b"ERR Protocol error: invalid bulk length"),
    (b"*-1\r\n", b"ERR Protocol error: invalid multibulk length"),
    (b"*-5\r\n", b"ERR Protocol error: invalid multibulk length"),
    # a digit run longer than CPython converts: isdigit() passes it and int() raises, so an unguarded parser dies here rather than rejecting
    (b"*1\r\n$" + b"9" * 4301 + b"\r\n", b"ERR Protocol error: invalid bulk length"),
    (b"*" + b"9" * 4301 + b"\r\n", b"ERR Protocol error: invalid multibulk length"),
]


@pytest.mark.parametrize("wire, message", REJECTED_HEADERS)
def test_rejected_from_header_alone(wire, message):
    with pytest.raises(resp.ProtocolError) as exc_info:
        resp.parse_command(wire)
    assert exc_info.value.message == message, (wire[:32], exc_info.value.message)


def test_oversized_digit_run_raises_protocol_error_not_value_error():
    # the guard is the interpreter's, so pin the assumption it rests on: a field one digit past the ceiling must not reach int() unguarded
    ceiling = sys.get_int_max_str_digits()
    field = b"9" * (ceiling + 1)
    with pytest.raises(ValueError):
        int(field)              # the raw conversion the parser must never let escape
    assert field.isdigit()      # and which the isdigit() guard alone does not stop
    with pytest.raises(resp.ProtocolError):
        resp.parse_command(b"*" + field + b"\r\n")
