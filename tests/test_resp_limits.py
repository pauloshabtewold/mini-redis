import pytest

import resp


def _assert_rejected_from_header_alone(wire, message):
    # wire is nothing but the header, so no body bytes follow it
    with pytest.raises(resp.ProtocolError) as exc_info:
        resp.parse_command(wire)
    assert exc_info.value.message == message, (wire, exc_info.value.message)


def test_negative_bulk_length_is_rejected():
    # multibulk wrapped
    _assert_rejected_from_header_alone(
        b"*1\r\n$-1\r\n", b"ERR Protocol error: invalid bulk length"
    )


def test_negative_multibulk_count_is_rejected():
    _assert_rejected_from_header_alone(
        b"*-1\r\n", b"ERR Protocol error: invalid multibulk length"
    )


def test_other_negative_bulk_length_is_rejected():
    _assert_rejected_from_header_alone(
        b"*1\r\n$-2\r\n", b"ERR Protocol error: invalid bulk length"
    )


def test_other_negative_multibulk_count_is_rejected():
    _assert_rejected_from_header_alone(
        b"*-5\r\n", b"ERR Protocol error: invalid multibulk length"
    )
