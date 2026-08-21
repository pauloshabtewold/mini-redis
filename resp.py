from collections.abc import Sequence

CRLF = b"\r\n"


class ProtocolError(Exception):
    """A RESP2 grammar violation. message is the RESP error body a
    client eventually sees, carried as `bytes` rather than `str` because
    it reaches the wire unchanged.
    """

    def __init__(self, message: bytes) -> None:
        super().__init__(message)
        self.message = message


def parse_command(buf: bytes | bytearray | memoryview) -> tuple[list[bytes] | None, int]:
    # memoryview has neither .find() nor .split(), so normalize once so the rest of this module can scan and slice buf uniformly.
    if isinstance(buf, memoryview):
        buf = bytes(buf)
    if buf[0:1] == b"*":
        return _parse_multibulk(buf)
    return _parse_inline(buf)


def _parse_length(field: bytes | bytearray, error_message: bytes) -> int:
    # isdigit() is False for a leading "-" as well as for any non-digit content, so "negative" and "not a decimal integer" collapse into the one check the shared error message already implies.
    if not field.isdigit():
        raise ProtocolError(error_message)
    return int(field)


def _parse_multibulk(buf: bytes | bytearray) -> tuple[list[bytes] | None, int]:
    header_end = buf.find(CRLF)
    if header_end == -1:
        return (None, 0)
    count = _parse_length(
        buf[1:header_end], b"ERR Protocol error: invalid multibulk length"
    )
    pos = header_end + 2
    if count == 0:
        return (None, pos)

    argv = []
    for _ in range(count):
        if pos >= len(buf):
            return (None, 0)
        if buf[pos:pos + 1] != b"$":
            raise ProtocolError(
                b"ERR Protocol error: expected '$', got '%c'" % buf[pos]
            )

        header_end = buf.find(CRLF, pos + 1)
        if header_end == -1:
            return (None, 0)
        length = _parse_length(
            buf[pos + 1:header_end], b"ERR Protocol error: invalid bulk length"
        )
        # a bulk body may legally contain \r\n, and searching for it truncates the value, so the end is computed from the declared length
        body_start = header_end + 2
        body_end = body_start + length
        if len(buf) < body_end + 2:
            return (None, 0)
        argv.append(bytes(buf[body_start:body_end]))
        if buf[body_end:body_end + 2] != CRLF:
            raise ProtocolError(b"ERR Protocol error: unterminated bulk string")

        pos = body_end + 2

    return (argv, pos)


def _parse_inline(buf: bytes | bytearray) -> tuple[list[bytes] | None, int]:
    newline = buf.find(b"\n")
    if newline == -1:
        return (None, 0)
    line = buf[:newline]
    if line.endswith(b"\r"):
        line = line[:-1]
    consumed = newline + 1
    argv = [bytes(field) for field in line.split()]
    if not argv:
        return (None, consumed)
    return (argv, consumed)


def encode_simple_string(value: bytes) -> bytes:
    return b"+" + value + CRLF


def encode_error(value: bytes) -> bytes:
    return b"-" + value + CRLF


def encode_integer(value: int) -> bytes:
    return b":%d" % value + CRLF


def encode_bulk_string(value: bytes | None) -> bytes:
    if value is None:
        # Null is response-only
        return b"$-1" + CRLF
    return b"$%d" % len(value) + CRLF + value + CRLF


def encode_array(parts: Sequence[bytes]) -> bytes:
    return b"*%d" % len(parts) + CRLF + b"".join(parts)
