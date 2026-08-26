"""RESP2 parser and serializer: bytes in, bytes out."""

from collections.abc import Sequence

CRLF = b"\r\n"

UNBALANCED_QUOTES = b"ERR Protocol error: unbalanced quotes in request"

# the escapes real Redis's sdssplitargs understands inside a double-quoted inline field
_INLINE_ESCAPES = {
    ord("n"): b"\n",
    ord("r"): b"\r",
    ord("t"): b"\t",
    ord("b"): b"\b",
    ord("a"): b"\a",
}

_HEX_DIGITS = frozenset(b"0123456789abcdefABCDEF")

# what ends an unquoted inline token, from sdssplitargs's own switch. not isspace(): that set carries
# \v and \f, which real Redis keeps inside a token, and omits \0, which real Redis splits on
_INLINE_SEPARATORS = frozenset(b" \n\r\t\0")


class ProtocolError(Exception):
    """A RESP2 grammar violation. message is the error's RESP body,
    carried as `bytes` rather than `str` because it goes on the wire
    unchanged.
    """

    def __init__(self, message: bytes) -> None:
        super().__init__(message)
        self.message = message


def parse_command(
    buf: bytes | bytearray | memoryview,
) -> tuple[list[bytes] | None, int, int]:
    """Parse one command off the front of buf.

    Returns (argv, consumed, needed). `needed` is the total buffer length at
    which another attempt can make progress, or 0 when no bound is known; it is
    meaningful only when `consumed` is 0. Without it a caller re-parses a
    partially delivered command from byte zero on every readable event, which is
    quadratic in the number of events it takes to deliver one.
    """
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
    try:
        return int(field)
    except ValueError:
        # isdigit() passes digit strings longer than CPython will convert, 4300 by default, and an escaping ValueError kills the process instead of the connection
        raise ProtocolError(error_message) from None


def _parse_multibulk(buf: bytes | bytearray) -> tuple[list[bytes] | None, int, int]:
    header_end = buf.find(CRLF)
    if header_end == -1:
        return (None, 0, 0)
    # a negative count is refused here, where real Redis consumes any count <= 0 silently and answers nothing -- a deliberate divergence, and the reason this call is not special-cased around the sign
    count = _parse_length(
        buf[1:header_end], b"ERR Protocol error: invalid multibulk length"
    )
    pos = header_end + 2
    if count == 0:
        # RESP's empty array: the header is consumed and no command comes out of it
        return (None, pos, 0)

    # every element is located before any of them is copied: an incomplete command that has already built an argv for the elements it passed is what makes a re-parse cost more than a scan
    bodies = []
    for _ in range(count):
        if pos >= len(buf):
            return (None, 0, pos + 1)
        if buf[pos:pos + 1] != b"$":
            # the byte is the client's and this message goes on the wire, so a raw \r or \n in it would split one error frame into two exactly as an unsanitized command name would in registry.dispatch
            got = bytes(buf[pos:pos + 1]).replace(b"\r", b" ").replace(b"\n", b" ")
            raise ProtocolError(
                b"ERR Protocol error: expected '$', got '%s'" % got
            )

        header_end = buf.find(CRLF, pos + 1)
        if header_end == -1:
            return (None, 0, 0)
        length = _parse_length(
            buf[pos + 1:header_end], b"ERR Protocol error: invalid bulk length"
        )
        # a bulk body may legally contain \r\n, and searching for it truncates the value, so the end is computed from the declared length
        body_start = header_end + 2
        body_end = body_start + length
        if len(buf) < body_end + 2:
            return (None, 0, body_end + 2)
        if buf[body_end:body_end + 2] != CRLF:
            raise ProtocolError(b"ERR Protocol error: unterminated bulk string")

        bodies.append((body_start, body_end))
        pos = body_end + 2

    return ([bytes(buf[start:end]) for start, end in bodies], pos, 0)


def _is_hex_escape(line: bytes, index: int) -> bool:
    # sliced rather than indexed because a line ending mid-escape must answer False, not IndexError
    digits = line[index + 2:index + 4]
    return (
        line[index + 1:index + 2] == b"x"
        and len(digits) == 2
        and all(digit in _HEX_DIGITS for digit in digits)
    )


def _split_inline(line: bytes) -> list[bytes]:
    # real Redis's sdssplitargs rather than bytes.split(): a quoted field is one argument however many spaces it holds, and an unbalanced quote is a protocol error rather than a token
    fields = []
    index = 0
    while True:
        while line[index:index + 1].isspace():
            index += 1
        if index >= len(line):
            return fields

        field = bytearray()
        in_double = False
        in_single = False
        done = False
        while not done:
            char = line[index:index + 1]
            if in_double:
                if char == b"\\" and _is_hex_escape(line, index):
                    field.append(int(line[index + 2:index + 4], 16))
                    index += 3
                elif char == b"\\" and line[index + 1:index + 2]:
                    index += 1
                    field += _INLINE_ESCAPES.get(line[index], line[index:index + 1])
                elif char == b'"':
                    # a closing quote ends the argument, so anything but a separator after it is a field this grammar cannot represent
                    if line[index + 1:index + 2] and not line[index + 1:index + 2].isspace():
                        raise ProtocolError(UNBALANCED_QUOTES)
                    done = True
                elif not char:
                    raise ProtocolError(UNBALANCED_QUOTES)
                else:
                    field += char
            elif in_single:
                if char == b"\\" and line[index + 1:index + 2] == b"'":
                    index += 1
                    field += b"'"
                elif char == b"'":
                    if line[index + 1:index + 2] and not line[index + 1:index + 2].isspace():
                        raise ProtocolError(UNBALANCED_QUOTES)
                    done = True
                elif not char:
                    raise ProtocolError(UNBALANCED_QUOTES)
                else:
                    field += char
            elif not char or char[0] in _INLINE_SEPARATORS:
                done = True
            elif char == b'"':
                in_double = True
            elif char == b"'":
                in_single = True
            else:
                field += char
            if char:
                index += 1

        fields.append(bytes(field))


def _parse_inline(buf: bytes | bytearray) -> tuple[list[bytes] | None, int, int]:
    newline = buf.find(b"\n")
    if newline == -1:
        return (None, 0, 0)
    line = buf[:newline]
    if line.endswith(b"\r"):
        line = line[:-1]
    consumed = newline + 1
    argv = _split_inline(bytes(line))
    if not argv:
        return (None, consumed, 0)
    return (argv, consumed, 0)


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
