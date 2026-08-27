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

# what separates two unquoted inline tokens. not isspace(): that set carries \v and \f, which real
# Redis keeps inside a token. \0 is not here either -- it ends the line rather than a token, below
_INLINE_SEPARATORS = frozenset({b" ", b"\n", b"\r", b"\t"})

# real Redis reads a multibulk count with string2ll and refuses anything above INT_MAX outright.
# a bulk length has its own, configurable ceiling, which is a later feature's flag rather than this
MAX_MULTIBULK_COUNT = 2**31 - 1


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
    meaningful only when `consumed` is 0. Progress means an argv or an error:
    the hint can only ever bound completion, so it never runs past the first
    byte that could carry a protocol error, and a caller that waits longer than
    it says turns malformed input into a hang.

    Only the multibulk path produces a non-zero hint, and only from a bulk
    element, where a declared length lets it skip the whole body rather than
    re-scanning it per byte. The inline path has no return site that can carry
    one: a line is complete or it is not, and nothing about a partial line
    bounds when a newline will arrive.

    Connection drives parse_multibulk_header and parse_bulk_element directly
    rather than calling this for a multibulk, so that locating N elements costs
    N steps rather than N per element. This is the whole-command form, over
    those same two primitives.
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
    # real Redis reads both fields with string2ll, which takes "0" only as the whole field and otherwise
    # wants a first digit of 1 to 9, so a padded count or length is a framing error there and here
    if len(field) > 1 and field[0:1] == b"0":
        raise ProtocolError(error_message)
    try:
        return int(field)
    except ValueError:
        # isdigit() passes digit strings longer than CPython will convert, 4300 by default, and an escaping ValueError kills the process instead of the connection
        raise ProtocolError(error_message) from None


def parse_multibulk_header(
    buf: bytes | bytearray | memoryview,
) -> tuple[int, int, int]:
    """Parse `*N\\r\\n` off the front of buf, returning (count, consumed, needed).

    Split from the elements so a caller can consume the header and then each
    element as it arrives, keeping its own running argv. A caller that instead
    re-parses the whole command on every readable event pays for locating each
    element once per element, which is quadratic in the count.
    """
    header_end = buf.find(CRLF)
    if header_end == -1:
        return (0, 0, 0)
    # a negative count is refused here, where real Redis consumes a syntactically valid count of zero or less silently and answers nothing -- a deliberate divergence, and the reason this call is not special-cased around the sign. "syntactically valid" is the whole rule: string2ll refuses -0 there as it does here, so the two agree on -0 and on 0 and part only on -1 and below
    invalid_count = b"ERR Protocol error: invalid multibulk length"
    count = _parse_length(buf[1:header_end], invalid_count)
    if count > MAX_MULTIBULK_COUNT:
        # refused at the header, as the reference does, rather than read one command later as an element
        raise ProtocolError(invalid_count)
    return (count, header_end + 2, 0)


def parse_bulk_element(
    buf: bytes | bytearray | memoryview,
) -> tuple[bytes | None, int, int]:
    """Parse one `$N\\r\\n<body>\\r\\n` element off the front of buf.

    Returns (body, consumed, needed) on the same contract as parse_command.
    """
    if not len(buf):
        # the type byte alone decides between an element and a protocol error, so one byte is progress
        return (None, 0, 1)
    if buf[0:1] != b"$":
        # the byte is the client's and this message goes on the wire, so a raw \r or \n in it would split one error frame into two exactly as an unsanitized command name would in registry.dispatch
        got = bytes(buf[0:1]).replace(b"\r", b" ").replace(b"\n", b" ")
        raise ProtocolError(b"ERR Protocol error: expected '$', got '%s'" % got)
    header_end = buf.find(CRLF, 1)
    if header_end == -1:
        return (None, 0, 0)
    length = _parse_length(
        buf[1:header_end], b"ERR Protocol error: invalid bulk length"
    )
    # a bulk body may legally contain \r\n, and searching for it truncates the value, so the end is computed from the declared length
    body_start = header_end + 2
    body_end = body_start + length
    if len(buf) < body_end + 2:
        return (None, 0, body_end + 2)
    if buf[body_end:body_end + 2] != CRLF:
        raise ProtocolError(b"ERR Protocol error: unterminated bulk string")
    return (bytes(buf[body_start:body_end]), body_end + 2, 0)


def _parse_multibulk(buf: bytes | bytearray) -> tuple[list[bytes] | None, int, int]:
    # the whole-command form, over the same two primitives Connection drives one step at a time, so there is one grammar here rather than two that can disagree
    count, consumed, needed = parse_multibulk_header(buf)
    if consumed == 0:
        return (None, 0, needed)
    if count == 0:
        # RESP's empty array: the header is consumed and no command comes out of it
        return (None, consumed, 0)

    argv = []
    pos = consumed
    for _ in range(count):
        body, used, needed = parse_bulk_element(buf[pos:])
        if used == 0:
            # needed is relative to the element; 0 stays 0, which means no bound is known
            return (None, 0, pos + needed if needed else 0)
        argv.append(body)
        pos += used

    return (argv, pos, 0)


def _is_hex_escape(line: bytes, index: int) -> bool:
    # sliced rather than indexed because a line ending mid-escape must answer False, not IndexError
    digits = line[index + 2:index + 4]
    return (
        line[index + 1:index + 2] == b"x"
        and len(digits) == 2
        and all(digit in _HEX_DIGITS for digit in digits)
    )


def _terminated(line: bytes, index: int) -> bool:
    # sdssplitargs scans a NUL-terminated string, so running off the end and meeting a NUL are the
    # same fact in every state below -- including inside quotes, where the terminator is what makes
    # them unbalanced, and after a closing quote, where it is what makes the field well formed
    char = line[index:index + 1]
    return not char or char == b"\x00"


def _split_inline(line: bytes) -> list[bytes]:
    # real Redis's sdssplitargs rather than bytes.split(): a quoted field is one argument however many spaces it holds, and an unbalanced quote is a protocol error rather than a token
    fields = []
    index = 0
    while True:
        while line[index:index + 1].isspace():
            index += 1
        if _terminated(line, index):
            # sdssplitargs guards its token loop with `if (*p)`, so a token that would start at the
            # terminator is never built -- the vector comes back empty rather than holding an empty field
            return fields

        field = bytearray()
        in_double = False
        in_single = False
        done = False
        at_end = False
        while not done:
            char = line[index:index + 1]
            if in_double:
                if char == b"\\" and _is_hex_escape(line, index):
                    field.append(int(line[index + 2:index + 4], 16))
                    index += 3
                elif char == b"\\" and not _terminated(line, index + 1):
                    index += 1
                    field += _INLINE_ESCAPES.get(line[index], line[index:index + 1])
                elif char == b'"':
                    # a closing quote ends the argument, so anything but a separator after it is a field this grammar cannot represent
                    if not _terminated(line, index + 1) and not line[index + 1:index + 2].isspace():
                        raise ProtocolError(UNBALANCED_QUOTES)
                    done = True
                elif _terminated(line, index):
                    raise ProtocolError(UNBALANCED_QUOTES)
                else:
                    field += char
            elif in_single:
                if char == b"\\" and line[index + 1:index + 2] == b"'":
                    index += 1
                    field += b"'"
                elif char == b"'":
                    if not _terminated(line, index + 1) and not line[index + 1:index + 2].isspace():
                        raise ProtocolError(UNBALANCED_QUOTES)
                    done = True
                elif _terminated(line, index):
                    raise ProtocolError(UNBALANCED_QUOTES)
                else:
                    field += char
            elif _terminated(line, index):
                # sdssplitargs's case '\0' is the C string terminator: it ends this token, and the outer
                # while(*p) then ends the line. it is not a separator the scan steps over and continues past
                done = True
                at_end = True
            elif char in _INLINE_SEPARATORS:
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
        if at_end:
            return fields


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
