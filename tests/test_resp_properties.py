"""The parser's contract over input this module builds, not its answers to chosen input.

Every other parser test here names an input and the output it expects, which is what
`test_resp.py` and `test_resp_limits.py` are for. That shape can only find defects on
inputs somebody thought of, and the one defect that reached a public remote -- a digit
run longer than the interpreter converts -- was on an input nobody would write by hand. These tests assert the properties instead: whatever the
bytes are, the parser raises nothing but `ProtocolError`, consumes no more than it was
given, leaves the caller's buffer alone, and never changes its mind about a command once
it has decoded one.

The corpus is built from a fixed seed, so a failure reproduces exactly. Each test
reports the first buffer that broke the property.
"""

import random

import resp
from tests.int_ceiling import OVERSIZED_DIGIT_RUN

SEED = 20260821
ROUNDS = 4000

# fragments chosen so that random concatenation lands on framing boundaries far more often
# than random bytes would -- headers, terminators, half-terminators and their near-misses.
# the quote and escape bytes are here because nothing else in the alphabet opens a quoted
# field, and the zero-prefixed headers because nothing else lands on a length that is padded
# rather than malformed: without either, those refusals are unreachable from this corpus
FRAGMENTS = [b"*", b"$", b"\r", b"\n", b"\r\n", b"0", b"1", b"2", b"9", b"-", b"+",
             b"a", b" ", b"\t", b"\x00", b"\xff", b"PING", b"12", b"$3", b"*2",
             b'"', b"'", b"\\", b"*0", b"$0"]

# complete commands and complete no-op headers: a buffer made only of these must drain to empty
UNITS = [b"*1\r\n$4\r\nPING\r\n", b"PING\r\n", b"SET k v\n", b"*0\r\n", b"\r\n", b"\n",
         b"*1\r\n$0\r\n\r\n", b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", b"*1\r\n$6\r\na\r\nb\r\n\r\n"]

WELL_FORMED = [b"*1\r\n$4\r\nPING\r\n", b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", b"*1\r\n$0\r\n\r\n",
               b"PING\r\n", b"*0\r\n", b"*1\r\n$6\r\na\r\nb\r\n\r\n"]

# headers whose declared sizes are far past anything a fragment run produces: the digit runs
# either side of the interpreter's conversion ceiling, and sizes no buffer will ever hold.
# Kept out of CORPUS because the prefix properties walk every cut of every wire
EXTREME = [b"*" + OVERSIZED_DIGIT_RUN + b"\r\n",
           b"*1\r\n$" + OVERSIZED_DIGIT_RUN + b"\r\n",
           b"*" + b"9" * 64 + b"\r\n",
           b"*1\r\n$" + b"9" * 64 + b"\r\n",
           b"*1000000000\r\n",
           b"*1\r\n$1073741824\r\n",
           b"*1\r\n$1073741824\r\nab\r\n"]


def _corpus():
    """Random fragment runs, every well-formed wire, and every single-byte corruption of one."""
    rnd = random.Random(SEED)
    wires = list(WELL_FORMED)
    for _ in range(ROUNDS):
        wires.append(b"".join(rnd.choice(FRAGMENTS) for _ in range(rnd.randint(0, 12))))
    for base in WELL_FORMED:
        for index in range(len(base)):
            for replacement in (b"*", b"$", b"\r", b"\n", b"0", b"9", b"-", b"a", b"\xff", b""):
                wires.append(base[:index] + replacement + base[index + 1:])
    return wires


CORPUS = _corpus()


def _parse(wire):
    """(outcome, result), where outcome is None for a parse and "refused" for a ProtocolError."""
    try:
        return None, resp.parse_command(wire)
    except resp.ProtocolError:
        return "refused", None


def _as_every_buffer_type(wire):
    return bytes(wire), bytearray(wire), memoryview(bytes(wire))


def test_no_input_escapes_as_anything_but_a_protocol_error():
    # the shape of the defect that reached a public remote: an exception type the caller's
    # except clause does not name escapes the connection boundary and kills the process
    for wire in CORPUS + EXTREME:
        for buf in _as_every_buffer_type(wire):
            try:
                resp.parse_command(buf)
            except resp.ProtocolError:
                pass
            except Exception as exc:                      # noqa: BLE001 -- the escape IS the finding
                raise AssertionError(
                    "%r escaped on %s %r" % (exc, type(buf).__name__, bytes(wire)[:80])
                ) from exc


def test_consumed_never_exceeds_the_buffer_and_argv_is_always_bytes():
    # every buffer type, because a slice of a bytearray is a bytearray and a slice of a
    # memoryview is a memoryview: a missing bytes() conversion is invisible on bytes input
    for wire in CORPUS + EXTREME:
        for buf in _as_every_buffer_type(wire):
            outcome, out = _parse(buf)
            if outcome:
                continue
            argv, consumed, _needed = out
            assert 0 <= consumed <= len(wire), (bytes(wire)[:80], consumed)
            assert argv is None or consumed > 0, (bytes(wire)[:80], argv)
            if argv is not None:
                assert all(type(a) is bytes for a in argv), (
                    type(buf).__name__, bytes(wire)[:80], [type(a).__name__ for a in argv])


def test_parse_command_never_writes_to_the_callers_buffer():
    # the buffer belongs to Connection, which deletes the consumed prefix itself; a parser
    # that trimmed it too would double-consume and lose whatever followed
    for wire in CORPUS + EXTREME:
        buf = bytearray(wire)
        before = bytes(buf)
        try:
            resp.parse_command(buf)
        except resp.ProtocolError:
            pass
        assert bytes(buf) == before, bytes(wire)[:80]


def test_a_command_decoded_from_a_prefix_survives_the_arrival_of_more_bytes():
    # take_commands() re-enters the parser on a growing buffer, so an answer that changes
    # once more bytes land means the same stream parses differently depending on recv() sizes
    for wire in CORPUS:
        full_outcome, full_out = _parse(wire)
        for cut in range(len(wire) + 1):
            outcome, out = _parse(wire[:cut])
            if outcome:
                break
            if out[0] is None or out[1] == 0:
                continue
            assert full_outcome is None, (wire[:80], cut, out)
            assert out == full_out, (wire[:80], cut, out, full_out)
            break


def test_a_prefix_of_an_accepted_buffer_is_never_rejected():
    # a rejection is a verdict about bytes the sender has not finished sending; reaching one
    # early closes a connection over a command that would have been valid once complete
    for wire in CORPUS:
        if _parse(wire)[0]:
            continue
        for cut in range(len(wire) + 1):
            assert not _parse(wire[:cut])[0], (wire[:80], cut)


def test_a_buffer_of_complete_units_drains_to_empty():
    # a unit that yields no command still has to report the bytes it consumed; one that
    # reports zero stalls take_commands() on a buffer that is entirely well formed
    rnd = random.Random(SEED)
    for _ in range(600):
        units = [rnd.choice(UNITS) for _ in range(rnd.randint(1, 5))]
        buf = bytearray(b"".join(units))
        for _ in range(len(units) + 4):
            _argv, consumed, _needed = resp.parse_command(buf)
            if consumed == 0:
                break
            del buf[:consumed]
        assert buf == bytearray(), (units, bytes(buf[:40]))


def test_every_encoded_command_parses_back_byte_identically():
    # the round trip a serialize/parse pair cannot fake: bodies are built out of the framing
    # bytes themselves, so a parser that scans for CRLF instead of honouring the length fails
    rnd = random.Random(SEED)
    pieces = [b"", b"a", b"\r\n", b"\r", b"\n", b"$3\r\nfoo\r\n", b"*2\r\n", b"\x00\xff", b"PING"]
    payloads = [[bytes(range(256))]]
    for _ in range(1500):
        payloads.append([b"".join(rnd.choice(pieces) for _ in range(rnd.randint(0, 4)))
                         for _ in range(rnd.randint(1, 4))])
    for argv_in in payloads:
        wire = resp.encode_array([resp.encode_bulk_string(a) for a in argv_in])
        assert resp.parse_command(wire) == (argv_in, len(wire), 0), (argv_in, wire[:80])
