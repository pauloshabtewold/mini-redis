"""Versioned snapshot format: save and load.

A blob is `MAGIC`, a little-endian `uint32` version, a little-endian `uint32` key count,
then one entry per key -- a `uint32` key length, the key bytes, one type byte, a signed
little-endian `int64` absolute expiry in milliseconds (`-1` for none), and the value: a
string as a `uint32` length plus bytes, a list as a `uint32` element count -- never
zero, because no command leaves an empty list behind: popping the last element removes
the key -- then a `uint32` length plus bytes per element -- and closes with a
little-endian `uint32` `zlib.crc32` of every byte before it.

`decode()` checks in this order and no other: the blob is at least sixteen bytes, the
magic matches, the trailer's checksum matches `zlib.crc32` of everything before it, the
version is one this module knows, then the entries parse. The checksum is verified
before any length prefix in the blob is trusted -- a flip inside a length field meets an
absurd number and a flip inside a payload byte would otherwise pass every bounds check
there is, so checking lengths first would refuse some corruption for the wrong reason and
accept the rest with silently wrong data.

`encode()`/`decode()` are the byte layer with no filesystem involved: a blob decodes the
same whatever it was read from, because neither function touches a path. Decoding
proceeds field by field rather than through `pickle` or `marshal`, because a snapshot is
untrusted input -- a file on disk can be corrupted or replaced, and the byte layer takes
bytes from any source -- and unpickling untrusted bytes executes arbitrary code.
`save()`/`load()` are the thin filesystem layer above them, and `check_writable()` refuses
a path a `save()` could not complete as things stand, before a server starts over it.
"""

import os
import struct
import tempfile
import zlib

from store import KIND_LIST, KIND_STRING, Store

# four ASCII bytes rather than a numeric constant, so a corrupt or truncated
# snapshot is still recognisable by eye in a hex dump
MAGIC = b"MRDB"
SNAPSHOT_VERSION = 1

# the format's own one-byte type codes: the byte's width and its two meanings --
# string or list -- are the format's own, and the numbers 0 and 1 are this project's
# choice. they are NOT Store's KIND_STRING/KIND_LIST: those are b"string" and b"list",
# six bytes and four where this field is one, and indexing bytes yields an int, so
# comparing this field against either of them is int == bytes and false however the
# format is implemented. public rather than underscored so a test names the code instead
# of a literal 0, which goes stale silently if these numbers ever move
TYPE_STRING = 0
TYPE_LIST = 1


class SnapshotError(Exception):
    """A snapshot could not be decoded or read, or its path failed the startup check.

    The one exception any caller catches: corrupt bytes, a checksum mismatch, an
    unsupported version, a path that exists but cannot be read, or a path
    `check_writable()` refuses. A `save()` that fails raises `OSError` instead, and a
    missing file raises `FileNotFoundError` -- see `load()` -- because absent and
    corrupt are different answers to the caller and `Server.__init__` acts on them
    differently.
    """


def encode(store: Store) -> bytes:
    """Serialize every key in `store`, including a resident-but-expired one, to bytes."""
    entries = []
    count = 0
    for key, kind, value, expiry in store.snapshot_items():
        entries.append(_encode_entry(key, kind, value, expiry))
        count += 1
    body = MAGIC + struct.pack("<II", SNAPSHOT_VERSION, count) + b"".join(entries)
    # every byte before the trailer, so a flip anywhere else -- the magic, the version,
    # the count, or any entry -- changes what this checksum covers
    return body + struct.pack("<I", zlib.crc32(body))


def _encode_entry(key: bytes, kind: bytes, value: object, expiry: int) -> bytes:
    # kind is always KIND_STRING or KIND_LIST here -- Store.kind_of() raises for any
    # other value, so the else branch below never has a third case to worry about
    type_byte = TYPE_LIST if kind == KIND_LIST else TYPE_STRING
    parts = [
        struct.pack("<I", len(key)), key,
        bytes((type_byte,)),
        struct.pack("<q", expiry),
    ]
    if kind == KIND_LIST:
        elements = list(value)
        if not elements:
            # refusing here keeps a store that has somehow broken the no-empty-list
            # rule from writing a file the next start would refuse -- the save fails
            # loudly instead, and save() encodes before it creates the temporary file,
            # so nothing on disk changes
            raise ValueError("cannot snapshot an empty list at key %r" % (key,))
        parts.append(struct.pack("<I", len(elements)))
        for element in elements:
            parts.append(struct.pack("<I", len(element)))
            parts.append(element)
    else:
        parts.append(struct.pack("<I", len(value)))
        parts.append(value)
    return b"".join(parts)


def decode(blob: bytes) -> Store:
    """Refuse a corrupt or truncated blob as `SnapshotError` rather than let a bounds
    error escape: every read below goes through `struct.unpack_from` or a single-byte
    index, because a slice past the end of `bytes` raises nothing and returns short,
    which is the one read neither `struct.error` nor `IndexError` can catch.
    """
    try:
        return _decode(blob)
    except (struct.error, IndexError, ValueError) as exc:
        raise SnapshotError("corrupt snapshot: %s" % exc) from exc


def _decode(blob: bytes) -> Store:
    if len(blob) < 16:
        raise SnapshotError("snapshot too short: %d bytes" % len(blob))
    if blob[:4] != MAGIC:
        raise SnapshotError("wrong magic: %r" % (blob[:4],))
    (trailer,) = struct.unpack_from("<I", blob, len(blob) - 4)
    if zlib.crc32(blob[:-4]) != trailer:
        raise SnapshotError("checksum mismatch")
    # checked only after the checksum above already matches -- checked first, a bit
    # flip landing in these four bytes would be refused as an unsupported version
    # rather than the corruption it actually is, the same wrong-reason problem a
    # flipped length prefix would cause below
    (version,) = struct.unpack_from("<I", blob, 4)
    if version != SNAPSHOT_VERSION:
        raise SnapshotError("unsupported snapshot version: %d" % version)
    (count,) = struct.unpack_from("<I", blob, 8)
    offset = 12
    items = []
    for _ in range(count):
        (key_len,) = struct.unpack_from("<I", blob, offset)
        offset += 4
        (key,) = struct.unpack_from("<%ds" % key_len, blob, offset)
        offset += key_len
        type_byte = blob[offset]
        offset += 1
        (expiry,) = struct.unpack_from("<q", blob, offset)
        offset += 8
        if type_byte == TYPE_LIST:
            kind = KIND_LIST
            (element_count,) = struct.unpack_from("<I", blob, offset)
            offset += 4
            if element_count == 0:
                # this server's own save can never write one, and loading one hands
                # LPOP/RPOP a container they pop from unguarded
                raise SnapshotError("empty list at key %r" % (key,))
            elements = []
            for _ in range(element_count):
                (element_len,) = struct.unpack_from("<I", blob, offset)
                offset += 4
                (element,) = struct.unpack_from("<%ds" % element_len, blob, offset)
                offset += element_len
                elements.append(element)
            # a plain list, not a deque -- Store.from_items() below decides the
            # concrete container each kind is stored as; this only owes it the values
            # in order
            value = elements
        else:
            # any byte other than TYPE_LIST reads as a string entry's layout -- an
            # unrecognised byte still names itself, through Store.from_items()'s own
            # ValueError below, rather than this decoder guessing a second layout for a
            # byte it does not know
            kind = KIND_STRING if type_byte == TYPE_STRING else type_byte
            (value_len,) = struct.unpack_from("<I", blob, offset)
            offset += 4
            (value,) = struct.unpack_from("<%ds" % value_len, blob, offset)
            offset += value_len
        items.append((key, kind, value, expiry))
    # offset lands exactly on the trailer once every declared entry is consumed;
    # short of it, a count smaller than what the payload actually holds would
    # otherwise be accepted with the extra bytes silently ignored
    if offset != len(blob) - 4:
        raise SnapshotError("trailing bytes after the last entry")
    return Store.from_items(items)


def _directory_of(path: str) -> str:
    # one definition for save() and check_writable(), so the directory a startup check
    # approves is the directory the save then writes into. a bare filename lives in the
    # current directory, which os.path.dirname spells as ""
    return os.path.dirname(path) or "."


def save(store: Store, path: str) -> None:
    """Write `store` to `path` atomically: encode first, then a temporary file in
    `path`'s own directory, `flush()`, `os.fsync()`, close, `os.rename()` over `path`.
    `path` itself is never opened for writing, so a failure at any step leaves whatever
    was there byte-identical.
    """
    blob = encode(store)
    directory = _directory_of(path)
    # the temporary file has to share path's own directory: os.rename() below is
    # atomic only within one filesystem, and the platform's default temp directory
    # is not guaranteed to sit on the same one as path
    descriptor, temp_path = tempfile.mkstemp(dir=directory)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(blob)
            handle.flush()
            os.fsync(handle.fileno())
        os.rename(temp_path, path)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise


def check_writable(path: str) -> None:
    """Refuse, as `SnapshotError`, a `path` a `save()` could not complete as things
    stand: one naming no file, one where a directory already stands, and one whose
    directory is missing, is not a directory, or cannot be written. A server started
    over such a path answers every write and loses all of them, with a traceback per
    interval as the only sign, so the refusal belongs before it starts. This sees the
    directory as it is at the call, and its permissions only -- a directory removed or
    made read-only afterwards, or a disk that fills up later, is met by the save itself.
    """
    if not os.path.basename(path):
        raise SnapshotError("cannot write snapshot %r: the path names no file" % (path,))
    if os.path.isdir(path):
        raise SnapshotError("cannot write snapshot %s: a directory stands at that path" % path)
    directory = _directory_of(path)
    if not os.path.isdir(directory):
        raise SnapshotError(
            "cannot write snapshot %s: %s does not exist or is not a directory"
            % (path, directory))
    # W_OK to create the temporary file and rename it into place, X_OK to reach names
    # inside the directory at all -- os.rename() needs both, and the file's own mode
    # needs neither
    if not os.access(directory, os.W_OK | os.X_OK):
        raise SnapshotError(
            "cannot write snapshot %s: %s is not writable" % (path, directory))


def load(path: str) -> Store:
    """Read `path` and decode it. A missing path raises `FileNotFoundError`, unchanged,
    because `Server.__init__` starts empty over that answer and refuses to start over
    every other one. An unreadable path -- a directory in the snapshot's place, or any
    other `OSError` -- and a corrupt blob both leave as `SnapshotError` naming `path`,
    so the caller has one exception to catch for both.
    """
    try:
        with open(path, "rb") as handle:
            blob = handle.read()
    except FileNotFoundError:
        # FileNotFoundError is itself an OSError, so this has to be carved out ahead
        # of the broader except below, or a missing path would be wrapped into
        # SnapshotError along with every genuinely unreadable one
        raise
    except OSError as exc:
        raise SnapshotError("cannot read snapshot %s: %s" % (path, exc)) from exc
    try:
        return decode(blob)
    except SnapshotError as exc:
        raise SnapshotError("corrupt snapshot %s: %s" % (path, exc)) from exc
