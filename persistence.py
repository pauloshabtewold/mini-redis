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
before any length prefix in the blob is trusted, but not because checking it later would
let corruption through silently -- moved after the entry loop, it still refuses every
single-bit flip test_persistence_properties.py's corpus throws at it, just for the wrong
reason: a flip inside a length field meets an absurd number and is caught there, by a
bounds error rather than a checksum mismatch, before the checksum is ever read. Checking
it first names the true cause instead of a coincidental one. What actually accepts
corruption with silently wrong data, the same corpus measures directly, is having no
checksum at all.

`encode()`/`decode()` are the byte layer with no filesystem involved: a blob decodes the
same whatever it was read from, because neither function touches a path. Decoding
proceeds field by field rather than through `pickle` or `marshal`, because a snapshot is
untrusted input -- a file on disk can be corrupted or replaced, and the byte layer takes
bytes from any source -- and unpickling untrusted bytes executes arbitrary code.
`save()`/`load()` are the thin filesystem layer above them; `check_writable()` refuses a
path a `save()` could not complete as things stand, and `stale_temporaries()` finds the
files an interrupted save may have left, by the name `save()` gives its temporary file.
"""

import errno
import os
import re
import stat
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

# a save's temporary file is the snapshot's own name, a dot, tempfile's eight random
# characters and this suffix: one a killed process strands is recognisably that
# snapshot's, and it ends in the snapshot extension, so whatever already ignores
# snapshots ignores it too
_TEMPORARY_SUFFIX = ".tmp.mrdb"


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


# the width every length prefix in the format is packed at -- struct.pack("<I", ...)
# raises struct.error past this bound, naming no key and no size, which is silent
# until the next periodic save dies with a traceback the log cannot connect back to
# whichever key grew past it. reachable with --max-value-size 0, which turns that cap
# off entirely
_UINT32_MAX = 0xFFFFFFFF


def _refuse_if_too_long_for_a_uint32(key: bytes, what: str, length: int) -> None:
    if length > _UINT32_MAX:
        raise ValueError(
            "cannot snapshot key %r: %s is %d bytes, past what this format's uint32 "
            "length field holds" % (key, what, length))


def _encode_entry(key: bytes, kind: bytes, value: object, expiry: int) -> bytes:
    # kind is always KIND_STRING or KIND_LIST here -- Store.kind_of() raises for any
    # other value, so the else branch below never has a third case to worry about
    _refuse_if_too_long_for_a_uint32(key, "the key itself", len(key))
    type_byte = TYPE_LIST if kind == KIND_LIST else TYPE_STRING
    parts = [
        struct.pack("<I", len(key)), key,
        bytes((type_byte,)),
        struct.pack("<q", expiry),
    ]
    if kind == KIND_LIST:
        # checked against value's own length before list() below ever materializes it,
        # so a container whose real elements are few but whose reported length is not
        # still refuses here rather than however list() would react to that
        _refuse_if_too_long_for_a_uint32(key, "its element count", len(value))
        elements = list(value)
        if not elements:
            # refusing here keeps a store that has somehow broken the no-empty-list
            # rule from writing a file the next start would refuse -- the save fails
            # loudly instead, and save() encodes before it creates the temporary file,
            # so nothing on disk changes
            raise ValueError("cannot snapshot an empty list at key %r" % (key,))
        parts.append(struct.pack("<I", len(elements)))
        for element in elements:
            _refuse_if_too_long_for_a_uint32(key, "one of its elements", len(element))
            parts.append(struct.pack("<I", len(element)))
            parts.append(element)
    else:
        _refuse_if_too_long_for_a_uint32(key, "its value", len(value))
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
            # any byte other than TYPE_LIST is read as a string entry's layout, rather
            # than this decoder guessing a second layout for a byte it does not know --
            # but that layout being self-consistent is not guaranteed. bytes actually
            # written as a list, behind an unrecognised type byte, are read as a string
            # length instead of an element count and land the offset short of or past
            # the trailer, refused below as trailing bytes rather than through
            # Store.from_items()'s ValueError, which only fires when what follows the
            # byte happens to parse as a legal string entry
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
    try:
        return Store.from_items(items)
    except ValueError:
        # from_items() refuses two entries naming one key -- reachable only from a
        # crafted file or a peer's bytes, since our own encoder walks a keyspace whose
        # keys are unique already -- and cannot say which key it was, because what it
        # was handed may be an iterator it has consumed. this holds the entries, so it
        # can: a scan for the first repeat names it, and finding none means the refusal
        # was about something else this decoder built -- an unrecognised kind byte --
        # which decode() above turns into the same one exception either way
        seen = set()
        for key, _, _, _ in items:
            if key in seen:
                raise SnapshotError("snapshot has key %r twice" % (key,)) from None
            seen.add(key)
        raise


def _directory_of(path: str) -> str:
    # one definition for save() and check_writable(), so the directory a startup check
    # approves is the directory the save then writes into. a bare filename lives in the
    # current directory, which os.path.dirname spells as ""
    return os.path.dirname(path) or "."


def save(store: Store, path: str) -> None:
    """Write `store` to `path` atomically: encode first, then a temporary file in
    `path`'s own directory, `flush()`, `os.fsync()`, close, `os.rename()` over `path`.
    `path` itself is never opened for writing, so a failure at any step leaves whatever
    was there byte-identical. `tempfile.mkstemp()` itself runs inside the guarded region
    below, with `temp_path` set to `None` ahead of it, so an interrupt at the earliest
    possible point -- before a temporary file exists at all -- finds nothing to clean up
    rather than acting on a name this function was never given.
    """
    blob = encode(store)
    directory = _directory_of(path)
    temp_path = None
    try:
        # the temporary file has to share path's own directory: os.rename() below is
        # atomic only within one filesystem, and the platform's default temp directory
        # is not guaranteed to sit on the same one as path
        descriptor, temp_path = tempfile.mkstemp(
            dir=directory, prefix=os.path.basename(path) + ".", suffix=_TEMPORARY_SUFFIX)
        try:
            handle = os.fdopen(descriptor, "wb")
        except BaseException:
            # fdopen failed to take ownership of the descriptor, so nothing else in
            # this function will ever close it
            os.close(descriptor)
            raise
        with handle:
            handle.write(blob)
            handle.flush()
            os.fsync(handle.fileno())
        _carry_over_the_mode(path, temp_path)
        os.rename(temp_path, path)
    except BaseException as original:
        # BaseException rather than Exception: a KeyboardInterrupt or SystemExit arriving
        # mid-write unwinds through here too, and would strand the file otherwise. a kill
        # never reaches this line at all, and the file it strands is left for
        # stale_temporaries() to name at the next start
        if temp_path is not None:
            try:
                os.remove(temp_path)
            except OSError as cleanup_exc:
                # the removal failing must not be silent: it is the only thing that
                # would otherwise say a full-size copy was left on disk, and the note
                # rides along on the exception the caller already logs
                original.add_note(
                    "left the temporary file %s behind: removing it failed too: %s"
                    % (temp_path, cleanup_exc))
        raise


def _carry_over_the_mode(path: str, temp_path: str) -> None:
    # a save replaces the snapshot's directory entry rather than its contents, so the
    # file that survives is the temporary one, carrying mkstemp's own 0600 rather than
    # whatever the snapshot it replaced had. an operator who widened the old file's mode
    # -- for a backup reader, say -- would find it narrowed again by a save they did not
    # think of as touching permissions, and nothing would say so. a snapshot written for
    # the first time keeps mkstemp's 0600, which is the safe end to start from
    try:
        os.chmod(temp_path, stat.S_IMODE(os.stat(path).st_mode))
    except OSError:
        # no snapshot there yet, or its mode cannot be read: the save is not the place to
        # answer for that, and the rename below is what this function exists to precede
        pass


def check_writable(path: str) -> None:
    """Refuse, as `SnapshotError`, a `path` a `save()` could not complete as things
    stand: one naming no file, one where a directory already stands, one whose
    directory is missing or is not a directory, one whose directory will create a file
    but will not let it be removed, and an existing snapshot whose own flags would block
    the rename that finishes a save -- the one operation a trial below cannot perform
    without destroying the snapshot, so it is checked separately rather than by trying
    it. Everything else is checked by trying it: a file made in `path`'s own directory
    with the same prefix and suffix `save()`'s temporary file uses, then removed. A
    server started over a path this refuses answers every write and loses all of them,
    with a traceback per interval as the only sign, so the refusal belongs before it
    starts. This sees the directory, and the existing snapshot if there is one, as they
    are at the call -- a directory made read-only afterwards, an ACL changed afterwards,
    or a disk that fills up later, is met by the save itself, not by this.
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
    # a save's last step is a rename over path itself -- an existing snapshot's own
    # immutable or append-only flags block exactly that, and no probe file created
    # beside it below can see a flag standing on a different name
    try:
        flags = getattr(os.stat(path), "st_flags", 0)
    except OSError:
        # missing, or standing behind a directory this check cannot even see into --
        # either way there is no snapshot here for a flag to stand on, and the probe
        # below is what names the real reason if the directory itself refuses
        flags = 0
    if flags & (stat.UF_IMMUTABLE | stat.SF_IMMUTABLE | stat.UF_APPEND | stat.SF_APPEND):
        raise SnapshotError(
            "cannot write snapshot %s: the existing snapshot's flags would block a "
            "rename over it" % (path,))
    # everything left standing -- directory permissions, an ACL, a name the filesystem
    # will not accept -- is answered by attempting exactly what a save attempts: the
    # same temporary file, in the same place, removed the same way
    try:
        descriptor, probe_path = tempfile.mkstemp(
            dir=directory, prefix=os.path.basename(path) + ".", suffix=_TEMPORARY_SUFFIX)
    except OSError as exc:
        # the two errnos a directory answers when it simply will not take the file name
        # what they are, because "is not writable" is what an operator can act on, where
        # the name of a temporary file they never chose reads as an internal detail. any
        # other errno -- a name too long, bytes the filesystem will not encode -- is left
        # to say what it is, since that is a fact about the path they typed
        if exc.errno in (errno.EACCES, errno.EPERM):
            raise SnapshotError(
                "cannot write snapshot %s: %s is not writable: %s"
                % (path, directory, exc)) from exc
        raise SnapshotError("cannot write snapshot %s: %s" % (path, exc)) from exc
    try:
        os.close(descriptor)
    except OSError:
        pass
    try:
        os.remove(probe_path)
    except OSError as exc:
        # the probe file itself is stranded by the same failure that refuses the start,
        # so the refusal names it: it is empty, it is the operator's to remove, and at
        # the next start it is reported among the files an interrupted save may have
        # left, where its name alone cannot say which of the two it is
        raise SnapshotError(
            "cannot write snapshot %s: a file can be created in %s but not removed, so "
            "%s is left there: %s" % (path, directory, os.path.basename(probe_path), exc)
        ) from exc


def stale_temporaries(path: str) -> list[str]:
    """Return, sorted, the names of the regular files beside `path` that a crashed save
    -- this build's or an earlier one's -- may have left there. Two shapes are
    recognised: the name `save()` gives its temporary file now -- `path`'s own basename,
    a dot, eight random characters, then `.tmp.mrdb` -- and the bare
    `tempfile.mkstemp()` default an earlier build used before a save named its
    temporary file after the snapshot, `tmp` followed by the same eight random
    characters and nothing else, which names nothing about which snapshot it belongs
    to. A name of either shape is reported unless it is known to name something other
    than a regular file -- a directory or a link of the right name is excluded, but a
    name whose type `os.scandir()` cannot determine is still reported rather than
    dropped, since a directory listable but not searchable answers the listing and then
    refuses every further lookup inside it, and dropping the name there is silence
    exactly where the warning matters most. A name that matches only once case is
    ignored is reported only where the filesystem itself agrees the two spellings are
    one file, which is asked by re-spelling the name and comparing what each spelling
    lands on -- a start given `DUMP.MRDB` and one given `dump.mrdb` are the same
    snapshot on one filesystem and two on another, and that answer belongs to the
    filesystem rather than to a guess made here. So nothing here removes one: what it
    holds is the operator's to judge, and its name is the only evidence of where it came
    from. Empty when the directory itself cannot be listed, which is all a directory
    with no read permission can answer.
    """
    directory = _directory_of(path)
    # tempfile's random part exactly -- eight characters of lowercase letters, digits and
    # the underscore -- rather than anything between the right prefix and suffix, so the
    # shape matched is no wider than the names a save actually produces
    basename = os.path.basename(path)
    current_shape = re.compile(
        re.escape(basename + ".") + "[a-z0-9_]{8}" + re.escape(_TEMPORARY_SUFFIX))
    # the same shape once case is ignored, which on a case-insensitive filesystem is how
    # a save started under another spelling of this very path named its temporary file
    other_case = re.compile(current_shape.pattern, re.IGNORECASE)
    legacy_shape = re.compile("tmp[a-z0-9_]{8}")
    try:
        entries = list(os.scandir(directory))
    except OSError:
        return []
    stale = []
    for entry in entries:
        if not (current_shape.fullmatch(entry.name) or legacy_shape.fullmatch(entry.name)):
            if not (other_case.fullmatch(entry.name)
                    and _one_file_under_both_spellings(directory, entry.name, basename)):
                continue
        try:
            # the entry's own cached type rather than a second, separate lstat: a
            # directory that can be listed but not searched answers the listing and
            # then refuses exactly the lstat a second call would need
            is_regular = entry.is_file(follow_symlinks=False)
        except OSError:
            # the type could not be determined -- reported anyway, because a name of
            # the right shape is itself the evidence this function exists to surface
            stale.append(entry.name)
            continue
        if is_regular:
            stale.append(entry.name)
    return sorted(stale)


def _one_file_under_both_spellings(directory: str, name: str, basename: str) -> bool:
    # the name as this path would have spelled it: only the snapshot's own basename can
    # differ in case, since the eight random characters and the suffix a save appends are
    # lower case already. if the filesystem folds case, both spellings land on one file
    # and this temporary file really is this path's; if it does not, the re-spelling
    # names a file belonging to a differently-named snapshot, or nothing at all
    respelled = basename + name[len(basename):]
    if respelled == name:
        return False
    try:
        mine = os.lstat(os.path.join(directory, respelled))
        theirs = os.lstat(os.path.join(directory, name))
    except OSError:
        return False
    return (mine.st_dev, mine.st_ino) == (theirs.st_dev, theirs.st_ino)


def load(path: str) -> Store:
    """Read `path` and decode it. A missing path raises `FileNotFoundError`, unchanged,
    because `Server.__init__` starts empty over that answer and refuses to start over
    every other one. A path naming anything other than a regular file -- a named pipe,
    most of all, whose `open()` blocks until a writer appears, with nothing said and no
    way out -- is refused as `SnapshotError` before it is ever opened; a dangling
    symlink still raises `FileNotFoundError` and a symlink to a regular file still
    loads, because `os.stat()` follows one and this check reads exactly what `open()`
    would land on. An unreadable path -- a directory in the snapshot's place, or any
    other `OSError` -- and a corrupt blob both leave as `SnapshotError` naming `path`,
    so the caller has one exception to catch for both.
    """
    try:
        mode = os.stat(path).st_mode
    except FileNotFoundError:
        # FileNotFoundError is itself an OSError, so this has to be carved out ahead
        # of the broader except below, or a missing path would be wrapped into
        # SnapshotError along with every genuinely unreadable one
        raise
    except OSError as exc:
        raise SnapshotError("cannot read snapshot %s: %s" % (path, exc)) from exc
    if not stat.S_ISREG(mode):
        # caught here rather than left to open(): a named pipe's open() blocks until a
        # writer appears, with no error and no way out, so the type has to be known
        # before the call that would hang
        raise SnapshotError("cannot read snapshot %s: not a regular file" % (path,))
    try:
        with open(path, "rb") as handle:
            blob = handle.read()
    except OSError as exc:
        raise SnapshotError("cannot read snapshot %s: %s" % (path, exc)) from exc
    try:
        return decode(blob)
    except SnapshotError as exc:
        raise SnapshotError("corrupt snapshot %s: %s" % (path, exc)) from exc
