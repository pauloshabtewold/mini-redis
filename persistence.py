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

This module raises rather than logs, with one exception: a directory that cannot be synced
after the rename has already put the new snapshot at the path, so it is a warning here and
not an exception the caller would report as a failed save.
"""

import collections
import errno
import fcntl
import logging
import os
import re
import stat
import struct
import tempfile
import zlib

from store import DuplicateKeyError, KIND_LIST, KIND_STRING, Store

# the one thing logged here: a save that finished but could not make its directory entry durable
# is not a failed save, and logging.lastResort gets that WARNING to stderr with no configuration
logger = logging.getLogger(__name__)

# four ASCII bytes rather than a numeric constant, so a corrupt or truncated
# snapshot is still recognisable by eye in a hex dump
MAGIC = b"MRDB"
SNAPSHOT_VERSION = 1

# the format's own one-byte codes, not Store's KIND_STRING/KIND_LIST, which are b"string" and
# b"list": indexing bytes yields an int, so comparing this field against either is false always
TYPE_STRING = 0
TYPE_LIST = 1

# snapshot name, a dot, tempfile's eight characters, this suffix: one a killed process strands is
# recognisably that snapshot's, and it ends in the snapshot extension, so snapshot ignores cover it
_TEMPORARY_SUFFIX = ".tmp.mrdb"
# the probe's own suffix, so stale_temporaries cannot read it as an interrupted save, and exactly
# as long as a save's: the write and sync took it 0.10 ms -> 3.03 ms, warning in 90 of 150 pairs
_PROBE_SUFFIX = ".prb.mrdb"


class SnapshotError(Exception):
    """A snapshot could not be decoded or read, or its path failed the startup check.

    The one exception any caller catches: corrupt bytes, a checksum mismatch, an
    unsupported version, a path that exists but cannot be read, or a path
    `check_writable()` refuses. A `save()` that fails raises `OSError` instead, and a
    missing file raises `FileNotFoundError` -- see `load()` -- because absent and
    corrupt are different answers to the caller and `Server.__init__` acts on them
    differently.
    """


class UnwritablePath(SnapshotError):
    """`check_writable()` refused `path`: no save there could complete as things stand.

    A `SnapshotError` subclass rather than a flag, so that every caller catching
    `SnapshotError` is unaffected and the one caller that has to tell this apart from a file
    that will not load -- `server.py`, which offers `--ignore-snapshot` for the second and
    must not for the first, since such a server would start and fail at its first save -- asks
    by type. It asked by substring until the path an operator typed turned out to be able to
    contain the substring.
    """


def encode(store: Store) -> bytes:
    """Serialize every key in `store`, including a resident-but-expired one, to bytes."""
    entries = []
    count = 0
    for key, kind, value, expiry in store.snapshot_items():
        entries.append(_encode_entry(key, kind, value, expiry))
        count += 1
    body = MAGIC + struct.pack("<II", SNAPSHOT_VERSION, count) + b"".join(entries)
    # every byte before the trailer, so a flip in the magic, version, count or any entry is covered
    return body + struct.pack("<I", zlib.crc32(body))


# every length prefix's width: struct.error past it names no key and no size, so the save fails
# every interval with nothing pointing at the key -- reachable with --max-value-size 0
_UINT32_MAX = 0xFFFFFFFF


def _refuse_if_too_long_for_a_uint32(key: bytes, what: str, length: int) -> None:
    if length > _UINT32_MAX:
        raise ValueError(
            "cannot snapshot key %r: %s is %d bytes, past what this format's uint32 "
            "length field holds" % (key, what, length))


def _encode_entry(key: bytes, kind: bytes, value: object, expiry: int) -> bytes:
    # Store.kind_of() raises for anything but KIND_STRING/KIND_LIST, so the else has no third case
    _refuse_if_too_long_for_a_uint32(key, "the key itself", len(key))
    type_byte = TYPE_LIST if kind == KIND_LIST else TYPE_STRING
    parts = [
        struct.pack("<I", len(key)), key,
        bytes((type_byte,)),
        struct.pack("<q", expiry),
    ]
    if kind == KIND_LIST:
        # checked on value's own length before list() materializes it, so a container reporting
        # more elements than it has is refused here rather than wherever list() would react
        _refuse_if_too_long_for_a_uint32(key, "its element count", len(value))
        elements = list(value)
        if not elements:
            # a broken no-empty-list rule would write a file the next start refuses; save()
            # encodes before it creates the temporary file, so nothing on disk changes
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
    # after the checksum, not before: a flip in these four bytes would otherwise be refused as an
    # unsupported version rather than the corruption it is
    (version,) = struct.unpack_from("<I", blob, 4)
    if version != SNAPSHOT_VERSION:
        # the direction is named so an operator is not sent after bad hardware; "an older build" is
        # not an answer, since SNAPSHOT_VERSION has been 1 from the start and nothing wrote lower
        written_by = ("a newer build" if version > SNAPSHOT_VERSION
                      else "a build this one knows nothing about")
        raise SnapshotError(
            "unsupported snapshot version: %d, where this build writes and reads %d, so this "
            "file was written by %s rather than corrupted"
            % (version, SNAPSHOT_VERSION, written_by))
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
                # our own save never writes one, and loading one hands LPOP/RPOP an unguarded pop
                raise SnapshotError("empty list at key %r" % (key,))
            elements = []
            for _ in range(element_count):
                (element_len,) = struct.unpack_from("<I", blob, offset)
                offset += 4
                (element,) = struct.unpack_from("<%ds" % element_len, blob, offset)
                offset += element_len
                elements.append(element)
            # a plain list: Store.from_items() decides the concrete container, this owes it order
            value = elements
        else:
            # an unknown type byte is read with the string layout rather than guessed at: list
            # bytes behind one land the offset off the trailer and are refused as trailing bytes
            kind = KIND_STRING if type_byte == TYPE_STRING else type_byte
            (value_len,) = struct.unpack_from("<I", blob, offset)
            offset += 4
            (value,) = struct.unpack_from("<%ds" % value_len, blob, offset)
            offset += value_len
        items.append((key, kind, value, expiry))
    # offset must land exactly on the trailer: short of it, a count smaller than the payload
    # would be accepted with the extra bytes ignored
    if offset != len(blob) - 4:
        raise SnapshotError("trailing bytes after the last entry")
    try:
        return Store.from_items(items)
    except DuplicateKeyError:
        # caught by type, not by scanning for a repeat: a blob with both a repeat and an unknown
        # kind byte reported the repeat where from_items() raised on the byte. it names no key
        counts = collections.Counter(key for key, _, _, _ in items)
        for key, _, _, _ in items:
            if counts[key] > 1:
                # the count, not the word "twice": three entries naming one key were
                # reported as two, which is a fact about the file an operator may act on
                raise SnapshotError(
                    "snapshot has key %r %d times" % (key, counts[key])) from None
        # the scan above finds the repeat DuplicateKeyError is raised for; reaching here is a bug
        # in one of the two rather than a fact about the file
        raise


def _directory_of(path: str) -> str:
    # one definition for save() and check_writable(), so the approved directory is the written one;
    # a bare filename lives in the current directory, which os.path.dirname spells ""
    return os.path.dirname(path) or "."


def save(store: Store, path: str) -> None:
    """Write `store` to `path` atomically: encode first, then a temporary file in
    `path`'s own directory, `flush()`, a device-level sync of that file, close,
    `os.rename()` over `path`, and a sync of the directory the rename wrote into.
    `path` itself is never opened for writing, so a failure up to the rename leaves
    whatever was there byte-identical. `tempfile.mkstemp()` itself runs inside the
    guarded region below, with `temp_path` set to `None` ahead of it, so an interrupt at
    the earliest possible point -- before a temporary file exists at all -- finds nothing
    to clean up rather than acting on a name this function was never given.

    That guarded region ends at the rename, which is where the save stops being
    reversible. Everything after it is a durability step over a snapshot that is already
    at the path, so it raises nothing: the directory's sync warns instead. Run inside the
    region, it made a save that had succeeded raise, had the handler try to remove a name
    the rename had already consumed, and attached a note saying a full-size temporary
    file had been stranded -- once per interval, over saves that were landing.

    The two syncs answer two different halves of a power cut. The first makes the bytes
    durable, the second makes the name that reaches them durable: they are separate
    writes the drive may commit in either order, so a snapshot whose data survived under
    a directory entry that did not is the previous snapshot, and one whose entry survived
    over data that did not is a file the checksum then refuses. A killed process needs
    neither, because the page cache outlives it -- that is what the rename alone already
    covered.
    """
    blob = encode(store)
    directory = _directory_of(path)
    temp_path = None
    try:
        # the temporary file shares path's directory: os.rename() is atomic only within one
        # filesystem, and the platform's default temp directory need not sit on path's
        descriptor, temp_path = tempfile.mkstemp(
            dir=directory, prefix=os.path.basename(path) + ".", suffix=_TEMPORARY_SUFFIX)
        try:
            handle = os.fdopen(descriptor, "wb")
        except BaseException:
            # fdopen did not take the descriptor, so nothing else here will ever close it
            os.close(descriptor)
            raise
        with handle:
            handle.write(blob)
            handle.flush()
            _sync_to_the_device(handle.fileno())
        _carry_over_the_mode(path, temp_path)
        os.rename(temp_path, path)
    except BaseException as original:
        # BaseException, not Exception: a KeyboardInterrupt mid-write would strand the file. a kill
        # reaches no line here, and what it strands stale_temporaries() names at the next start
        if temp_path is not None:
            # a full device raises "[Errno 28] No space left on device" with filename None, and the
            # caller logs str(exc) alone; errno None is a bare message a filename would eat
            if (isinstance(original, OSError) and original.filename is None
                    and original.errno is not None):
                original.filename = temp_path
            try:
                os.remove(temp_path)
            except OSError as cleanup_exc:
                # a failed removal leaves a full-size copy on disk and only this says so,
                # on the exception the caller already logs
                original.add_note(
                    "left the temporary file %s behind: removing it failed too: %s"
                    % (temp_path, cleanup_exc))
        raise
    # outside the guard deliberately: the rename has returned, so the snapshot is at the path, no
    # temporary file is left to remove, and nothing from here on can be a failed save
    _sync_the_directory(directory)


# the errnos for a sync the filesystem does not do, as against one that failed: read as failure, a
# directory with nothing to sync would report an error every interval over a landed snapshot
_SYNC_NOT_SUPPORTED = frozenset(
    code for code in (getattr(errno, name, None)
                      for name in ("EINVAL", "ENOTSUP", "EOPNOTSUPP", "ENOTTY"))
    if code is not None
)


def _sync_to_the_device(descriptor: int) -> None:
    # on macOS os.fsync() leaves the bytes in the drive's own write cache, lost to the power cut
    # this sync exists for; F_FULLFSYNC flushes it, and is where macOS differs from elsewhere
    full_sync = getattr(fcntl, "F_FULLFSYNC", None)
    if full_sync is not None:
        try:
            fcntl.fcntl(descriptor, full_sync)
            return
        except OSError as exc:
            if exc.errno not in _SYNC_NOT_SUPPORTED:
                raise
    os.fsync(descriptor)


def _sync_the_directory(directory: str) -> None:
    # the rename's directory entry is durable only once the directory is synced: without this the
    # bytes survive a power cut under a name still pointing at the snapshot before them
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError as exc:
        _report_an_unsynced_directory(directory, exc)
        return
    try:
        _sync_to_the_device(descriptor)
    except OSError as exc:
        _report_an_unsynced_directory(directory, exc)
    finally:
        os.close(descriptor)


def _report_an_unsynced_directory(directory: str, exc: OSError) -> None:
    # a filesystem that keeps no directory to sync says so with one of these, and logging
    # that every interval would be noise about a snapshot that is already in place
    if exc.errno in _SYNC_NOT_SUPPORTED:
        return
    # the bytes are on the drive and the entry naming them may not be, so it is said; a warning and
    # not a raise, since the caller would log a landed save as "snapshot save failed"
    logger.warning(
        "the snapshot was written and renamed into place, but %s could not be synced, so "
        "the directory entry naming it may not survive a power cut: %s", directory, exc)


def _carry_over_the_mode(path: str, temp_path: str) -> None:
    # the surviving file is the temporary one, carrying mkstemp's 0600, so a mode an operator
    # widened would be narrowed back by a save with nothing said; a first snapshot keeps 0600
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
    with the same prefix `save()`'s temporary file uses and a suffix of the same length
    that nothing mistakes for one, written to, synced, then removed. A server started over
    a path this refuses answers every write and loses all of them, with one logged failure
    and then a line per hundred repeats as the only sign, so the
    refusal belongs before it starts. This sees the directory, and the existing snapshot
    if there is one, as they are at the call -- a directory made read-only afterwards, an
    ACL changed afterwards, or a device that fills up afterwards, is met by the save
    itself, not by this.

    The write is one byte, so what it answers is whether the device takes a byte, not
    whether it has room for the next snapshot, whose size this cannot know. A device
    with room for the probe and not for the keyspace therefore starts and fails at the
    save, exactly as one that filled up afterwards does. What the byte does catch is a
    device already out of room at the call, which a probe that wrote nothing did not: a
    volume with no room left accepts the directory entry and refuses the first byte, and
    before the write was added such a path started, answered `SET` with `OK` and lost the
    keyspace at the stop under a drain line reading complete.

    So is one thing present at the call: a permission that blocks the removal of the
    existing snapshot's own directory entry, which is what the rename needs and what no
    probe beside it can ask about. An ACL denying `delete` on the snapshot is the case --
    the flags above catch what a flags field can express and nothing else. Rehearsing it
    means removing that entry, or making a second name for the snapshot and removing
    that; a version of this check did the latter, and in two rounds it deleted a file it
    had not created and made concurrent starts refuse each other, to catch a configuration
    that fails safely anyway. Uncaught, such a path starts and then fails every save,
    leaving the snapshot it could not replace intact: the first failure names the
    exception and the repeats after it are counted rather than reprinted, because what
    fails here fails once an interval for as long as the server runs, and `server.py`'s
    `_guard_task` says what an unbounded stream of them costs. That is the trade: this
    check refuses what it can prove, and does not write into the operator's directory to
    guess at the rest.
    """
    if not os.path.basename(path):
        raise UnwritablePath("cannot write snapshot %r: the path names no file" % (path,))
    # lstat, not isdir: rename does not follow a symlink in its last component, so a link to a
    # directory is a path a save writes -- and the first save replaces that link with a plain file
    try:
        path_mode = os.lstat(path).st_mode
    except OSError:
        # nothing at the path, or a directory not searchable: either way no entry for the rename
        # to collide with, and the probe below names the real reason
        path_mode = None
    if path_mode is not None and stat.S_ISDIR(path_mode):
        raise UnwritablePath("cannot write snapshot %s: a directory stands at that path" % path)
    directory = _directory_of(path)
    if not os.path.isdir(directory):
        raise UnwritablePath(
            "cannot write snapshot %s: %s does not exist or is not a directory"
            % (path, directory))
    # the rename is over path itself, so the snapshot's own immutable or append-only flags block it
    # and no probe beside it sees them; lstat again, since a save never touches a link's target
    try:
        flags = getattr(os.lstat(path), "st_flags", 0)
    except OSError:
        # no snapshot here for a flag to stand on, and the probe below names the real reason
        flags = 0
    if flags & (stat.UF_IMMUTABLE | stat.SF_IMMUTABLE | stat.UF_APPEND | stat.SF_APPEND):
        raise UnwritablePath(
            "cannot write snapshot %s: the existing snapshot's flags would block a "
            "rename over it" % (path,))
    # permissions, an ACL and a name the filesystem refuses are answered by attempting what a save
    # attempts -- same write, same sync, same removal, under a suffix nothing mistakes for one
    try:
        descriptor, probe_path = tempfile.mkstemp(
            dir=directory, prefix=os.path.basename(path) + ".", suffix=_PROBE_SUFFIX)
    except OSError as exc:
        # the two errnos for a directory that will not take the file are named "not writable", what
        # an operator can act on; any other errno is a fact about the path they typed
        if exc.errno in (errno.EACCES, errno.EPERM):
            raise UnwritablePath(
                "cannot write snapshot %s: %s is not writable: %s"
                % (path, directory, exc)) from exc
        raise UnwritablePath("cannot write snapshot %s: %s" % (path, exc)) from exc
    # a device with no room takes the entry and refuses the bytes, and deferred allocation answers
    # at the sync: 1,012 KiB "available" gave ENOSPC at one byte, and a Ctrl-C strands it 5 of 80
    try:
        try:
            os.write(descriptor, b"\0")
            _sync_to_the_device(descriptor)
        except OSError as exc:
            raise UnwritablePath(
                "cannot write snapshot %s: %s took the file but not the bytes a save writes: %s"
                % (path, directory, exc)) from exc
        finally:
            # in a finally: every arm below needs it closed, the interrupt to the outer handler too
            try:
                os.close(descriptor)
            except OSError:
                pass
        try:
            os.remove(probe_path)
        except OSError as exc:
            # the same failure strands the probe, so the refusal names it: the operator's to
            # remove, and not reported at the next start, since this suffix is not a save's
            raise UnwritablePath(
                "cannot write snapshot %s: a file can be created in %s but not removed, so "
                "%s is left there: %s" % (path, directory, os.path.basename(probe_path), exc)
            ) from exc
    except BaseException:
        try:
            os.remove(probe_path)
        except OSError:
            pass
        raise


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
    # tempfile's random part exactly -- eight of [a-z0-9_] -- not anything between prefix and
    # suffix, so the shape matched is no wider than the names a save produces
    basename = os.path.basename(path)
    current_shape = re.compile(
        re.escape(basename + ".") + "[a-z0-9_]{8}" + re.escape(_TEMPORARY_SUFFIX))
    # the same shape once case is ignored, which on a case-insensitive filesystem is how
    # a save started under another spelling of this very path named its temporary file
    other_case = re.compile(current_shape.pattern, re.IGNORECASE)
    legacy_shape = _LEGACY_SHAPE
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
            # the entry's cached type, not a second lstat: a directory listable but not searchable
            # answers the listing and then refuses exactly that lstat
            is_regular = entry.is_file(follow_symlinks=False)
        except OSError:
            # the type could not be determined -- reported anyway, because a name of
            # the right shape is itself the evidence this function exists to surface
            stale.append(entry.name)
            continue
        if is_regular:
            stale.append(entry.name)
    return sorted(stale)


# mkstemp's own default name, tmp and eight characters: it ties itself to no snapshot, and anything
# that ever called mkstemp in that directory leaves the same shape, so a caller must tell them apart
_LEGACY_SHAPE = re.compile("tmp[a-z0-9_]{8}")


def is_legacy_temporary_name(name: str) -> bool:
    """True for a name of the bare `tmpXXXXXXXX` shape an earlier build of this server
    stranded. Nothing in that name ties it to any snapshot, so a caller reporting one
    must not say it belongs to the path it was found beside: `tempfile.mkstemp()` is the
    standard library's, and any program at all can leave the same name in that directory.
    """
    return bool(_LEGACY_SHAPE.fullmatch(name))


def _one_file_under_both_spellings(directory: str, name: str, basename: str) -> bool:
    # the name as this path would spell it -- basename as given, whole tail lowered, since that is
    # what a save writes -- so a filesystem that folds case lands both spellings on one file
    respelled = basename + name[len(basename):].lower()
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
    most of all, whose blocking `open()` waits for a writer with nothing said and no way
    out -- is refused as `SnapshotError`, by opening it `O_NONBLOCK` and asking the
    descriptor itself what it is: the type is read off the open file rather than off the
    path, so nothing can replace the path between the question and the read. A dangling
    symlink still raises `FileNotFoundError` and a symlink to a regular file still
    loads, because `os.open()` follows one. An unreadable path -- a directory in the
    snapshot's place, or any other `OSError` -- and a corrupt blob both leave as
    `SnapshotError` naming `path`, so the caller has one exception to catch for both.
    """
    try:
        # O_NONBLOCK so a fifo returns at once instead of waiting for a writer with no way out; the
        # type is read off this descriptor, since stat-then-open lost the race 202 times in 7 ms
        descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    except FileNotFoundError:
        # FileNotFoundError is an OSError, so it is carved out ahead of the arm below or a missing
        # path leaves as SnapshotError along with every unreadable one
        raise
    except OSError as exc:
        raise SnapshotError("cannot read snapshot %s: %s" % (path, exc)) from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise SnapshotError("cannot read snapshot %s: not a regular file" % (path,))
        # closefd=False: the finally owns the descriptor, and a second close hits a number some
        # later open() may already have been handed
        with open(descriptor, "rb", closefd=False) as handle:
            blob = handle.read()
    except OSError as exc:
        raise SnapshotError("cannot read snapshot %s: %s" % (path, exc)) from exc
    finally:
        os.close(descriptor)
    try:
        return decode(blob)
    except SnapshotError as exc:
        # a version this build does not know is not corruption and is not labelled as it: the
        # checksum passed, so the bytes are exactly what some build wrote
        if str(exc).startswith("unsupported snapshot version"):
            raise SnapshotError("cannot read snapshot %s: %s" % (path, exc)) from exc
        raise SnapshotError("corrupt snapshot %s: %s" % (path, exc)) from exc
