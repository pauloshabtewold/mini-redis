"""The snapshot format's contract over chosen inputs: a round trip preserving keys,
both kinds and an exact absolute deadline, the version field at its own offset, a save
observed writing through a temporary file, and every refusal this module names -- an
unsupported version, a blob under the sixteen-byte header, a wrong magic, a trailing
byte past the trailer, truncation at every offset, an unrecognised kind byte, a list
entry whose element count is zero, an empty list refused at encode with the save
leaving nothing on disk, a rename that fails once, an interrupt mid-save leaving no
temporary file, the temporary file's name and the report that finds that name and no
other, the two paths `load()` tells
apart, a missing one and a corrupt one, every shape of path the first save could not
write refused by `check_writable()`, entries that stop short of the trailer with a
correct checksum, a checksummed blob whose layout runs past its own end -- once through
each of the two bounds errors the decoder translates -- and the two type bytes pinned
against their literal values.
test_persistence_properties.py covers the refusal contract over inputs this module does
not choose.
"""

import errno
import faulthandler
import fcntl
import logging
import os
import subprocess
import getpass
import pathlib
import stat
import struct
import threading
import tempfile
import zlib
from collections import deque

import pytest

import commands
import persistence
from store import KIND_LIST, Store
from tests.conftest import FROZEN, FrozenStore


# normal


def test_a_round_trip_preserves_keys_values_and_kinds():
    store = Store()
    store.write(b"s", b"v", keep_ttl=False)
    store.write(b"l", deque([b"a", b"b"]), keep_ttl=False)
    loaded = persistence.decode(persistence.encode(store))
    assert sorted(loaded._data) == sorted(store._data)
    assert loaded.lookup(b"s") == b"v"
    assert list(loaded.lookup(b"l")) == [b"a", b"b"]


def test_a_round_trip_preserves_an_absolute_deadline_exactly():
    store = FrozenStore()
    store.write(b"t", b"v", keep_ttl=False)
    store.expire_at(b"t", FROZEN + 100_000)
    loaded = persistence.decode(persistence.encode(store))
    assert loaded.deadline(b"t") == FROZEN + 100_000


def test_a_loaded_list_value_is_a_deque_that_answers_its_kind():
    store = Store()
    store.write(b"l", deque([b"a"]), keep_ttl=False)
    loaded = persistence.decode(persistence.encode(store))
    assert type(loaded._data[b"l"]) is deque
    assert loaded.kind_of(loaded._data[b"l"]) == KIND_LIST


def test_a_list_command_dispatched_against_a_loaded_store_answers():
    store = Store()
    store.write(b"l", deque([b"a", b"", b"\xff"]), keep_ttl=False)
    loaded = persistence.decode(persistence.encode(store))
    # a value that decoded back as a Python list rather than a deque raises TypeError
    # out of kind_of(), which dispatch() does not catch -- so the connection closes
    # instead of answering. dispatching a real command here checks that outcome
    # directly, rather than only inspecting the loaded value's type
    response, effects = commands.dispatch(loaded, None, [b"LRANGE", b"l", b"0", b"-1"])
    assert response == b"*3\r\n$1\r\na\r\n$0\r\n\r\n$1\r\n\xff\r\n"
    assert effects == []


def test_an_empty_store_round_trips():
    loaded = persistence.decode(persistence.encode(Store()))
    assert loaded._data == {}


def test_binary_keys_and_values_round_trip():
    store = Store()
    store.write(b"bin", b"\x00\xff\r\n", keep_ttl=False)
    store.write(b"\x00k\xff", b"v", keep_ttl=False)
    loaded = persistence.decode(persistence.encode(store))
    assert loaded._data[b"bin"] == b"\x00\xff\r\n"
    assert loaded._data[b"\x00k\xff"] == b"v"


# edge


def test_the_version_field_sits_at_its_own_offset():
    blob = persistence.encode(Store())
    (version,) = struct.unpack_from("<I", blob, 4)
    assert version == persistence.SNAPSHOT_VERSION


def test_save_writes_through_a_temporary_file_and_renames_it(tmp_path, monkeypatch):
    path = tmp_path / "dump.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)

    # one shared list rather than two separate ones, so the order fsync and rename
    # actually ran in is read back from the list itself instead of assumed from the
    # order the code below happens to call them in
    events = []
    synced = []
    renamed = []
    real_fsync, real_rename, real_fcntl = os.fsync, os.rename, fcntl.fcntl

    def recording_fsync(fd):
        # the size on disk at the moment the sync is called, before real_fsync runs --
        # bytes still sitting in handle's own userspace buffer have not reached the
        # file this descriptor names yet, so a deleted flush() shows up here as a short
        # size rather than only in a size read back after save() has already returned.
        # directories are recorded separately below: the one that matters here is the
        # sync of the snapshot's own bytes
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            events.append(("sync-directory", None))
            return real_fsync(fd)
        events.append(("fsync", os.fstat(fd).st_size))
        synced.append(fd)
        return real_fsync(fd)

    def recording_full_sync(fd, command, *rest):
        # where the platform has it, the file's own sync is a device-level flush rather
        # than os.fsync, so both have to be watched or this test reads as "never synced"
        # on one platform and passes on the other
        if command == getattr(fcntl, "F_FULLFSYNC", object()):
            return recording_fsync(fd)
        return real_fcntl(fd, command, *rest)

    def recording_rename(a, b):
        events.append(("rename", None))
        renamed.append((a, b))
        return real_rename(a, b)

    monkeypatch.setattr(os, "fsync", recording_fsync)
    monkeypatch.setattr(fcntl, "fcntl", recording_full_sync)
    monkeypatch.setattr(os, "rename", recording_rename)

    persistence.save(store, str(path))

    assert synced, "the snapshot's bytes were never synced, so they are not durable"
    assert len(renamed) == 1
    src, dst = renamed[0]
    assert str(dst) == str(path)
    assert str(src) != str(path), (
        "the snapshot was renamed onto itself, so the real file was written in place"
    )
    assert pathlib.Path(src).parent == path.parent, (
        "the temporary file must share the directory, or the rename crosses devices"
    )

    assert len(synced) == 1 and len(renamed) == 1, (
        "save must sync the file exactly once and rename exactly once: %r" % (events,)
    )
    # the order first, so a rename that ran ahead of the fsync is reported as that rather
    # than as a size mismatch read off the wrong event
    assert [kind for kind, _ in events] == ["fsync", "rename", "sync-directory"], (
        "the file's sync must come before the rename, or a power loss right after the "
        "rename can leave the real path naming a file whose bytes never reached the "
        "disk, and the directory's sync must come after it, or the entry that names "
        "those bytes is itself what the power cut loses: %r" % (events,)
    )
    (fsync_size,) = [size for kind, size in events if kind == "fsync"]
    assert fsync_size == len(persistence.encode(store)), (
        "the bytes were still in a userspace buffer when the fsync ran", fsync_size
    )


def test_the_file_sync_falls_back_when_the_device_flush_is_not_supported(tmp_path, monkeypatch):
    # F_FULLFSYNC exists on the platform but the filesystem under this path refuses it,
    # which some do -- a network mount, a disk image. the save has to fall back to the
    # sync every platform has rather than fail, and the file still has to be synced:
    # treating "not supported" as a failed save would lose every snapshot on those mounts
    if not hasattr(fcntl, "F_FULLFSYNC"):
        pytest.skip("this platform has no device-level flush to fall back from")
    plain_syncs = []
    real_fsync = os.fsync

    def refusing_full_sync(fd, command, *rest):
        raise OSError(errno.ENOTSUP, "Operation not supported")

    def counting_fsync(fd):
        plain_syncs.append(fd)
        return real_fsync(fd)

    monkeypatch.setattr(fcntl, "fcntl", refusing_full_sync)
    monkeypatch.setattr(os, "fsync", counting_fsync)
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    path = tmp_path / "dump.mrdb"
    persistence.save(store, str(path))
    assert plain_syncs, "the save gave up instead of falling back to os.fsync"
    assert persistence.load(str(path)).lookup(b"k") == b"v"


def test_a_device_flush_that_fails_for_any_other_reason_fails_the_save(tmp_path, monkeypatch):
    # only "not supported" is tolerated: an I/O error from the flush is the drive saying
    # the bytes are not on it, which is the one thing this call exists to find out, and a
    # save that swallowed it would report success over a snapshot that may not be there
    if not hasattr(fcntl, "F_FULLFSYNC"):
        pytest.skip("this platform has no device-level flush to fail")

    def failing_full_sync(fd, command, *rest):
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(fcntl, "fcntl", failing_full_sync)
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    with pytest.raises(OSError) as refused:
        persistence.save(store, str(tmp_path / "dump.mrdb"))
    assert refused.value.errno == errno.EIO
    assert list(tmp_path.iterdir()) == [], "a failed save left its temporary file behind"


def test_a_directory_that_cannot_be_synced_does_not_fail_a_save_already_on_disk(
    tmp_path, monkeypatch
):
    # the directory sync runs after the rename, so by the time it can fail the snapshot
    # is already in place and the old one is already gone. a filesystem that does not
    # sync directories at all answers this way, and reporting it as a failed save would
    # log an error every interval about a save that worked
    real_fsync, real_fcntl = os.fsync, fcntl.fcntl

    def refuse_for_directories(fd, *rest):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(errno.EINVAL, "Invalid argument")
        return real_fsync(fd) if not rest else real_fcntl(fd, *rest)

    monkeypatch.setattr(os, "fsync", refuse_for_directories)
    monkeypatch.setattr(fcntl, "fcntl", refuse_for_directories)
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    path = tmp_path / "dump.mrdb"
    persistence.save(store, str(path))
    assert persistence.load(str(path)).lookup(b"k") == b"v"


# error


def test_an_unsupported_version_is_refused():
    blob = bytearray(persistence.encode(Store()))
    blob[4:8] = struct.pack("<I", persistence.SNAPSHOT_VERSION + 1)
    # recomputed to match the flipped version -- left as it was, the checksum check
    # ahead of the version check in decode order would refuse this blob first, for a
    # mismatch rather than for its version
    blob[-4:] = struct.pack("<I", zlib.crc32(bytes(blob[:-4])))
    with pytest.raises(persistence.SnapshotError):
        persistence.decode(bytes(blob))


def test_a_blob_shorter_than_the_header_is_refused():
    # matched on the message and not only on the type: without the length floor this
    # twelve-byte blob is still refused, for a checksum that happens not to match, and
    # the length check that must run first goes missing in silence -- match= is what
    # actually proves it ran, rather than some other check refusing this blob for the
    # wrong reason and the test passing anyway
    with pytest.raises(persistence.SnapshotError, match="too short"):
        persistence.decode(b"MRDB" + b"\x00" * 8)


def test_a_wrong_magic_is_refused():
    blob = persistence.encode(Store())
    with pytest.raises(persistence.SnapshotError):
        persistence.decode(b"XRDB" + blob[4:])


def test_entries_that_end_before_the_trailer_are_refused():
    # a blob whose checksum is correct and whose declared count stops short of the bytes
    # that follow it. unreachable from this encoder -- any appended byte moves the CRC --
    # and reachable exactly where this module's docstring says the decoder is aimed:
    # a peer that chooses both the count and the bytes behind it. ignored rather than
    # refused, a follower silently drops every entry past the count and diverges from
    # its leader on a key neither end will mention again
    store = Store()
    store.write(b"a", b"v", keep_ttl=False)
    store.write(b"b", b"w", keep_ttl=False)
    body = bytearray(persistence.encode(store)[:-4])
    body[8:12] = struct.pack("<I", 1)
    spliced = bytes(body) + struct.pack("<I", zlib.crc32(bytes(body)))
    # the control: the trailer is recomputed and right, so what follows is refused for
    # its length and not for its checksum
    (trailer,) = struct.unpack_from("<I", spliced, len(spliced) - 4)
    assert zlib.crc32(spliced[:-4]) == trailer
    with pytest.raises(persistence.SnapshotError, match="trailing bytes"):
        persistence.decode(spliced)


def test_the_two_type_bytes_are_the_literal_values_this_format_version_fixes():
    # against literals, never against the constants' own names: encode and decode are
    # symmetric, so swapping the two round-trips perfectly in process and is invisible to
    # every other assertion in this module -- while every snapshot an earlier build wrote,
    # and every peer that speaks these bytes, still reads 0 as a string and 1 as a list
    assert (persistence.TYPE_STRING, persistence.TYPE_LIST) == (0, 1), (
        "the on-wire type codes moved without SNAPSHOT_VERSION moving with them",
        persistence.TYPE_STRING, persistence.TYPE_LIST)
    for value, expected in ((b"v", 0), (deque([b"a"]), 1)):
        store = Store()
        store.write(b"k", value, keep_ttl=False)
        blob = persistence.encode(store)
        # magic, version and key count are four bytes each, then a four-byte key length
        # and the key itself, and the type byte is next
        type_at = 4 + 4 + 4 + 4 + len(b"k")
        assert blob[type_at] == expected, (
            "the byte on the wire is not the literal this format version fixes",
            type(value).__name__, blob[type_at], expected)


def test_a_trailing_byte_past_the_trailer_is_refused():
    blob = persistence.encode(Store())
    with pytest.raises(persistence.SnapshotError):
        persistence.decode(blob + b"\x00")


def test_truncation_at_every_offset_is_refused_as_snapshot_error():
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    store.write(b"l", deque([b"a"]), keep_ttl=False)
    store.expire_at(b"k", store.now_ms() + 100_000)
    blob = persistence.encode(store)
    for cut in range(len(blob)):
        with pytest.raises(persistence.SnapshotError):
            persistence.decode(blob[:cut])


def test_no_refusal_anywhere_leaks_a_struct_error():
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    store.write(b"l", deque([b"a"]), keep_ttl=False)
    store.expire_at(b"k", store.now_ms() + 100_000)
    blob = persistence.encode(store)

    for cut in range(len(blob)):
        try:
            persistence.decode(blob[:cut])
        except persistence.SnapshotError:
            pass
        except (struct.error, IndexError) as exc:
            pytest.fail("truncation at %d leaked %s: %s" % (cut, type(exc).__name__, exc))

    for bad in (b"MRDB" + b"\x00" * 8, b"XRDB" + blob[4:], blob + b"\x00"):
        try:
            persistence.decode(bad)
        except persistence.SnapshotError:
            pass
        except Exception as exc:
            pytest.fail("%r leaked %s instead of SnapshotError" % (bad[:8], type(exc).__name__))


def test_a_checksummed_blob_that_runs_past_its_own_end_is_refused_through_each_bounds_error():
    # the checksum is compared first, so no truncation and no flipped bit ever reaches the
    # parser's bounds errors -- only a blob whose trailer agrees with a layout that runs off
    # the end does, which is what a peer choosing both the bytes and their checksum can
    # send. one blob per exception the decoder has to translate: a key whose length
    # swallows the trailer, so the type byte is read past the end, and a count one larger
    # than the entries present, so the next key length is read past the end. the cause is
    # asserted as well as the type, or either blob could be refused by some other check
    # and prove nothing about the arm it is here for
    def checksummed(body):
        return body + struct.pack("<I", zlib.crc32(body))

    version = persistence.SNAPSHOT_VERSION
    key_over_the_trailer = checksummed(persistence.MAGIC + struct.pack("<III", version, 1, 4))
    one_entry = (struct.pack("<I", 1) + b"k" + bytes((persistence.TYPE_STRING,))
                 + struct.pack("<qI", -1, 1) + b"v")
    count_past_the_entries = checksummed(
        persistence.MAGIC + struct.pack("<II", version, 2) + one_entry)

    for blob, cause in ((key_over_the_trailer, IndexError), (count_past_the_entries, struct.error)):
        with pytest.raises(persistence.SnapshotError) as refused:
            persistence.decode(blob)
        assert isinstance(refused.value.__cause__, cause), (
            "refused, but not through the %s the blob was built to reach" % cause.__name__,
            refused.value.__cause__)


def test_an_unrecognised_kind_byte_is_refused_as_snapshot_error():
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    blob = bytearray(persistence.encode(store))
    # magic, version and key count are four bytes each, then a four-byte key length and
    # the key itself, and the type byte is next
    type_at = 4 + 4 + 4 + 4 + len(b"k")
    # int against int: indexing bytes yields an int, so what stands here is the format's
    # own one-byte code and never Store's six-byte KIND_STRING
    assert blob[type_at] == persistence.TYPE_STRING, (
        "the type byte is not at the offset this format fixes"
    )
    unknown = 0x7F
    assert unknown not in (persistence.TYPE_STRING, persistence.TYPE_LIST)
    blob[type_at] = unknown
    blob[-4:] = struct.pack("<I", zlib.crc32(bytes(blob[:-4])))
    with pytest.raises(persistence.SnapshotError):
        persistence.decode(bytes(blob))


def test_a_list_with_no_elements_is_refused_as_snapshot_error():
    store = Store()
    store.write(b"l", deque([b"a"]), keep_ttl=False)
    blob = persistence.encode(store)
    loaded = persistence.decode(blob)
    assert list(loaded._data[b"l"]) == [b"a"], (
        "the control blob does not hold the one element the byte surgery below targets"
    )

    # magic, version and key count are four bytes each, then a four-byte key length and
    # the key itself, one type byte and an eight-byte expiry, and the list's own element
    # count is next
    count_at = 4 + 4 + 4 + 4 + len(b"l") + 1 + 8
    body = bytearray(blob[:-4])
    # the element's own length prefix and bytes sit right after the count -- stripped
    # out along with it so a decoder with no check for this reads the shortened body as
    # a complete, trailing-byte-free zero-element list rather than stumbling onto the
    # refusal by accident
    element_at = count_at + 4
    element_end = element_at + 4 + len(b"a")
    del body[element_at:element_end]
    struct.pack_into("<I", body, count_at, 0)
    spliced = bytes(body) + struct.pack("<I", zlib.crc32(bytes(body)))

    with pytest.raises(persistence.SnapshotError, match="empty list"):
        persistence.decode(spliced)


def test_a_store_holding_an_empty_list_is_refused_at_encode_and_the_save_writes_nothing(
    tmp_path
):
    store = Store()
    store.write(b"l", deque(), keep_ttl=False)
    with pytest.raises(ValueError, match="empty list"):
        persistence.encode(store)
    with pytest.raises(ValueError, match="empty list"):
        persistence.save(store, str(tmp_path / "dump.mrdb"))
    assert not list(tmp_path.iterdir()), (
        "save left something behind in tmp_path after refusing to encode"
    )


def _make_rename_failing_once(real_rename, calls):
    # fails only the first call, so a save can be interrupted once and then succeed
    def rename_failing_once(src, dst):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("the rename was refused")
        return real_rename(src, dst)

    return rename_failing_once


def test_a_rename_that_fails_leaves_the_previous_snapshot_byte_identical_and_loadable(
    tmp_path, monkeypatch
):
    path = tmp_path / "dump.mrdb"
    good = Store()
    good.write(b"first", b"value", keep_ttl=False)
    persistence.save(good, str(path))
    before = path.read_bytes()

    later = Store()
    later.write(b"second", b"value", keep_ttl=False)
    real_rename = os.rename
    calls = {"n": 0}
    monkeypatch.setattr(os, "rename", _make_rename_failing_once(real_rename, calls))
    with pytest.raises(OSError):
        persistence.save(later, str(path))
    assert path.read_bytes() == before, "the previous snapshot was modified"
    assert sorted(persistence.load(str(path))._data) == [b"first"], (
        "the previous snapshot no longer loads"
    )
    left = [p.name for p in tmp_path.iterdir() if p.name != "dump.mrdb"]
    assert not left, "a temporary file was left behind: %s" % left


def test_a_save_after_the_injection_is_undone_replaces_the_file(tmp_path):
    path = tmp_path / "dump.mrdb"
    good = Store()
    good.write(b"first", b"value", keep_ttl=False)
    persistence.save(good, str(path))

    later = Store()
    later.write(b"second", b"value", keep_ttl=False)
    real_rename = os.rename
    calls = {"n": 0}

    # assigned and restored by hand so the point the real os.rename comes back is
    # explicit in the test body -- after the failing save and before the save that
    # must succeed, which is the ordering this test exists to check
    os.rename = _make_rename_failing_once(real_rename, calls)
    try:
        with pytest.raises(OSError):
            persistence.save(later, str(path))
    finally:
        os.rename = real_rename

    persistence.save(later, str(path))
    assert sorted(persistence.load(str(path))._data) == [b"second"], (
        "the save that follows the undone injection did not replace the file"
    )


def test_an_interrupt_mid_save_leaves_no_temporary_file(tmp_path, monkeypatch):
    # a KeyboardInterrupt is a BaseException and no Exception, so a cleanup that caught
    # only Exception let it unwind past the removal and strand a file the size of the
    # snapshot beside it
    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt

    # both, since which one syncs the file is the platform's choice
    monkeypatch.setattr(os, "fsync", interrupted)
    monkeypatch.setattr(fcntl, "fcntl", interrupted)
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    with pytest.raises(KeyboardInterrupt):
        persistence.save(store, str(tmp_path / "dump.mrdb"))
    assert list(tmp_path.iterdir()) == [], sorted(p.name for p in tmp_path.iterdir())


def test_a_save_names_its_temporary_file_after_the_snapshot_and_only_that_name_is_reported(
    tmp_path, monkeypatch
):
    path = tmp_path / "dump.mrdb"
    renamed = []
    real_rename = os.rename

    def recording_rename(src, dst):
        renamed.append(pathlib.Path(src).name)
        return real_rename(src, dst)

    monkeypatch.setattr(os, "rename", recording_rename)
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(path))
    monkeypatch.undo()
    (temporary,) = renamed
    # ending in the snapshot extension is what lets whatever ignores snapshots ignore a
    # stranded one too
    assert temporary.startswith("dump.mrdb.") and temporary.endswith(".mrdb"), temporary

    # the very name a real save used, left behind the way a kill leaves it, is what
    # stale_temporaries() finds -- so if tempfile's own naming ever moves, this is where the
    # two stop agreeing. beside it, names no save to this path makes -- a random part one
    # character short and one character long among them -- and a directory and a link
    # with the right name, none of which it may report
    stranded = tmp_path / temporary
    stranded.write_bytes(b"left by a killed save")
    near_misses = [
        "dump.mrdb.tmp.mrdb",
        "dump.mrdb.abcd1234.tmp",
        "dump.mrdb.abcd12345.tmp.mrdb",
        "dump.mrdb.abcd123.tmp.mrdb",
        "other.mrdb.abcd1234.tmp.mrdb",
        "dump.mrdb.ab-d1234.tmp.mrdb",
    ]
    for name in near_misses:
        (tmp_path / name).write_bytes(b"not a temporary file a save made")
    (tmp_path / "dump.mrdb.dir12345.tmp.mrdb").mkdir()
    (tmp_path / "link-target").write_bytes(b"a file some link of the right name points at")
    (tmp_path / "dump.mrdb.link1234.tmp.mrdb").symlink_to(tmp_path / "link-target")
    assert persistence.stale_temporaries(str(path)) == [temporary]
    # and finding them touches nothing: every entry is still there, the snapshot included
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(
        ["dump.mrdb", temporary, "dump.mrdb.dir12345.tmp.mrdb", "link-target",
         "dump.mrdb.link1234.tmp.mrdb"] + near_misses)
    assert stranded.read_bytes() == b"left by a killed save"
    assert sorted(persistence.load(str(path))._data) == [b"k"]


def test_load_on_a_missing_path_raises_file_not_found_rather_than_snapshot_error(tmp_path):
    missing = tmp_path / "absent.mrdb"
    with pytest.raises(FileNotFoundError):
        persistence.load(str(missing))


def test_load_on_an_unreadable_path_raises_snapshot_error(tmp_path):
    directory = tmp_path / "adirectory.mrdb"
    directory.mkdir()
    with pytest.raises(persistence.SnapshotError):
        persistence.load(str(directory))


def test_load_refuses_a_named_pipe_by_type_without_ever_blocking_on_open(tmp_path):
    # a fifo's open() blocks until a writer shows up -- with no writer coming, the old
    # load() would hang here forever. the bound below is insurance in case a future
    # change reopens that hang: it kills the process rather than letting the suite hang
    # with it, since timeout(1) is not available here
    fifo_path = tmp_path / "dump.mrdb"
    os.mkfifo(fifo_path)
    faulthandler.dump_traceback_later(5, exit=True)
    try:
        with pytest.raises(persistence.SnapshotError, match="not a regular file"):
            persistence.load(str(fifo_path))
    finally:
        faulthandler.cancel_dump_traceback_later()


def test_load_on_a_dangling_symlink_still_raises_file_not_found(tmp_path):
    link = tmp_path / "dump.mrdb"
    link.symlink_to(tmp_path / "gone.mrdb")
    with pytest.raises(FileNotFoundError):
        persistence.load(str(link))


def test_load_through_a_symlink_to_a_good_snapshot_still_loads(tmp_path):
    real_path = tmp_path / "real.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(real_path))
    link = tmp_path / "dump.mrdb"
    link.symlink_to(real_path)
    loaded = persistence.load(str(link))
    assert loaded._data[b"k"] == b"v"


def test_check_writable_refuses_every_path_the_first_save_could_not_complete(
    tmp_path, monkeypatch
):
    (tmp_path / "in-the-way.mrdb").mkdir()
    a_file = tmp_path / "a-file"
    a_file.write_bytes(b"")
    refused = [
        ("", "names no file"),
        (str(tmp_path) + os.sep, "names no file"),
        (str(tmp_path / "in-the-way.mrdb"), "a directory stands at that path"),
        (str(tmp_path / "missing" / "dump.mrdb"), "does not exist or is not a directory"),
        (str(a_file / "dump.mrdb"), "does not exist or is not a directory"),
        # tried for real rather than measured -- a save's own temporary file, eighteen
        # bytes longer than this basename, is what this filesystem actually refuses
        (str(tmp_path / ("s" * 245 + ".mrdb")), "cannot write snapshot"),
    ]
    for path, reason in refused:
        with pytest.raises(persistence.SnapshotError, match=reason):
            persistence.check_writable(path)

    # the control: a writable directory, a bare filename -- whose directory is the
    # current one, spelled "" by os.path.dirname, confirmed here from inside tmp_path
    # rather than wherever the suite happens to run from -- and a snapshot already
    # there are all accepted, and the check itself leaves nothing behind
    before = sorted(p.name for p in tmp_path.iterdir())
    persistence.check_writable(str(tmp_path / "dump.mrdb"))
    monkeypatch.chdir(tmp_path)
    persistence.check_writable("dump.mrdb")
    persistence.check_writable(str(tmp_path / ("s" * 200 + ".mrdb")))
    good = Store()
    good.write(b"k", b"v", keep_ttl=False)
    persistence.save(good, str(tmp_path / "saved.mrdb"))
    persistence.check_writable(str(tmp_path / "saved.mrdb"))
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(before + ["saved.mrdb"])


def test_check_writable_needs_both_write_and_search_permission_on_the_directory(tmp_path):
    # 0600 is writable but not searchable, 0555 is searchable but not writable -- a
    # directory tried at only 0500 (searchable, not writable) or only 0466-style modes
    # cannot tell the two permissions apart, since either alone already refuses. both
    # are needed for os.rename() to land the temporary file at path, and check_writable()
    # now finds that out by attempting exactly what save() attempts
    writable_not_searchable = tmp_path / "writable-not-searchable"
    writable_not_searchable.mkdir()
    writable_not_searchable.chmod(0o600)
    searchable_not_writable = tmp_path / "searchable-not-writable"
    searchable_not_writable.mkdir()
    searchable_not_writable.chmod(0o555)
    try:
        with pytest.raises(persistence.SnapshotError):
            persistence.check_writable(str(writable_not_searchable / "dump.mrdb"))
        with pytest.raises(persistence.SnapshotError):
            persistence.check_writable(str(searchable_not_writable / "dump.mrdb"))
    finally:
        writable_not_searchable.chmod(0o700)
        searchable_not_writable.chmod(0o700)


def test_check_writable_refuses_a_directory_that_takes_a_file_but_will_not_let_it_be_removed(
    tmp_path
):
    if not hasattr(os, "chflags"):
        pytest.skip("no os.chflags on this platform to build an append-only directory")
    append_only = tmp_path / "append-only"
    append_only.mkdir()
    try:
        os.chflags(str(append_only), stat.UF_APPEND)
    except OSError:
        pytest.skip("this filesystem does not honor UF_APPEND on a directory")
    try:
        with pytest.raises(persistence.SnapshotError, match="cannot be removed|not removed"):
            persistence.check_writable(str(append_only / "dump.mrdb"))
    finally:
        os.chflags(str(append_only), 0)


def test_check_writable_refuses_an_existing_snapshot_whose_flags_would_block_the_rename(
    tmp_path
):
    if not hasattr(os, "chflags") or not hasattr(stat, "UF_IMMUTABLE"):
        pytest.skip("no chflags/UF_IMMUTABLE on this platform")
    path = tmp_path / "dump.mrdb"
    good = Store()
    good.write(b"k", b"v", keep_ttl=False)
    persistence.save(good, str(path))
    try:
        os.chflags(str(path), stat.UF_IMMUTABLE)
    except OSError:
        pytest.skip("this filesystem does not honor UF_IMMUTABLE")
    try:
        with pytest.raises(persistence.SnapshotError):
            persistence.check_writable(str(path))
    finally:
        os.chflags(str(path), 0)


def test_check_writable_refuses_a_basename_whose_temporary_name_the_filesystem_will_not_accept(
    tmp_path
):
    long_name = "s" * 245 + ".mrdb"
    with pytest.raises(persistence.SnapshotError):
        persistence.check_writable(str(tmp_path / long_name))


def test_check_writable_accepts_the_longest_accented_basename_this_filesystem_takes(tmp_path):
    # the limit belongs to the filesystem, and the two the suite runs on disagree about
    # what it counts: one counts characters, where two hundred accented ones fit, and one
    # counts utf-8 bytes, where the same name is twice too long. so the name is found by
    # asking -- the longest one whose temporary file this filesystem actually creates --
    # and the check is then required to accept exactly that, which is what a byte count
    # against PC_NAME_MAX got wrong wherever the two differ
    accepted = None
    # upward to the first name this filesystem will not take, so what is tested is its
    # own ceiling rather than a number this test brought with it
    for characters in range(1, 401):
        candidate = tmp_path / ("é" * characters + ".mrdb")
        probe = tmp_path / (candidate.name + ".abcdefgh.tmp.mrdb")
        try:
            probe.touch()
        except OSError:
            break
        probe.unlink()
        accepted = candidate
    assert accepted is not None, "no accented basename at all could be written here"
    persistence.check_writable(str(accepted))
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(accepted))
    loaded = persistence.load(str(accepted))
    assert loaded.lookup(b"k") == b"v"
    # and the other side of the same boundary: one character more than this filesystem
    # takes is refused by the check rather than met by a save that fails every interval
    too_long = tmp_path / ("é" * (len(accepted.name) - len(".mrdb") + 1) + ".mrdb")
    with pytest.raises(persistence.SnapshotError):
        persistence.check_writable(str(too_long))


def test_a_failed_removal_after_a_failed_rename_names_the_stranded_temporary_file_in_the_propagating_exceptions_notes(
    tmp_path, monkeypatch
):
    path = tmp_path / "dump.mrdb"
    good = Store()
    good.write(b"first", b"value", keep_ttl=False)
    persistence.save(good, str(path))
    before = path.read_bytes()

    later = Store()
    later.write(b"second", b"value", keep_ttl=False)

    def failing_rename(a, b):
        raise OSError("the rename was refused")

    def failing_remove(p):
        raise OSError("the removal was refused too")

    monkeypatch.setattr(os, "rename", failing_rename)
    monkeypatch.setattr(os, "remove", failing_remove)
    with pytest.raises(OSError) as failed:
        persistence.save(later, str(path))
    notes = getattr(failed.value, "__notes__", [])
    assert notes, "the rename-then-remove failure left no note on the propagating exception"
    assert any(".tmp.mrdb" in note for note in notes), (
        "no note names the stranded temporary file: %r" % (notes,)
    )
    assert path.read_bytes() == before, "the previous snapshot was modified"
    leftover = [p.name for p in tmp_path.iterdir() if p.name != "dump.mrdb"]
    assert len(leftover) == 1 and leftover[0].endswith(".tmp.mrdb"), (
        "the stranded temporary file itself is missing: %r" % (leftover,)
    )


def test_the_temporary_file_is_created_inside_the_guarded_region_so_an_interrupt_as_mkstemp_returns_leaves_nothing_behind(
    tmp_path, monkeypatch
):
    # tempfile.mkstemp() itself now runs inside save()'s guarded try, with temp_path
    # pre-set to None ahead of it -- so an interrupt at the earliest possible point,
    # before mkstemp hands back a path at all, finds nothing to clean up rather than
    # crashing on a name save() was never given
    def interrupted_mkstemp(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(tempfile, "mkstemp", interrupted_mkstemp)
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    with pytest.raises(KeyboardInterrupt):
        persistence.save(store, str(tmp_path / "dump.mrdb"))
    assert list(tmp_path.iterdir()) == []


def test_stale_temporaries_names_a_file_in_a_directory_that_is_listable_but_not_searchable(
    tmp_path
):
    directory = tmp_path / "listable-not-searchable"
    directory.mkdir()
    stranded_name = "dump.mrdb.abcd1234.tmp.mrdb"
    (directory / stranded_name).write_bytes(b"left by a killed save")
    directory.chmod(0o444)
    try:
        assert persistence.stale_temporaries(str(directory / "dump.mrdb")) == [stranded_name]
    finally:
        directory.chmod(0o755)


def test_stale_temporaries_names_the_older_bare_tmpxxxxxxxx_shape(tmp_path):
    path = tmp_path / "dump.mrdb"
    legacy = tmp_path / "tmpabcd1234"
    legacy.write_bytes(b"left by an older build's crash")
    assert persistence.stale_temporaries(str(path)) == ["tmpabcd1234"]


def test_stale_temporaries_follows_this_filesystem_on_another_spelling_of_the_path(tmp_path):
    # a save started as DUMP.MRDB and a start given dump.mrdb are one snapshot where the
    # filesystem folds case and two where it does not, so what is asserted here is that
    # the report agrees with the filesystem underneath it rather than with either guess.
    # the same probe the code cannot make for itself is cheap in a test: write one name,
    # look for the other
    (tmp_path / "Case.probe").write_bytes(b"")
    folds_case = (tmp_path / "case.PROBE").exists()
    path = tmp_path / "dump.mrdb"
    other_spelling = "DUMP.MRDB.abcd1234.tmp.mrdb"
    (tmp_path / other_spelling).write_bytes(b"left by a save started under the other spelling")
    reported = persistence.stale_temporaries(str(path))
    if folds_case:
        assert reported == [other_spelling], (
            "this filesystem folds case, so that file is this path's own temporary file "
            "and nothing else would ever name it", reported)
    else:
        assert reported == [], (
            "this filesystem keeps the two spellings apart, so that file belongs to a "
            "snapshot named DUMP.MRDB and claiming it for dump.mrdb would be wrong",
            reported)
    # and either way a temporary file whose snapshot name merely differs is not this
    # path's, which is what keeps the case-folding arm from widening into a guess
    assert persistence.stale_temporaries(str(tmp_path / "other.mrdb")) == []


def test_a_save_keeps_the_mode_of_the_snapshot_it_replaces(tmp_path):
    # the file that survives a save is the temporary one, renamed over the old snapshot,
    # so without carrying the mode across, every save quietly narrows a file an operator
    # widened -- for a backup reader, say -- and nothing reports it. a first save has no
    # mode to carry and keeps mkstemp's own 0600
    path = tmp_path / "dump.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(path))
    assert stat.S_IMODE(path.stat().st_mode) == 0o600, "a first save should stay private"
    path.chmod(0o644)
    persistence.save(store, str(path))
    assert stat.S_IMODE(path.stat().st_mode) == 0o644, (
        "the save replaced the snapshot's mode with its temporary file's")


def test_stale_temporaries_still_refuses_a_symlink_and_a_directory_and_escapes_metacharacters_and_sorts_the_result(
    tmp_path
):
    path = tmp_path / "dump.mrdb"
    current = "dump.mrdb.bbbb2222.tmp.mrdb"
    legacy = "tmpaaaa1111"
    (tmp_path / current).write_bytes(b"a real temporary file")
    (tmp_path / legacy).write_bytes(b"a real legacy temporary file")
    (tmp_path / "dump.mrdb.cccc3333.tmp.mrdb").mkdir()
    (tmp_path / "link-target").write_bytes(b"whatever a link of the right name points at")
    (tmp_path / "dump.mrdb.dddd4444.tmp.mrdb").symlink_to(tmp_path / "link-target")
    # "." in the snapshot's own basename is a regex metacharacter -- unescaped, it would
    # also match a literal "X" here, wrongly claiming a file that belongs to a
    # differently-named snapshot
    (tmp_path / "dumpXmrdb.eeee5555.tmp.mrdb").write_bytes(b"belongs to a different name")
    assert persistence.stale_temporaries(str(path)) == sorted([current, legacy])


def test_a_snapshot_with_a_repeated_key_is_refused_naming_the_key():
    store = Store()
    store.write(b"only", b"first", keep_ttl=False)
    blob = bytearray(persistence.encode(store))
    header = bytes(blob[:8])
    entry = bytes(blob[12:-4])
    body = header + struct.pack("<I", 2) + entry + entry
    spliced = body + struct.pack("<I", zlib.crc32(body))
    with pytest.raises(persistence.SnapshotError, match="only"):
        persistence.decode(spliced)


def test_a_snapshot_version_of_zero_with_a_correct_checksum_is_refused_as_unsupported():
    # pins != rather than > deciding this: SNAPSHOT_VERSION is 1, so a mutant comparing
    # with > would let a version of 0 straight through to an empty, otherwise valid blob
    blob = bytearray(persistence.encode(Store()))
    blob[4:8] = struct.pack("<I", 0)
    blob[-4:] = struct.pack("<I", zlib.crc32(bytes(blob[:-4])))
    with pytest.raises(persistence.SnapshotError, match="unsupported"):
        persistence.decode(bytes(blob))


def test_an_unrecognised_type_byte_laid_out_as_a_list_is_refused():
    # test_an_unrecognised_kind_byte_is_refused_as_snapshot_error above flips the type
    # byte on a string entry, which a decoder branching on "!= TYPE_STRING" refuses
    # exactly as well as one branching on "== TYPE_LIST" does. this flips it on a list
    # entry instead: the bytes behind the byte are shaped as a genuine list, so a
    # decoder that takes any byte other than TYPE_STRING for TYPE_LIST would parse it
    # as one and never refuse it at all
    store = Store()
    store.write(b"l", deque([b"a", b"b"]), keep_ttl=False)
    blob = bytearray(persistence.encode(store))
    # magic, version and key count are four bytes each, then a four-byte key length and
    # the key itself, and the type byte is next
    type_at = 4 + 4 + 4 + 4 + len(b"l")
    assert blob[type_at] == persistence.TYPE_LIST
    unknown = 0x7F
    assert unknown not in (persistence.TYPE_STRING, persistence.TYPE_LIST)
    blob[type_at] = unknown
    blob[-4:] = struct.pack("<I", zlib.crc32(bytes(blob[:-4])))
    with pytest.raises(persistence.SnapshotError):
        persistence.decode(bytes(blob))


class _FakeLengthBytes(bytes):
    """Stands in for a value long enough to overflow encode()'s uint32 length field,
    without ever allocating the four gibibytes such a value would actually take: only
    `__len__` lies, so the guard sees the size it would refuse while the fixture itself
    holds a handful of real bytes.
    """

    def __len__(self):
        return 0x1_0000_0000


def test_a_value_too_long_for_the_formats_uint32_length_field_is_refused_naming_the_key(
    tmp_path
):
    store = Store()
    store.write(b"toolong", _FakeLengthBytes(b"x"), keep_ttl=False)
    with pytest.raises(ValueError, match="toolong"):
        persistence.encode(store)
    # save() must fail before it creates a temporary file, the same guarantee the
    # empty-list refusal above already gives
    with pytest.raises(ValueError, match="toolong"):
        persistence.save(store, str(tmp_path / "dump.mrdb"))
    assert list(tmp_path.iterdir()) == [], "save left something behind before encoding failed"


def test_a_key_too_long_for_the_formats_uint32_length_field_is_refused(tmp_path):
    store = Store()
    store.write(_FakeLengthBytes(b"k"), b"v", keep_ttl=False)
    with pytest.raises(ValueError, match="the key itself"):
        persistence.encode(store)
    assert list(tmp_path.iterdir()) == [], "save must not create anything before encode runs"


def test_a_list_element_too_long_for_the_formats_uint32_length_field_is_refused(tmp_path):
    store = Store()
    store.write(b"l", deque([_FakeLengthBytes(b"x")]), keep_ttl=False)
    with pytest.raises(ValueError, match="one of its elements"):
        persistence.encode(store)
    assert list(tmp_path.iterdir()) == [], "save must not create anything before encode runs"


class _FakeLengthDeque(deque):
    """Stands in for a list with billions of elements, without ever holding that many:
    only `__len__` lies. `_encode_entry` checks the container's own length before it
    ever calls `list()` on it, so the handful of real elements this fixture actually
    holds are never materialized before the refusal fires.
    """

    def __len__(self):
        return 0x1_0000_0000


def test_a_list_element_count_too_long_for_the_formats_uint32_length_field_is_refused(tmp_path):
    store = Store()
    store.write(b"l", _FakeLengthDeque([b"a"]), keep_ttl=False)
    with pytest.raises(ValueError, match="its element count"):
        persistence.encode(store)
    assert list(tmp_path.iterdir()) == [], "save must not create anything before encode runs"


def test_load_decides_the_type_from_the_descriptor_not_the_path(tmp_path, monkeypatch):
    # the regular-file check reads os.fstat() on the descriptor load() itself opened,
    # not os.stat() on the path -- a fifo reported for a descriptor that really names a
    # regular file proves that: a decoder that re-stated the path here would see the
    # real, unmodified file and load it instead of refusing it
    path = tmp_path / "dump.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(path))

    real_fstat = os.fstat

    def fifo_fstat(fd):
        result = real_fstat(fd)
        fake_mode = stat.S_IFIFO | stat.S_IMODE(result.st_mode)
        return os.stat_result((fake_mode,) + tuple(result)[1:])

    monkeypatch.setattr(os, "fstat", fifo_fstat)
    with pytest.raises(persistence.SnapshotError, match="not a regular file"):
        persistence.load(str(path))


def test_load_opens_with_o_nonblock_so_a_fifo_cannot_hang_it(tmp_path, monkeypatch):
    path = tmp_path / "dump.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(path))

    real_open = os.open
    flags_seen = []

    def recording_open(p, flags, *args, **kwargs):
        flags_seen.append(flags)
        return real_open(p, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", recording_open)
    persistence.load(str(path))
    assert flags_seen, "load() never called os.open()"
    assert flags_seen[0] & os.O_NONBLOCK, (
        "load() must open with O_NONBLOCK, or a fifo at the path blocks it forever",
        flags_seen[0])


def test_load_closes_its_descriptor_even_when_the_blob_is_corrupt(tmp_path, monkeypatch):
    path = tmp_path / "dump.mrdb"
    path.write_bytes(b"not a valid snapshot at all")

    real_open, real_close = os.open, os.close
    opened = []
    closed = []

    def recording_open(p, flags, *args, **kwargs):
        fd = real_open(p, flags, *args, **kwargs)
        opened.append(fd)
        return fd

    def recording_close(fd):
        closed.append(fd)
        return real_close(fd)

    monkeypatch.setattr(os, "open", recording_open)
    monkeypatch.setattr(os, "close", recording_close)
    for _ in range(50):
        with pytest.raises(persistence.SnapshotError):
            persistence.load(str(path))
    assert len(opened) == 50, opened
    assert closed == opened, (
        "a descriptor from a failed, corrupt load was never closed", opened, closed)


def test_check_writable_accepts_a_symlink_to_a_directory_and_save_writes_through_it(tmp_path):
    # os.rename() replaces the symlink itself rather than following it into the
    # directory it names, so a save through a "current -> real-dir/dump.mrdb"-style
    # path never touches what the link points at
    real_dir = tmp_path / "elsewhere"
    real_dir.mkdir()
    (real_dir / "untouched.txt").write_bytes(b"leave this alone")
    link = tmp_path / "dump.mrdb"
    link.symlink_to(real_dir)

    persistence.check_writable(str(link))
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(link))

    assert not link.is_symlink(), (
        "save should have replaced the symlink with the snapshot, not written through it"
    )
    assert persistence.load(str(link)).lookup(b"k") == b"v"
    assert sorted(p.name for p in real_dir.iterdir()) == ["untouched.txt"], (
        "the directory the symlink pointed at was written into"
    )
    assert (real_dir / "untouched.txt").read_bytes() == b"leave this alone"


def test_check_writable_accepts_a_symlink_to_an_immutable_file_and_save_leaves_it_untouched(
    tmp_path
):
    if not hasattr(os, "chflags") or not hasattr(stat, "UF_IMMUTABLE"):
        pytest.skip("no chflags/UF_IMMUTABLE on this platform")
    target = tmp_path / "immutable-target"
    target.write_bytes(b"never touched")
    try:
        os.chflags(str(target), stat.UF_IMMUTABLE)
    except OSError:
        pytest.skip("this filesystem does not honor UF_IMMUTABLE")
    try:
        # the flags that could block a rename are the symlink's own -- checked with
        # lstat, not the immutable target's -- and a plain symlink carries none
        link = tmp_path / "dump.mrdb"
        link.symlink_to(target)
        persistence.check_writable(str(link))
        store = Store()
        store.write(b"k", b"v", keep_ttl=False)
        persistence.save(store, str(link))
        assert not link.is_symlink(), (
            "save should have replaced the symlink, not written through it to the "
            "immutable target"
        )
        assert persistence.load(str(link)).lookup(b"k") == b"v"
        assert target.read_bytes() == b"never touched"
    finally:
        os.chflags(str(target), 0)


def test_a_directory_that_can_be_renamed_into_but_not_read_does_not_fail_the_save(
    tmp_path, caplog
):
    # write and execute permission is everything os.rename() needs from a directory;
    # read permission is a separate thing, needed only to open the directory itself for
    # the sync that follows the rename -- a directory granting the first but not the
    # second is exactly the case _sync_the_directory()'s own os.open() can fail on,
    # after the snapshot is already safely renamed into place
    directory = tmp_path / "write-and-execute-only"
    directory.mkdir()
    directory.chmod(0o300)
    try:
        path = directory / "dump.mrdb"
        store = Store()
        store.write(b"k", b"v", keep_ttl=False)
        with caplog.at_level(logging.WARNING, logger="persistence"):
            persistence.save(store, str(path))  # must return, not raise
    finally:
        directory.chmod(0o700)

    assert persistence.load(str(path)).lookup(b"k") == b"v", (
        "the snapshot was not written even though the rename that lands it needs no "
        "read permission on the directory"
    )
    assert sorted(p.name for p in directory.iterdir()) == ["dump.mrdb"], (
        "something besides the snapshot itself was left beside it"
    )
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, (
        "a directory that could not be synced must log exactly one warning", warnings)
    assert str(directory) in warnings[0].getMessage(), (
        "the warning does not name the directory that could not be synced",
        warnings[0].getMessage())


def test_a_snapshot_with_an_unrecognised_kind_byte_and_a_repeated_key_is_refused_naming_the_kind():
    # the same construction test_a_snapshot_with_a_repeated_key_is_refused_naming_the_key
    # above uses, except the entry's own type byte is first flipped to one this format
    # does not recognise -- from_items() raises on that before it ever gets far enough
    # to notice the repeat, so the refusal has to name the kind and not the repeat
    store = Store()
    store.write(b"only", b"first", keep_ttl=False)
    blob = bytearray(persistence.encode(store))
    header = bytes(blob[:8])
    entry = bytearray(blob[12:-4])
    # a four-byte key length and the key itself come first in one entry, and the type
    # byte is next
    type_at = 4 + len(b"only")
    assert entry[type_at] == persistence.TYPE_STRING
    entry[type_at] = 0x7F
    entry = bytes(entry)
    body = header + struct.pack("<I", 2) + entry + entry
    spliced = body + struct.pack("<I", zlib.crc32(body))
    with pytest.raises(persistence.SnapshotError, match="not a snapshot kind") as refused:
        persistence.decode(spliced)
    assert "twice" not in str(refused.value), (
        "the kind byte is what from_items() actually raised on, before the repeat was "
        "ever reached -- naming the repeat instead hides the real cause",
        str(refused.value))


def test_stale_temporaries_follows_this_filesystem_on_another_spelling_of_the_suffix(tmp_path):
    # a save always lower-cases the random part and the suffix it appends, so the
    # canonical spelling of any temporary file this path could have produced keeps the
    # basename exactly as given and lowers everything after it. a name differing only in
    # the suffix's own case -- TMP.MRDB rather than tmp.mrdb -- is this path's temporary
    # file wherever the filesystem folds case, and belongs to nothing here otherwise,
    # which is asked of the filesystem itself rather than assumed either way
    (tmp_path / "Case.probe").write_bytes(b"")
    folds_case = (tmp_path / "case.PROBE").exists()
    path = tmp_path / "dump.mrdb"
    other_suffix = "dump.mrdb.abcd1234.TMP.MRDB"
    (tmp_path / other_suffix).write_bytes(b"left by a save whose suffix case differs")
    reported = persistence.stale_temporaries(str(path))
    if folds_case:
        assert reported == [other_suffix], (
            "this filesystem folds case, so that file is this path's own temporary "
            "file", reported)
    else:
        assert reported == [], (
            "this filesystem keeps the two spellings apart, so a suffix spelled in "
            "upper case belongs to no snapshot this call can name", reported)


def test_is_legacy_temporary_name_recognizes_only_the_bare_tmp_shape():
    assert persistence.is_legacy_temporary_name("tmpabcd1234")
    assert not persistence.is_legacy_temporary_name("dump.mrdb.abcd1234.tmp.mrdb")


class _FakeLengthAtTheUint32Ceiling(bytes):
    """Reports a length of exactly `persistence._UINT32_MAX`, the one value the guard's
    `>` comparison must accept, without ever holding that many real bytes -- only
    `__len__` lies, the same trick `_FakeLengthBytes` above plays for a length past it.
    """

    def __len__(self):
        return persistence._UINT32_MAX


def test_a_value_exactly_at_the_uint32_ceiling_is_accepted_not_refused():
    # pins > rather than >=: a length of exactly _UINT32_MAX is the largest this
    # format's length field can hold, not one past it, and belongs on the accepted side
    # of the boundary
    store = Store()
    store.write(b"k", _FakeLengthAtTheUint32Ceiling(b"x"), keep_ttl=False)
    persistence.encode(store)  # must not raise


def test_check_writable_names_an_immutable_directory_as_not_writable(tmp_path):
    if not hasattr(os, "chflags") or not hasattr(stat, "UF_IMMUTABLE"):
        pytest.skip("no chflags/UF_IMMUTABLE on this platform")
    directory = tmp_path / "immutable-directory"
    directory.mkdir()
    try:
        os.chflags(str(directory), stat.UF_IMMUTABLE)
    except OSError:
        pytest.skip("this filesystem does not honor UF_IMMUTABLE on a directory")
    try:
        # mkstemp inside an immutable directory fails with EPERM rather than EACCES --
        # both have to be recognised, or this refusal falls through to the generic
        # message instead of naming the directory not writable
        with pytest.raises(persistence.SnapshotError, match="is not writable"):
            persistence.check_writable(str(directory / "dump.mrdb"))
    finally:
        os.chflags(str(directory), 0)


def test_check_writable_refuses_an_existing_snapshot_under_uf_append(tmp_path):
    if not hasattr(os, "chflags") or not hasattr(stat, "UF_APPEND"):
        pytest.skip("no chflags/UF_APPEND on this platform")
    path = tmp_path / "dump.mrdb"
    good = Store()
    good.write(b"k", b"v", keep_ttl=False)
    persistence.save(good, str(path))
    try:
        os.chflags(str(path), stat.UF_APPEND)
    except OSError:
        pytest.skip("this filesystem does not honor UF_APPEND")
    try:
        with pytest.raises(persistence.SnapshotError):
            persistence.check_writable(str(path))
    finally:
        os.chflags(str(path), 0)


def test_check_writable_refuses_an_existing_snapshot_under_sf_append(tmp_path, monkeypatch):
    # asked of the platform's own stat result rather than of the stat module: the
    # SF_APPEND constant is defined everywhere Python runs, and the field the bitmask
    # under test reads is not -- st_flags is BSD's, and on Linux the fake below cannot
    # be built at all. Guarding on the constant skipped nothing and went red in CI
    if not hasattr(os.lstat(str(tmp_path)), "st_flags"):
        pytest.skip("this platform's stat results carry no flags field")
    path = tmp_path / "dump.mrdb"
    good = Store()
    good.write(b"k", b"v", keep_ttl=False)
    persistence.save(good, str(path))

    # SF_APPEND is a superuser-only flag to set on this platform, so it is faked onto
    # the snapshot's own lstat rather than actually applied with chflags -- the bitmask
    # under test only reads st_flags off whatever lstat() answers and does not care how
    # the flag arrived there
    real_lstat = os.lstat
    target = str(path)

    def sf_append_lstat(p, *args, **kwargs):
        result = real_lstat(p, *args, **kwargs)
        if str(p) == target:
            return os.stat_result(
                tuple(result), {"st_flags": result.st_flags | stat.SF_APPEND})
        return result

    monkeypatch.setattr(os, "lstat", sf_append_lstat)
    with pytest.raises(persistence.SnapshotError):
        persistence.check_writable(str(path))


def test_save_closes_the_mkstemp_descriptor_when_fdopen_raises(tmp_path, monkeypatch):
    closed = []
    real_close = os.close

    def recording_close(fd):
        closed.append(fd)
        return real_close(fd)

    def failing_fdopen(fd, *args, **kwargs):
        raise OSError("fdopen refused")

    monkeypatch.setattr(os, "close", recording_close)
    monkeypatch.setattr(os, "fdopen", failing_fdopen)
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    with pytest.raises(OSError, match="fdopen refused"):
        persistence.save(store, str(tmp_path / "dump.mrdb"))
    assert closed, "save() left the mkstemp descriptor open when fdopen raised"
    assert list(tmp_path.iterdir()) == [], (
        "the unopened temporary file was left on disk after fdopen raised"
    )


def test_sync_the_directory_closes_its_descriptor(tmp_path, monkeypatch):
    real_open, real_close = os.open, os.close
    opened = []
    closed = []

    def recording_open(p, flags, *args, **kwargs):
        fd = real_open(p, flags, *args, **kwargs)
        opened.append(fd)
        return fd

    def recording_close(fd):
        closed.append(fd)
        return real_close(fd)

    monkeypatch.setattr(os, "open", recording_open)
    monkeypatch.setattr(os, "close", recording_close)
    for _ in range(50):
        persistence._sync_the_directory(str(tmp_path))
    assert len(opened) == 50, opened
    assert closed == opened, (
        "_sync_the_directory left a descriptor open on some call", opened, closed)


def test_one_file_under_both_spellings_returns_false_when_the_respelling_names_nothing(
    tmp_path
):
    # called directly with a name whose re-spelled counterpart does not exist on disk at
    # all -- a stub that always answered True would pass every positive case elsewhere in
    # this module too, since those all have a real file at the respelled name
    assert persistence._one_file_under_both_spellings(
        str(tmp_path), "dump.mrdb.abcd1234.TMP.MRDB", "dump.mrdb"
    ) is False


class _FakeEntryWhoseTypeCannotBeDetermined:
    """Stands in for an `os.DirEntry` whose cached type `is_file()` cannot answer --
    what a directory listable but not searchable does to every entry inside it.
    """

    def __init__(self, name):
        self.name = name

    def is_file(self, follow_symlinks=False):
        raise OSError("type could not be determined")


def test_stale_temporaries_reports_a_name_whose_type_scandir_cannot_determine(
    tmp_path, monkeypatch
):
    path = tmp_path / "dump.mrdb"
    stuck_name = "dump.mrdb.abcd1234.tmp.mrdb"

    def fake_scandir(directory):
        return iter([_FakeEntryWhoseTypeCannotBeDetermined(stuck_name)])

    monkeypatch.setattr(os, "scandir", fake_scandir)
    assert persistence.stale_temporaries(str(path)) == [stuck_name], (
        "a name whose type os.scandir() cannot determine must still be reported -- "
        "dropping it is silence exactly where the warning matters most"
    )


def _deny_delete_or_skip(path, user_denied):
    # asked of the platform by trying it, not of the platform's name: chmod +a is macOS's
    # ACL syntax and every other system refuses the flag outright, which is the only
    # answer this needs
    done = subprocess.run(["chmod", "+a", "%s deny delete" % user_denied, str(path)],
                          capture_output=True, text=True)
    if done.returncode != 0:
        pytest.skip("this platform will not set an ACL denying delete: %s" % done.stderr.strip())


def _clear_deny_delete(path, user_denied):
    subprocess.run(["chmod", "-a", "%s deny delete" % user_denied, str(path)],
                   capture_output=True, text=True)


def test_check_writable_refuses_a_snapshot_an_acl_will_not_let_a_rename_replace(tmp_path):
    # the flags field says what chflags can express and nothing about an ACL, and the
    # probe that follows makes a NEW file, whose permissions are the directory's. A
    # snapshot under `deny delete` passed the whole check and then failed every save,
    # once an interval, forever -- which is the failure this check exists to move to
    # startup rather than a fact about the path it cannot see
    path = tmp_path / "dump.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(path))
    before = path.read_bytes()
    user = getpass.getuser()
    _deny_delete_or_skip(path, user)
    try:
        with pytest.raises(persistence.SnapshotError, match="cannot be removed"):
            persistence.check_writable(str(path))
        # and the refusal is right about the save: the rename really is denied
        with pytest.raises(OSError):
            persistence.save(store, str(path))
        assert path.read_bytes() == before, "the refused save changed the snapshot"
    finally:
        _clear_deny_delete(path, user)
        for left in tmp_path.iterdir():
            if left != path:
                left.unlink()


def test_the_delete_rehearsal_leaves_nothing_behind_on_a_path_it_accepts(tmp_path):
    path = tmp_path / "dump.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(path))
    persistence.check_writable(str(path))
    assert [entry.name for entry in tmp_path.iterdir()] == ["dump.mrdb"], (
        "the rehearsal left a second name for the snapshot behind on a path it accepted"
    )


def test_the_link_probe_is_not_reported_as_a_stranded_save(tmp_path):
    # it is a second name for the operator's own data, not a temporary file, and telling
    # them to weigh it as one would invite them to read it as a newer snapshot
    path = tmp_path / "dump.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(path))
    os.link(str(path), str(tmp_path / ("dump.mrdb.%d.deadbeef.linkprobe" % os.getpid())))
    (tmp_path / "dump.mrdb.abcd1234.tmp.mrdb").write_bytes(b"a real stranded save")
    assert persistence.stale_temporaries(str(path)) == ["dump.mrdb.abcd1234.tmp.mrdb"]
    assert not persistence.is_legacy_temporary_name("dump.mrdb.linkprobe")


def test_a_key_repeated_three_times_is_refused_with_its_own_count():
    # "twice" whatever the count is a claim about the file an operator may act on
    def entry(key, value):
        return (struct.pack("<I", len(key)) + key + bytes((persistence.TYPE_STRING,))
                + struct.pack("<q", -1) + struct.pack("<I", len(value)) + value)

    body = (persistence.MAGIC + struct.pack("<II", persistence.SNAPSHOT_VERSION, 3)
            + entry(b"triple", b"a") + entry(b"triple", b"b") + entry(b"triple", b"c"))
    blob = body + struct.pack("<I", zlib.crc32(body))
    with pytest.raises(persistence.SnapshotError, match=r"3 times"):
        persistence.decode(blob)


def test_the_delete_rehearsal_is_skipped_for_a_symlink_whose_target_is_undeletable(tmp_path):
    # the rehearsal makes a hard link, and os.link follows a symlink to its target, so
    # running it on a symlinked path asks about the target's deletability -- which a save
    # never needs, because the rename replaces the link and leaves the target alone.
    # Without the guard that keeps the rehearsal to a regular file, a snapshot path
    # pointing at an undeletable file is refused although a save writes it: the same
    # confusion between the entry and what it points at that the lstat checks above fixed
    target = tmp_path / "real.mrdb"
    link = tmp_path / "dump.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(target))
    target_bytes = target.read_bytes()
    link.symlink_to(target)
    user = getpass.getuser()
    _deny_delete_or_skip(target, user)
    try:
        persistence.check_writable(str(link))
        persistence.save(store, str(link))
        assert not link.is_symlink(), "the save did not replace the link, as rename does"
        assert target.read_bytes() == target_bytes, "the undeletable target was touched"
        assert sorted(persistence.load(str(link))._data) == [b"k"]
    finally:
        _clear_deny_delete(target, user)


def test_a_repeat_after_a_unique_key_names_the_repeat_and_not_the_first_key():
    # every other repeat test builds a blob holding one distinct key, where a scan that
    # reported whichever key it reached first would still be right by accident. This one
    # puts a unique key ahead of the repeat, so naming the first key is visibly wrong
    def entry(key, value):
        return (struct.pack("<I", len(key)) + key + bytes((persistence.TYPE_STRING,))
                + struct.pack("<q", -1) + struct.pack("<I", len(value)) + value)

    body = (persistence.MAGIC + struct.pack("<II", persistence.SNAPSHOT_VERSION, 3)
            + entry(b"alone", b"a") + entry(b"pair", b"b") + entry(b"pair", b"c"))
    blob = body + struct.pack("<I", zlib.crc32(body))
    with pytest.raises(persistence.SnapshotError) as refused:
        persistence.decode(blob)
    assert "pair" in str(refused.value), (
        "the refusal named a key that is not the repeated one", str(refused.value))
    assert "alone" not in str(refused.value), str(refused.value)
    assert "2 times" in str(refused.value), str(refused.value)


def test_a_directory_sync_that_raises_outright_does_not_claim_a_stranded_file(
    tmp_path, monkeypatch
):
    # _sync_the_directory swallows what it means to swallow, but its own finally closes a
    # descriptor and that close is not inside anything. Run from inside save()'s guarded
    # region, an exception escaping here reaches the handler that removes the temporary
    # file -- a name the rename has already consumed -- and attaches a note saying a
    # full-size copy was stranded, over a save whose snapshot is on disk and correct
    real_close = os.close

    def close_refusing_directories(fd):
        try:
            is_directory = stat.S_ISDIR(os.fstat(fd).st_mode)
        except OSError:
            is_directory = False
        real_close(fd)
        if is_directory:
            raise OSError(errno.EIO, "Input/output error")

    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    path = tmp_path / "dump.mrdb"
    monkeypatch.setattr(os, "close", close_refusing_directories)
    raised = None
    try:
        persistence.save(store, str(path))
    except OSError as exc:
        raised = exc
    monkeypatch.undo()

    assert path.exists() and sorted(persistence.load(str(path))._data) == [b"k"], (
        "the snapshot is not on disk, so this test is not measuring what it means to"
    )
    left = [entry.name for entry in tmp_path.iterdir() if entry.name != "dump.mrdb"]
    assert not left, ("a temporary file was stranded by a failure after the rename", left)
    if raised is not None:
        assert not any("left the temporary file" in note
                       for note in getattr(raised, "__notes__", [])), (
            "a save whose snapshot is on disk claimed it stranded a temporary file",
            getattr(raised, "__notes__", []))


def test_the_rehearsal_never_touches_a_name_it_did_not_create(tmp_path):
    # its link carries this process's pid and eight random characters, so it cannot
    # collide with a file that is already there and has no reason to remove one. A fixed
    # name meant the opposite: an operator's own file under it, or a link to an older
    # snapshot whose own name is gone, was deleted by a check that exists to refuse
    # before anything is touched
    path = tmp_path / "dump.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(path))

    theirs = tmp_path / "dump.mrdb.linkprobe"
    theirs.write_bytes(b"not this check's to remove")
    older = tmp_path / "dump.mrdb.1.deadbeef.linkprobe"
    other = Store()
    other.write(b"older", b"value", keep_ttl=False)
    persistence.save(other, str(older))

    persistence.check_writable(str(path))

    assert theirs.read_bytes() == b"not this check's to remove", (
        "the startup check destroyed a file it did not create")
    assert sorted(persistence.load(str(older))._data) == [b"older"], (
        "a file shaped exactly like the probe's own name was removed")
    assert sorted(entry.name for entry in tmp_path.iterdir()) == [
        "dump.mrdb", "dump.mrdb.1.deadbeef.linkprobe", "dump.mrdb.linkprobe"], (
        "the rehearsal left something of its own behind")


def test_concurrent_rehearsals_do_not_refuse_each_other(tmp_path, monkeypatch):
    # two servers starting against one snapshot path is a real arrangement -- a
    # supervisor restart overlapping the instance it replaces. With one shared probe name
    # they contended for a single directory entry and refused each other: measured, five
    # to seven of twelve concurrent calls refused on a path that is fine, one of them
    # naming another instance's probe as a file to move aside
    path = tmp_path / "dump.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(path))

    # two things are checked, because the interesting one is not visible from outside.
    # Sharing a name no longer makes concurrent starts refuse each other -- the loser's
    # os.link now meets EEXIST and takes the "cannot rehearse" branch -- so it fails
    # silently instead, by skipping the check on every start but one. What says the
    # rehearsals are independent is that each used a name of its own, which is asserted
    # directly; the absence of refusals is the cheaper half and is kept beside it
    workers, rounds = 12, 5
    refusals = []
    names = []
    guard = threading.Lock()
    real_link = os.link

    def recording_link(source, target, *args, **kwargs):
        with guard:
            names.append(target)
        return real_link(source, target, *args, **kwargs)

    monkeypatch.setattr(os, "link", recording_link)

    for _ in range(rounds):
        barrier = threading.Barrier(workers)

        def rehearse():
            barrier.wait()
            try:
                persistence.check_writable(str(path))
            except persistence.SnapshotError as exc:
                with guard:
                    refusals.append(str(exc))

        threads = [threading.Thread(target=rehearse) for _ in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert not refusals, ("concurrent starts refused a path that is writable", refusals)
    assert len(names) == workers * rounds, ("a rehearsal did not run", len(names))
    assert len(set(names)) == len(names), (
        "two rehearsals contended for one name",
        sorted(n for n in names if names.count(n) > 1)[:4])
    assert [entry.name for entry in tmp_path.iterdir()] == ["dump.mrdb"], (
        "a concurrent rehearsal left a link behind")


def test_a_filesystem_that_will_not_take_a_hard_link_leaves_the_check_where_it_stood(
    tmp_path, monkeypatch
):
    # the rehearsal is an addition, not a precondition: where os.link cannot be made the
    # check falls back to what it did before rather than refusing every path
    path = tmp_path / "dump.mrdb"
    store = Store()
    store.write(b"k", b"v", keep_ttl=False)
    persistence.save(store, str(path))

    def no_hard_links(*args, **kwargs):
        raise OSError(errno.ENOSYS, "Function not implemented")

    monkeypatch.setattr(os, "link", no_hard_links)
    persistence.check_writable(str(path))
    assert [entry.name for entry in tmp_path.iterdir()] == ["dump.mrdb"]
