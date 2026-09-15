"""The snapshot format's contract over chosen inputs: a round trip preserving keys,
both kinds and an exact absolute deadline, the version field at its own offset, a save
observed writing through a temporary file, and every refusal this module names -- an
unsupported version, a blob under the sixteen-byte header, a wrong magic, a trailing
byte past the trailer, truncation at every offset, an unrecognised kind byte, a list
entry whose element count is zero, an empty list refused at encode with the save
leaving nothing on disk, a rename that fails once, and the two paths `load()` tells
apart, a missing one and a corrupt one, entries that stop short of the trailer with a
correct checksum, and the two type bytes pinned against their literal values.
test_persistence_properties.py covers the refusal contract over inputs this module does
not choose.
"""

import os
import pathlib
import struct
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
    real_fsync, real_rename = os.fsync, os.rename

    def recording_fsync(fd):
        # the size on disk at the moment fsync is called, before real_fsync runs --
        # bytes still sitting in handle's own userspace buffer have not reached the
        # file this descriptor names yet, so a deleted flush() shows up here as a short
        # size rather than only in a size read back after save() has already returned
        events.append(("fsync", os.fstat(fd).st_size))
        synced.append(fd)
        return real_fsync(fd)

    def recording_rename(a, b):
        events.append(("rename", None))
        renamed.append((a, b))
        return real_rename(a, b)

    monkeypatch.setattr(os, "fsync", recording_fsync)
    monkeypatch.setattr(os, "rename", recording_rename)

    persistence.save(store, str(path))

    assert synced, "os.fsync was never called, so the bytes are not durable"
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
        "save must fsync exactly once and rename exactly once: %r" % (events,)
    )
    # the order first, so a rename that ran ahead of the fsync is reported as that rather
    # than as a size mismatch read off the wrong event
    assert [kind for kind, _ in events] == ["fsync", "rename"], (
        "fsync must come before rename, or a power loss right after the rename can leave "
        "the real path naming a file whose bytes never reached the disk: %r" % (events,)
    )
    (fsync_size,) = [size for kind, size in events if kind == "fsync"]
    assert fsync_size == len(persistence.encode(store)), (
        "the bytes were still in a userspace buffer when the fsync ran", fsync_size
    )


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


def test_load_on_a_missing_path_raises_file_not_found_rather_than_snapshot_error(tmp_path):
    missing = tmp_path / "absent.mrdb"
    with pytest.raises(FileNotFoundError):
        persistence.load(str(missing))


def test_load_on_an_unreadable_path_raises_snapshot_error(tmp_path):
    directory = tmp_path / "adirectory.mrdb"
    directory.mkdir()
    with pytest.raises(persistence.SnapshotError):
        persistence.load(str(directory))
