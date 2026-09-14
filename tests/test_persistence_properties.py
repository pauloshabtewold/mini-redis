"""The snapshot format's contract over bit-flipped inputs rather than chosen ones: a
fixture snapshot holding a string, a list and a TTL, corrupted one bit at a random
offset, must always refuse as `SnapshotError` and never load a store that differs from
the original. Two companions keep the corpus honest: one asserting the flips actually
reach every region the format has, and one measuring directly what a format with no
checksum at all would accept over this same corpus: some fraction of those same flips,
accepted with data that differs from the original.

The corpus is built from a fixed seed, matching test_store_properties.py's shape (there
is no `hypothesis` dependency in this project and none is added here). A failure names
the round, the byte offset and the bit.
"""

import random
import struct
import zlib
from collections import deque

import persistence
from store import Store

SEED = 20260821
ROUNDS = 200


def _fixture_blob():
    store = Store()
    store.write(b"alpha", b"HELLOWORLD", keep_ttl=False)
    store.write(b"beta", deque([b"x", b"y"]), keep_ttl=False)
    store.write(b"gamma", b"v", keep_ttl=False)
    store.expire_at(b"gamma", 1_700_000_000_000)
    return persistence.encode(store), dict(store._data)


def test_every_single_bit_flip_is_refused_as_snapshot_error():
    blob, _ = _fixture_blob()
    rnd = random.Random(SEED)
    for round_index in range(ROUNDS):
        offset = rnd.randrange(len(blob))
        bit = rnd.randrange(8)
        flipped = bytearray(blob)
        flipped[offset] ^= 1 << bit
        try:
            persistence.decode(bytes(flipped))
        except persistence.SnapshotError as exc:
            # the type alone is not proof: checking the version or a length prefix before
            # the checksum would also refuse every flip as SnapshotError, for the wrong
            # reason. a flip in the magic is refused before the checksum is ever read;
            # every other flip corrupts bytes the checksum covers, and a CRC32 catches
            # every single-bit error, so nothing past the magic can reach any later check
            message = str(exc)
            if offset < 4:
                assert message.startswith("wrong magic"), (
                    "round %d offset %d bit %d refused as %r, not wrong magic"
                    % (round_index, offset, bit, message)
                )
            else:
                assert message == "checksum mismatch", (
                    "round %d offset %d bit %d refused as %r, not checksum mismatch"
                    % (round_index, offset, bit, message)
                )
            continue
        except Exception as exc:
            raise AssertionError(
                "round %d offset %d bit %d leaked %s instead of SnapshotError"
                % (round_index, offset, bit, type(exc).__name__)
            ) from exc
        else:
            raise AssertionError(
                "round %d offset %d bit %d was accepted" % (round_index, offset, bit)
            )


def test_no_single_bit_flip_loads_a_store_that_differs_from_the_original():
    # the test above asserts every flip refuses; this asserts the same corpus never
    # reaches a decoded store to compare in the first place -- a decoder refusing for
    # the wrong reason but still occasionally decoding something would pass that test
    # and fail this one
    blob, original = _fixture_blob()
    rnd = random.Random(SEED)
    for round_index in range(ROUNDS):
        offset = rnd.randrange(len(blob))
        bit = rnd.randrange(8)
        flipped = bytearray(blob)
        flipped[offset] ^= 1 << bit
        try:
            loaded = persistence.decode(bytes(flipped))
        except persistence.SnapshotError:
            continue
        assert dict(loaded._data) == original, (
            "round %d offset %d bit %d loaded a store that differs from the original"
            % (round_index, offset, bit)
        )


def test_the_flip_corpus_reaches_every_region_of_the_format():
    blob, _ = _fixture_blob()
    rnd = random.Random(SEED)
    regions = {"magic": 0, "version": 0, "payload": 0, "trailer": 0}
    for _ in range(ROUNDS):
        offset = rnd.randrange(len(blob))
        # the bit itself goes unused here -- drawn only so this rnd consumes the same
        # two numbers per round as the bit-flip tests above and below, keeping its
        # offsets in step with the exact corpus those tests run rather than a similar
        # one that drifts apart after round one
        rnd.randrange(8)
        if offset < 4:
            regions["magic"] += 1
        elif offset < 8:
            regions["version"] += 1
        elif offset >= len(blob) - 4:
            regions["trailer"] += 1
        else:
            regions["payload"] += 1
    assert all(regions.values()), "the corpus never reached some region: %r" % regions


def test_without_the_checksum_some_flips_are_accepted_with_wrong_data():
    # measures directly, against this fixture, what a format with no checksum at all
    # would accept: the same corpus, with the trailer recomputed to agree with each
    # flip, is what a decoder with no checksum to check would be handed
    blob, original = _fixture_blob()
    rnd = random.Random(SEED)
    accepted_without_checksum = 0
    for _ in range(ROUNDS):
        offset = rnd.randrange(len(blob))
        bit = rnd.randrange(8)
        flipped = bytearray(blob)
        flipped[offset] ^= 1 << bit
        flipped[-4:] = struct.pack("<I", zlib.crc32(bytes(flipped[:-4])))
        try:
            loaded = persistence.decode(bytes(flipped))
        except persistence.SnapshotError:
            continue
        if dict(loaded._data) != original:
            accepted_without_checksum += 1
    assert accepted_without_checksum > 0, (
        "with the trailer made to agree, no flip was ever accepted with wrong data -- "
        "this corpus does not demonstrate what the checksum is for"
    )
