"""The snapshot format's contract over bit-flipped inputs rather than chosen ones: a
fixture snapshot holding a string, a list and a TTL, corrupted one bit at a random
offset, must always refuse as `SnapshotError`, for its reason -- so none can load a store
whose values or deadlines differ from the original, and a flip that is accepted is
reported with whether the store it loaded does. Two companions keep the corpus honest:
one asserting the flips actually reach every region the format has, and one measuring
directly what a format with no checksum at all would accept over this same corpus:
some fraction of those same flips, accepted with values or deadlines that differ from
the original.

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
    # the whole store, not only its values -- key, kind, value and absolute expiry --
    # or a flip that lands on a deadline and nowhere else has nothing here to disagree
    # with, both in the refusal test's report and in the control at the bottom
    return persistence.encode(store), list(store.snapshot_items())


def test_every_single_bit_flip_is_refused_as_snapshot_error():
    blob, original = _fixture_blob()
    rnd = random.Random(SEED)
    for round_index in range(ROUNDS):
        offset = rnd.randrange(len(blob))
        bit = rnd.randrange(8)
        flipped = bytearray(blob)
        flipped[offset] ^= 1 << bit
        try:
            loaded = persistence.decode(bytes(flipped))
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
            # accepted at all is already the failure, so no flip in the corpus can load
            # a store that differs without failing here. what the report adds is which of
            # two different defects it was: a flip loaded as the original store, or one
            # loaded as wrong data. snapshot_items() rather than _data alone, so a flip
            # that changed only a deadline reads as wrong data too
            differs = list(loaded.snapshot_items()) != original
            raise AssertionError(
                "round %d offset %d bit %d was accepted, loading %s" % (
                    round_index, offset, bit,
                    "a store that differs from the original" if differs
                    else "a store identical to the original")
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
    original_values = {key: value for key, _, value, _ in original}
    rnd = random.Random(SEED)
    accepted_without_checksum = 0
    accepted_with_only_a_deadline_changed = 0
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
        items = list(loaded.snapshot_items())
        if items != original:
            accepted_without_checksum += 1
            # isolates a flip that reached only a deadline: every value still matches
            # by key, so the whole-store difference asserted above owes entirely to
            # the expiry field none of this loop's other bookkeeping looks at
            loaded_values = {key: value for key, _, value, _ in items}
            if loaded_values == original_values:
                accepted_with_only_a_deadline_changed += 1
    assert accepted_without_checksum > 0, (
        "with the trailer made to agree, no flip was ever accepted with wrong data -- "
        "this corpus does not demonstrate what the checksum is for"
    )
    assert accepted_with_only_a_deadline_changed > 0, (
        "every accepted flip changed some value too, so a comparison over values alone "
        "would never have been shown a flip that reached only a deadline"
    )
