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


def _ordinary41():
    # the fixture the README's figures are taken on: 41 plainly named keys, one of them a
    # list, one of them carrying a TTL, sized so the blob lands on exactly 2,036 bytes.
    # plainly named matters -- key00 and key01 differ by one bit, so this fixture has
    # duplicate-key collisions and _separated41() below has none, which is the whole
    # point of publishing two numbers instead of one
    store = Store()
    for i in range(41):
        store.write(b"key%02d" % i, b"v" * 27, keep_ttl=False)
    store.write(b"key20", deque([b"e" * 15, b"e" * 15]), keep_ttl=False)
    store.expire_at(b"key07", 1_700_000_000_000)
    return persistence.encode(store)


def _separated41():
    # the maximum over key choices at this size: five-byte keys drawn from an alphabet
    # that leaves every pair at Hamming distance 2 or more, so no single flip in a key can
    # produce a key another entry already holds and the duplicate-key count is zero
    alphabet = b'!"$\'(+-'
    store = Store()
    keys = []
    for i in range(41):
        keys.append(b"k" + bytes((alphabet[i % 7], alphabet[(i // 7) % 7])) + b"ey")
    assert len(set(keys)) == 41, "the key scheme collided"
    for key in keys:
        store.write(key, b"v" * 27, keep_ttl=False)
    store.write(keys[20], deque([b"e" * 15, b"e" * 15]), keep_ttl=False)
    store.expire_at(keys[7], 1_700_000_000_000)
    return persistence.encode(store), keys


def _why_refused(message):
    # classified off the whole message and never off its first colon-separated field:
    # every reason but three opens "corrupt snapshot:", so splitting there collapses the
    # length prefix and the type byte into one bucket, and the duplicate-key messages name
    # the key so each is its own string. the README itemises these, so the buckets have to
    # be the ones it names
    if "wrong magic" in message:
        return "wrong magic"
    if "unsupported snapshot version" in message:
        return "unsupported version"
    if "trailing bytes after the last entry" in message:
        return "trailing bytes"
    if " times" in message:
        return "duplicate key"
    if "empty list" in message:
        return "empty list"
    if "not a snapshot kind" in message:
        return "type byte naming no kind"
    if "unpack_from requires" in message or "buffer" in message or "index out of range" in message:
        return "length prefix off the end"
    raise AssertionError("unclassified refusal, so the itemisation cannot sum: %r" % message)


def _sweep_with_the_trailer_recomputed(blob):
    # what a length-prefixed format with no checksum amounts to: the trailer is recomputed
    # over the corrupted bytes, so it cannot be what refuses the flip. three outcomes, and
    # every flip lands in exactly one -- the trailer's own 32 positions are overwritten by
    # the recomputation, so the decoder is handed the original bytes and returns the
    # original store, which is accepted but as IDENTICAL data and not as different
    original = persistence.decode(blob)
    different = identical = 0
    reasons = {}
    for offset in range(len(blob)):
        for bit in range(8):
            flipped = bytearray(blob)
            flipped[offset] ^= 1 << bit
            flipped[-4:] = struct.pack("<I", zlib.crc32(bytes(flipped[:-4])))
            try:
                loaded = persistence.decode(bytes(flipped))
            except persistence.SnapshotError as exc:
                why = _why_refused(str(exc))
                reasons[why] = reasons.get(why, 0) + 1
                continue
            if list(loaded.snapshot_items()) == list(original.snapshot_items()):
                identical += 1
            else:
                different += 1
    return different, identical, reasons


def test_the_exhaustive_bit_flip_shares_the_readme_publishes():
    # the README's flagship finding. before this, its shares came from 3,000 random trials
    # that no file here reproduced, and the three figures it quoted could not describe one
    # fixture: 475 + 48 + 5 is 17.60% of 3,000 against a stated 19%, and the 80.9% it gave
    # as the shape's own exhaustive share is reachable only by a key set with zero
    # duplicate-key catches, which 5 such catches contradicts
    blob = _ordinary41()
    assert len(blob) == 2036, ("the fixture the figures belong to is 2,036 bytes", len(blob))
    different, identical, reasons = _sweep_with_the_trailer_recomputed(blob)
    caught = sum(reasons.values())
    assert (different, identical, caught) == (12942, 32, 3314), (
        "the published shares are 79.4573% accepted as different data, 0.1965% accepted as "
        "the original store and 20.3463% caught", different, identical, caught, reasons)
    assert different + identical + caught == 2036 * 8, (
        "every flip has to land in exactly one of the three outcomes")
    assert identical == 32, (
        "the 32 accepted-identical positions are the four bytes of the CRC trailer, which "
        "the recomputation overwrites -- the outcome the README used to leave unnamed")
    # the breakdown has to sum to the whole, which is what the published 475/48/5 did not
    assert sum(reasons.values()) == caught, (reasons, caught)
    assert reasons == {
        "length prefix off the end": 2759,
        "type byte naming no kind": 280,
        "duplicate key": 202,
        "unsupported version": 32,
        "wrong magic": 32,
        "trailing bytes": 8,
        "empty list": 1,
    }, ("the itemised refusal reasons the README now publishes", reasons)


def test_the_bit_flip_maximum_is_a_key_choice_and_not_the_shapes_figure():
    # 80.8939% is reached only where no single flip in a key can produce a key another
    # entry already holds, and such a fixture has ZERO duplicate-key refusals. the README
    # used to publish that share beside "5 by a key arriving twice", which cannot both be
    # true of one blob -- every duplicate-key catch comes one-for-one out of the accepted
    # pool, so the maximum is exactly the fixture with none
    blob, _keys = _separated41()
    assert len(blob) == 2036, ("the maximum is quoted at the same size", len(blob))
    different, identical, reasons = _sweep_with_the_trailer_recomputed(blob)
    caught = sum(reasons.values())
    assert (different, identical, caught) == (13144, 32, 3112), (
        "the published maximum is 80.6974% accepted with 19.1061% caught",
        different, identical, caught, reasons)
    assert reasons.get("duplicate key", 0) == 0, (
        "the maximum is the fixture with no duplicate-key catches, which is why it is a "
        "maximum over key choices and not a property of the shape", reasons)
    # the law the two fixtures share: the accepted-different count plus the duplicate-key
    # count is the payload bits, so the accepted share IS the payload byte share
    ordinary_different, _, ordinary_reasons = _sweep_with_the_trailer_recomputed(_ordinary41())
    assert ordinary_different + ordinary_reasons["duplicate key"] == different, (
        "payload bits are the same for both fixtures, so the duplicate-key catches are "
        "exactly what the ordinary fixture loses from the accepted pool",
        ordinary_different, ordinary_reasons.get("duplicate key"), different)


def test_the_three_key_fixtures_accepted_share_is_exactly_half():
    # the README quotes 50.0% here, and it is exact rather than rounded: the fixture is 102
    # bytes of which exactly 51 are payload
    blob, _original = _fixture_blob()
    assert len(blob) == 102, len(blob)
    different, identical, reasons = _sweep_with_the_trailer_recomputed(blob)
    assert different == 408 and different * 2 == len(blob) * 8, (
        "exactly half of the bit positions are accepted as different data",
        different, len(blob) * 8)
    assert identical == 32, identical
    assert sum(reasons.values()) == 376, reasons
