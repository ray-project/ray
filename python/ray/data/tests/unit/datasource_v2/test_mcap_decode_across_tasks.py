"""Unit tests for decoded MCAP reads split across tasks.

A task that owns part of a video stream primes its decoder from a lead-in,
feeds it the frames other tasks own inside its span, reads the stream's
parameter sets where it starts when its first keyframe lacks them, and tells
``fps`` thinning about the pictures just before and after its own, so a split
read keeps the frames a whole-file read keeps. No Ray cluster: the indexer,
scanner and reader are driven directly.
"""

import dataclasses
import importlib.util
import io
import os
from fractions import Fraction
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap import (
    mcap_chunks,
    mcap_datasource_v2,
    mcap_decode,
    mcap_decoded_messages,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import FrameDecoder
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    TimeRange,
    VideoOptions,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    detect_codec,
    is_keyframe,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    FRAME_NS,
    FRAME_SHAPE,
    GOP,
    H264_FILE_FRAMES,
    LONG_SLOT_STILLS,
    SECOND,
    START_CODE_SECOND,
    VP9_KEYFRAME,
    VP9_PFRAME,
    LateContext,
    SeekLog,
    UnstampedAtFlushContext,
    camera_times,
    cdr_video,
    centred_nearest,
    chunks_read,
    encode_h264,
    encode_h265,
    infer_schema,
    length_prefixed,
    list_manifests,
    listed_if_custom,
    manifest_for_frames,
    pack,
    parameter_sets_once,
    read_all,
    read_rows,
    scanner_for,
    split_after,
    split_at_first_idr,
    summary_of,
    write_payloads,
    write_stills,
    write_two_cameras,
)

pytestmark = [
    pytest.mark.skipif(
        importlib.util.find_spec("mcap") is None,
        reason="mcap module not available. Install with: pip install mcap",
    ),
    pytest.mark.skipif(
        importlib.util.find_spec("av") is None,
        reason="av not available. Install with: pip install av",
    ),
]


def test_decode_primes_a_task_that_starts_mid_gop(h264_file):
    """Tasks that start mid-GOP read back to the keyframe before them, so three
    tasks decode the same frames as a whole-file read."""
    datasource = MCAPDatasourceV2([h264_file], video=VideoOptions())
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    n = len(manifest)
    assert n >= 3
    # Chunks 0-1, 2-3 and 4-5: the second task starts at frame 8, the third at 21.
    parts = [
        FileManifest(block.slice(0, n // 3)),
        FileManifest(block.slice(n // 3, n // 3)),
        FileManifest(block.slice(2 * (n // 3))),
    ]

    whole = read_rows(datasource, [manifest])
    split = read_rows(datasource, parts)

    assert sorted(r["sequence"] for r in split) == sorted(r["sequence"] for r in whole)
    assert len(split) == H264_FILE_FRAMES
    by_seq = {r["sequence"]: r["frame"] for r in whole}
    for row in split:
        assert (row["frame"] == by_seq[row["sequence"]]).all()


def test_each_video_channel_gets_its_own_lead_in(tmp_path):
    """A task owning frames 26-29 of both cameras primes each from its own
    keyframe. ``/b`` runs eight frames behind ``/a``, so its keyframe (frame 20)
    comes after ``/a``'s first owned frame, in chunks the task does not own."""
    path = os.path.join(tmp_path, "two.mcap")
    channels = write_two_cameras(path, offset_frames=8)
    datasource = MCAPDatasourceV2([path], video=VideoOptions())
    (manifest,) = list_manifests(datasource)
    summary = summary_of(path)
    assert len(summary.chunk_indexes) == 60

    def frame_of(chunk):
        """The channel and frame of a chunk, which holds one message."""
        (channel_id,) = chunk.message_index_offsets
        shift = 8 if channel_id == channels["/b"] else 0
        return channel_id, chunk.message_start_time // FRAME_NS - shift

    owned_offsets = {
        c.chunk_start_offset for c in summary.chunk_indexes if frame_of(c)[1] >= 26
    }
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    rows = [
        i
        for i, md in enumerate(manifest.file_chunk_metadatas)
        if md is not None and int(md["unit_ids"][0]) in owned_offsets
    ]
    assert len(rows) == 8
    part = FileManifest(block.take(rows))

    split = read_rows(datasource, [part])

    assert sorted((r["topic"], r["sequence"]) for r in split) == [
        (topic, seq) for topic in ("/a", "/b") for seq in range(26, 30)
    ]
    whole = read_rows(datasource, [manifest])
    reference = {(r["topic"], r["sequence"]): r["frame"] for r in whole}
    for row in split:
        assert (row["frame"] == reference[(row["topic"], row["sequence"])]).all()


class HeldBackDecoder(FrameDecoder):
    """A decoder that releases every frame one packet late, as a real one may."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._held = []

    def decode(self, message, wanted=None):
        ready, self._held = self._held, list(super().decode(message, wanted))
        yield from ready

    def flush(self):
        yield from self._held
        self._held = []
        yield from super().flush()


@pytest.mark.parametrize("held_back", [False, True], ids=["prompt", "held-back"])
def test_gaps_between_owned_chunks_are_fed_to_the_decoder(
    tmp_path, monkeypatch, held_back
):
    """Frames another task owns in the middle of a channel's stream are fed to
    the decoder, so the frames after the gap decode correctly. A frame of ours
    that the decoder held back across the gap is still emitted."""
    if held_back:
        monkeypatch.setattr(mcap_decoded_messages, "FrameDecoder", HeldBackDecoder)
    path = os.path.join(tmp_path, "gaps.mcap")
    channels = write_two_cameras(path, offset_frames=0)
    datasource = MCAPDatasourceV2([path], topics=["/a"], video=VideoOptions())

    # A gap inside a GOP (frames 15-16) and one that spans keyframes (frames 10-21).
    for owned_frames in (
        list(range(0, 15)) + list(range(17, 30)),
        list(range(0, 10)) + list(range(22, 30)),
    ):
        part, whole_manifest = manifest_for_frames(
            datasource, path, channels, 0, {("/a", i) for i in owned_frames}
        )
        rows = read_rows(datasource, [part])
        assert [r["sequence"] for r in rows] == owned_frames
        whole = read_rows(datasource, [whole_manifest])
        reference = {r["sequence"]: r["frame"] for r in whole}
        for row in rows:
            assert (row["frame"] == reference[row["sequence"]]).all()


# Frame spacing of the ``write_chunks`` recordings: 100 ms.
TENTH_S = SECOND // 10


def write_chunks(
    path: str,
    packets: List[bytes],
    chunks: List[List[int]],
    log_times: List[int],
    pad_times: Optional[Dict[int, int]] = None,
) -> None:
    """Write ``packets[i]`` on ``/camera`` at ``log_times[i]``, chunk by chunk.

    ``chunks`` lists each chunk's frames in write order. A message on ``/pad``
    larger than the chunk size closes each chunk. It is logged at the chunk's
    latest frame, so the chunk spans only its frames, unless ``pad_times``
    moves the pad of chunk ``k`` to ``pad_times[k]``.
    """
    from mcap.writer import CompressionType, Writer

    chunk_size = 1 << 16
    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=chunk_size, compression=CompressionType.ZSTD)
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name="foxglove.CompressedVideo", encoding="ros2msg", data=b"video\n"
        )
        camera = writer.register_channel(
            schema_id=schema_id, topic="/camera", message_encoding="cdr"
        )
        pad = writer.register_channel(schema_id=0, topic="/pad", message_encoding="cdr")
        for k, frames in enumerate(chunks):
            for i in frames:
                writer.add_message(
                    channel_id=camera,
                    log_time=log_times[i],
                    publish_time=log_times[i],
                    data=packets[i],
                    sequence=i,
                )
            pad_time = (pad_times or {}).get(k, max(log_times[i] for i in frames))
            writer.add_message(
                channel_id=pad,
                log_time=pad_time,
                publish_time=pad_time,
                data=bytes(chunk_size),
            )
        writer.finish()


@pytest.mark.parametrize("fps", [None, 2], ids=["every-frame", "fps"])
def test_frames_another_task_owns_inside_an_owned_chunk_are_fed(tmp_path, fps):
    """Frame 15 was written late, into the next chunk, so frames 14 and 16 share
    a chunk without it. The task owning that chunk still feeds frame 15 to the
    decoder and to ``fps`` thinning, so a split read matches a whole-file read."""
    path = os.path.join(tmp_path, "late_frame.mcap")
    chunks = [list(range(15)) + list(range(16, 20)), [15] + list(range(20, 30))]
    # Frames 100 ms apart: with fps=2, frame 15 is the centre of the slot
    # holding frames 13 to 17, so it beats its equally near neighbours 14 and 16.
    write_chunks(path, encode_h264(30), chunks, [i * TENTH_S for i in range(30)])
    datasource = MCAPDatasourceV2(
        [path], topics=["/camera"], video=VideoOptions(fps=fps)
    )
    (manifest,) = list_manifests(datasource)
    assert len(manifest) == 2

    whole = read_rows(datasource, [manifest])
    kept = [r["sequence"] for r in whole]
    assert kept == (list(range(30)) if fps is None else [0, 5, 10, 15, 20, 25, 29])
    split = read_rows(datasource, split_after(manifest, 1))

    assert sorted(r["sequence"] for r in split) == kept
    reference = {r["sequence"]: r["frame"] for r in whole}
    for row in split:
        assert (row["frame"] == reference[row["sequence"]]).all()
    counted = read_rows(datasource, split_after(manifest, 1), columns=["sequence"])
    assert sorted(r["sequence"] for r in counted) == kept


def test_pruned_projection_counts_the_frames_a_decoder_would_emit(
    h264_file, monkeypatch
):
    """A read without ``frame``, as in ``count()``, yields the same rows as a full
    read. With no keyframe in reach, both skip frames until the next keyframe."""
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0.05")
    datasource = MCAPDatasourceV2([h264_file], video=VideoOptions())
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    summary = summary_of(h264_file)
    # Chunks 2 and 3 start mid-GOP, at frames 8 and 15, over 50 ms after keyframe 0.
    rows = [
        i
        for i, c in enumerate(summary.chunk_indexes)
        if 2 <= c.message_start_time // FRAME_NS < 20
    ]
    assert rows
    part = FileManifest(block.take(rows))

    decoded = read_rows(datasource, [part])
    counted = read_rows(datasource, [part], columns=["sequence"])

    assert [r["sequence"] for r in decoded] == [r["sequence"] for r in counted]
    assert decoded and all(r["sequence"] >= 10 for r in decoded)


def test_lead_in_parameter_sets_do_not_take_an_fps_slot(tmp_path):
    """A parameter-set message in the lead-in is not a frame, so it must not take
    the ``fps`` slot of the owned keyframe just after it."""
    packets = encode_h264(10)
    parameter_sets, keyframe = split_at_first_idr(packets[0])
    path = os.path.join(tmp_path, "ps.mcap")
    # Parameter sets at 10 ms and the keyframe at 20 ms share the 100 ms slot
    # centred on 0. Taken for a frame, the parameter sets would be nearer.
    log_times = [10_000_000, 20_000_000] + [i * 100_000_000 for i in range(1, 10)]
    write_payloads(
        path,
        [parameter_sets, keyframe] + packets[1:],
        schema_name="foxglove.CompressedVideo",
        chunk_size=1,
        log_times=log_times,
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=10))
    (manifest,) = list_manifests(datasource)

    whole = read_rows(datasource, [manifest])
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    # A task owning everything but the parameter-set message, which is its lead-in.
    split = read_rows(datasource, [FileManifest(block.slice(1))])

    assert [r["sequence"] for r in whole] == list(range(1, 11))
    assert [r["sequence"] for r in split] == [r["sequence"] for r in whole]


def test_gap_longer_than_the_lead_in_goes_cold_until_a_keyframe(tmp_path, monkeypatch):
    """A gap longer than the look-back cap, with no keyframe in the span read
    back, leaves the decoder without references. The frames after it are
    skipped until the next keyframe."""
    path = os.path.join(tmp_path, "longgap.mcap")
    channels = write_two_cameras(path, offset_frames=0)
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0.1")
    datasource = MCAPDatasourceV2([path], topics=["/a"], video=VideoOptions())
    # Another task owns frames 5-17, and the 100 ms look-back holds no keyframe.
    owned_frames = list(range(0, 5)) + list(range(18, 30))
    part, whole_manifest = manifest_for_frames(
        datasource, path, channels, 0, {("/a", i) for i in owned_frames}
    )

    rows = read_rows(datasource, [part])

    expected = list(range(0, 5)) + list(range(20, 30))
    assert [r["sequence"] for r in rows] == expected
    whole = read_rows(datasource, [whole_manifest])
    reference = {r["sequence"]: r["frame"] for r in whole}
    for row in rows:
        assert (row["frame"] == reference[row["sequence"]]).all()
    counted = read_rows(datasource, [part], columns=["sequence"])
    assert [r["sequence"] for r in counted] == expected


def test_zero_look_back_still_goes_cold_after_a_skipped_span(tmp_path):
    """With a zero look-back cap, a span of frames owned by another task still
    leaves the decoder without references. Our frames after it are dropped until
    the next keyframe, with or without ``frame``."""
    path = os.path.join(tmp_path, "nolookback.mcap")
    channels = write_two_cameras(path, offset_frames=0)
    datasource = MCAPDatasourceV2([path], topics=["/a"], video=VideoOptions())
    owned_frames = list(range(0, 5)) + list(range(17, 30))
    part, _ = manifest_for_frames(
        datasource, path, channels, 0, {("/a", i) for i in owned_frames}
    )
    scanner = scanner_for(datasource)
    scanner = dataclasses.replace(scanner, max_lead_in_ns=0)
    expected = list(range(0, 5)) + list(range(20, 30))  # 17-19 follow a skipped span

    def sequences(s):
        """The sequences a read of ``part`` with scanner ``s`` yields."""
        return [
            row["sequence"]
            for table in s.create_reader().read(part)
            for row in table.to_pylist()
        ]

    assert sequences(scanner) == expected
    assert sequences(scanner.prune_columns(["sequence"])) == expected


def test_cold_frames_still_count_for_fps(h264_file):
    """Frames skipped while a channel is cold still count for ``fps``, so a split
    read keeps no frame that the whole-file read drops."""
    # 300 ms slots: cold frame 9 (297 ms) is nearer the centre than keyframe 10.
    datasource = MCAPDatasourceV2([h264_file], video=VideoOptions(fps=Fraction(10, 3)))
    (manifest,) = list_manifests(datasource)
    whole = read_rows(datasource, [manifest])
    assert [r["sequence"] for r in whole] == [0, 9, 18, 27]
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    scanner = scanner_for(datasource)
    scanner = dataclasses.replace(scanner, max_lead_in_ns=50_000_000)
    # The task owns chunks 2-5, from frame 8, with keyframe 0 out of reach.
    part = FileManifest(block.slice(2))
    # Frame 9 keeps its slot but is cold, so keyframe 10 must not take it.
    expected = [18, 27]

    for projection in (scanner, scanner.prune_columns(["sequence"])):
        rows = [
            row["sequence"]
            for table in projection.create_reader().read(part)
            for row in table.to_pylist()
        ]
        assert rows == expected


def test_time_gap_without_skipped_messages_keeps_decoding(tmp_path):
    """A pause longer than the look-back cap skips no messages, so the decoder
    keeps its state and the frames after the pause decode."""
    log_times = [i * FRAME_NS for i in range(30)]
    for i in range(15, 30):
        log_times[i] += 20 * SECOND  # a 20 s pause mid-GOP
    path = os.path.join(tmp_path, "pause.mcap")
    write_payloads(
        path,
        encode_h264(30),
        schema_name="foxglove.CompressedVideo",
        chunk_size=1,
        log_times=log_times,
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions())
    (manifest,) = list_manifests(datasource)
    whole = read_rows(datasource, [manifest])
    assert [r["sequence"] for r in whole] == list(range(30))
    block = manifest.as_block()
    assert isinstance(block, pa.Table)

    # A task owning frames 12-19, one chunk per frame, straddles the pause.
    rows = read_rows(datasource, [FileManifest(block.slice(12, 8))])

    assert [r["sequence"] for r in rows] == list(range(12, 20))
    reference = {r["sequence"]: r["frame"] for r in whole}
    for row in rows:
        assert (row["frame"] == reference[row["sequence"]]).all()


def test_pause_across_another_tasks_chunk_keeps_decoding(tmp_path):
    """A pause longer than the look-back cap skips no frame, even where another
    task's chunk spans the pause because another topic fills it. The task owning
    the chunks on both sides keeps decoding, as a whole-file read does."""
    log_times = [i * TENTH_S for i in range(30)]
    for i in range(15, 30):
        log_times[i] += 20 * SECOND  # a 20 s pause mid-GOP, twice the default cap
    chunks = [list(range(15)), [15], list(range(16, 30))]
    path = os.path.join(tmp_path, "pause.mcap")
    # Chunk 1's /pad message is logged where the pause starts, so that chunk
    # spans the pause though its one camera frame comes after it.
    pause_start = log_times[14] + TENTH_S
    write_chunks(path, encode_h264(30), chunks, log_times, pad_times={1: pause_start})
    datasource = MCAPDatasourceV2([path], topics=["/camera"], video=VideoOptions())
    (manifest,) = list_manifests(datasource)
    assert len(manifest) == 3
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    # This task owns chunks 0 and 2; another task owns chunk 1.
    part = FileManifest(block.take([0, 2]))

    rows = read_rows(datasource, [part])

    owned = list(range(15)) + list(range(16, 30))
    assert [r["sequence"] for r in rows] == owned
    whole = read_rows(datasource, [manifest])
    reference = {r["sequence"]: r["frame"] for r in whole}
    for row in rows:
        assert (row["frame"] == reference[row["sequence"]]).all()
    counted = read_rows(datasource, [part], columns=["sequence"])
    assert [r["sequence"] for r in counted] == owned


@pytest.mark.parametrize("columns", [None, ["log_time"]], ids=["decoded", "pruned"])
def test_fps_slot_longer_than_the_look_back_does_not_depend_on_the_split(
    tmp_path, columns
):
    """With 20 s ``fps`` slots and the default 10 s look-back, a task that starts
    at 25.5 s, after a pause, learns from the message index that the still at
    15 s is nearer their 20 s centre, so it keeps none of its own there."""
    path = os.path.join(tmp_path, "pause.mcap")
    write_stills(path, LONG_SLOT_STILLS)
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=0.05))
    (manifest,) = list_manifests(datasource)

    whole = read_rows(datasource, [manifest], columns)
    split = read_rows(datasource, split_after(manifest, 1), columns)

    assert [r["log_time"] for r in whole] == [15 * SECOND, 32_500_000_000]
    assert [r["log_time"] for r in split] == [r["log_time"] for r in whole]


@pytest.mark.parametrize("columns", [None, ["log_time"]], ids=["decoded", "pruned"])
def test_fps_seed_covers_a_gap_longer_than_the_look_back(tmp_path, columns):
    """A task owns the stills at 18 s and from 47 s on, another task those at
    21 s and 35 s, beyond the 10 s look-back. From the message index the task
    learns that 21 s is nearer its 20 s slot centre than 18 s, and 35 s nearer
    the 40 s one than 47 s, so it keeps neither."""
    path = os.path.join(tmp_path, "gap.mcap")
    write_stills(path, [18, 21, 35] + list(range(47, 54)))
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=0.05))
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    ours = FileManifest(block.take([0] + list(range(3, 10))))
    theirs = FileManifest(block.slice(1, 2))

    whole = read_rows(datasource, [manifest], columns)
    split = read_rows(datasource, [ours, theirs], columns)

    assert [r["log_time"] for r in whole] == [21 * SECOND, 35 * SECOND, 53 * SECOND]
    assert sorted(r["log_time"] for r in split) == [r["log_time"] for r in whole]


@pytest.mark.parametrize("columns", [None, ["log_time"]], ids=["decoded", "pruned"])
def test_fps_notes_unread_gap_pictures_before_those_read_back(tmp_path, columns):
    """At a gap past the look-back, the task notes the unread still after its
    waiting frame before the stills it reads back. The unread 21 s is nearer
    the 20 s centre than our 18 s; the 26 s read back is not."""
    path = os.path.join(tmp_path, "gap_order.mcap")
    write_stills(path, [18, 21, 26] + list(range(35, 42)))
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=0.05))
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    # Ours: 18 s and from 35 s on. The look-back from 35 s reads back 26 s only.
    ours = FileManifest(block.take([0] + list(range(3, 10))))
    theirs = FileManifest(block.slice(1, 2))

    whole = read_rows(datasource, [manifest], columns)
    split = read_rows(datasource, [ours, theirs], columns)

    assert [r["log_time"] for r in whole] == [21 * SECOND, 40 * SECOND]
    assert sorted(r["log_time"] for r in split) == [r["log_time"] for r in whole]


def test_a_frame_released_unstamped_at_the_end_is_thinned(tmp_path, monkeypatch):
    """A frame the decoder releases without a stamp when the task ends takes the
    time of the last packet fed, and ``fps`` thins it like any other.

    The 30 frames fall in the 1 s slots centred on 0 s and 1 s, which keep
    frames 0 and 29. Frame 29 is the one released unstamped.
    """
    from ray.data._internal.datasource_v2.formats.mcap import mcap_decode

    real_context = mcap_decode._codec_context
    monkeypatch.setattr(
        mcap_decode,
        "_codec_context",
        lambda codec: UnstampedAtFlushContext(real_context(codec)),
    )
    path = os.path.join(tmp_path, "h264.mcap")
    write_payloads(path, encode_h264(30), schema_name="foxglove.CompressedVideo")
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=1))

    rows = read_rows(datasource, list_manifests(datasource))

    assert [row["sequence"] for row in rows] == [0, 29]


def test_a_frame_released_late_at_a_gap_keeps_its_slot_decision(tmp_path, monkeypatch):
    """A task's frame before a gap that the decoder releases late is decided by
    log times alone, so a split read keeps the frames a whole-file read keeps."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_decode

    real_context = mcap_decode._codec_context
    monkeypatch.setattr(
        mcap_decode, "_codec_context", lambda codec: LateContext(real_context(codec))
    )
    # Frame 9, released late, is the nearest to the 20 s slot centre. The other
    # task owns GOP 10-19 from 41 s. Our GOP 20-29 follows a pause, from 55 s.
    seconds = (
        [10 + i / 10 for i in range(9)]
        + [20.5]
        + [41 + i / 10 for i in range(10)]
        + [55 + i / 10 for i in range(10)]
    )
    path = os.path.join(tmp_path, "late.mcap")
    write_payloads(
        path,
        encode_h264(30),
        schema_name="foxglove.CompressedVideo",
        chunk_size=1,
        log_times=[round(s * SECOND) for s in seconds],
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=0.05))
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    ours = FileManifest(block.take(list(range(10)) + list(range(20, 30))))
    theirs = FileManifest(block.slice(10, 10))

    whole = read_rows(datasource, [manifest])
    split = read_rows(datasource, [ours, theirs])

    assert [r["sequence"] for r in whole] == [9, 10, 29]
    assert sorted(r["sequence"] for r in split) == [9, 10, 29]


def has_encoder(name):
    """Whether this build of ``av`` has the encoder ``name``."""
    import av

    try:
        av.codec.Codec(name, "w")
    except Exception:  # noqa: BLE001 - PyAV raises its own hierarchy
        return False
    return True


def encode_av1(num_frames, width=64, height=64):
    """AV1 temporal units from SVT-AV1, one per frame, a keyframe every ``GOP``."""
    import av
    import numpy as np

    context = av.CodecContext.create("libsvtav1", "w")
    context.width, context.height, context.pix_fmt = width, height, "yuv420p"
    context.time_base = Fraction(1, 30)
    context.framerate = Fraction(30, 1)
    context.gop_size = GOP
    context.max_b_frames = 0
    # Low-delay prediction: each frame refers only to earlier ones.
    context.options = {"preset": "12", "svtav1-params": f"keyint={GOP}:pred-struct=1"}
    packets = []
    for i in range(num_frames):
        array = np.full((height, width, 3), i * 8 % 256, dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(array, format="rgb24").reformat(
            format="yuv420p"
        )
        frame.pts = i
        packets.extend(bytes(p) for p in context.encode(frame))
    packets.extend(bytes(p) for p in context.encode(None))
    return packets


@pytest.mark.parametrize("gap", ["primed", "cold"])
def test_frames_held_at_a_gap_keep_their_fps_slot(tmp_path, monkeypatch, gap):
    """Frames of ours that the AV1 decoder still holds at a gap keep their ``fps``
    slots, so a split read keeps the frames a whole-file read keeps."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_decode

    if not has_encoder("libsvtav1"):
        pytest.skip("this build of av has no AV1 encoder")
    real_context = mcap_decode._codec_context
    # Releasing three packets late keeps at least frames 6-8 in the decoder at the gap.
    monkeypatch.setattr(
        mcap_decode,
        "_codec_context",
        lambda codec: LateContext(real_context(codec), delay=3),
    )
    packets = encode_av1(30)
    keyframes = [i for i, p in enumerate(packets) if is_keyframe(p, VideoCodec.AV1)]
    assert keyframes == [0, 10, 20]
    # Frame 8 is on the centre of its slot, at 5 s.
    seconds = [0.5 * (i + 1) for i in range(7)] + [4.8, 5.0, 5.2, 5.4, 5.5]
    seconds += [5.55, 5.6, 5.65] + [5.7 + 0.2 * i for i in range(15)]
    path = os.path.join(tmp_path, "av1.mcap")
    write_payloads(
        path,
        packets,
        schema_name="foxglove.CompressedVideo",
        chunk_size=1,
        log_times=[round(s * SECOND) for s in seconds],
    )
    if gap == "primed":
        # Another task owns 9-11. The gap is fed from keyframe 10, so 9 (5.2 s)
        # is noted for fps while frame 8 is still inside the decoder.
        theirs = list(range(9, 12))
        cold = set()
    else:
        # Another task owns 9-14. The look-back reaches only 13 and 14, so the
        # channel goes cold and skips 15-19 until keyframe 20.
        monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0.12")
        theirs = list(range(9, 15))
        cold = set(range(15, 20))
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=1))
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    ours = [i for i in range(30) if i not in theirs]

    whole = read_rows(datasource, [manifest])
    split = read_rows(
        datasource, [FileManifest(block.take(ours)), FileManifest(block.take(theirs))]
    )

    kept = [r["sequence"] for r in whole]
    assert 8 in kept
    assert sorted(r["sequence"] for r in split) == [s for s in kept if s not in cold]


def test_fps_without_message_indexes_sees_no_further_than_the_task(tmp_path):
    """A file written without message indexes still reads. A task then sees no
    picture after its own, so the one ending on 19 s keeps it, though 20 s, in
    the next task, is nearer the slot's centre."""
    from mcap.writer import IndexType

    path = os.path.join(tmp_path, "no_message_index.mcap")
    write_stills(
        path,
        [1] + list(range(15, 23)),
        index_types=IndexType.ATTACHMENT | IndexType.CHUNK | IndexType.METADATA,
    )
    summary = summary_of(path)
    assert summary.chunk_indexes
    assert not any(c.message_index_offsets for c in summary.chunk_indexes)
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=0.05))
    (manifest,) = list_manifests(datasource)

    whole = read_rows(datasource, [manifest])
    # The first task owns the stills at 1 s and 15-19 s.
    split = read_rows(datasource, split_after(manifest, 6))

    assert [r["log_time"] for r in whole] == [1 * SECOND, 20 * SECOND]
    assert [r["log_time"] for r in split] == [1 * SECOND, 19 * SECOND, 20 * SECOND]


@pytest.mark.parametrize(
    "schema_name",
    ["foxglove.CompressedVideo", "custom_msgs/msg/Frame"],
    ids=["video-schema", "custom-schema"],
)
def test_mid_gop_task_tells_the_codec_from_its_lead_in(tmp_path, schema_name):
    """A task that starts on a VP9 inter frame takes the codec from the keyframe in
    its lead-in. With no keyframe in reach it goes cold instead of failing, also
    for a listed topic with a custom schema name."""
    payloads = [VP9_KEYFRAME if i % GOP == 0 else VP9_PFRAME for i in range(30)]
    assert detect_codec(VP9_PFRAME) is None
    path = os.path.join(tmp_path, "vp9.mcap")
    write_payloads(path, payloads, schema_name=schema_name, chunk_size=1)
    listed = listed_if_custom(schema_name)
    datasource = MCAPDatasourceV2([path], video=VideoOptions(), video_topics=listed)
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)

    # A decoder rejects the synthetic frames, so count the rows with ``frame`` pruned.
    counted = read_rows(
        datasource, [FileManifest(block.slice(12, 8))], columns=["sequence"]
    )
    assert [r["sequence"] for r in counted] == list(range(12, 20))
    read_rows(datasource, [FileManifest(block.slice(12, 8))])  # must not raise

    # With keyframe 10 out of reach the codec is unknown: cold until keyframe 20.
    cold = MCAPDatasourceV2([path], video=VideoOptions(), video_topics=listed)
    scanner = scanner_for(cold)
    scanner = dataclasses.replace(scanner, max_lead_in_ns=50_000_000)
    rows = [
        row
        for table in scanner.prune_columns(["sequence"])
        .create_reader()
        .read(FileManifest(block.slice(12, 14)))
        for row in table.to_pylist()
    ]
    assert [r["sequence"] for r in rows] == list(range(20, 26))


def test_a_kept_still_that_does_not_decode_leaves_its_slot_empty(tmp_path):
    """The frame ``fps`` keeps is chosen from log times, so a corrupt one leaves
    its slot empty: no neighbour takes it, in a whole-file or a split read.

    With ``fps=10``, frame 3 (99 ms) is nearest the 100 ms slot centre and is
    corrupt. Frame 4 shares its slot and is the first frame of the second task.
    """
    import numpy as np
    from PIL import Image

    payloads = []
    for i in range(6):
        buffer = io.BytesIO()
        Image.fromarray(np.full(FRAME_SHAPE, i * 20, dtype=np.uint8)).save(
            buffer, format="JPEG"
        )
        payloads.append(buffer.getvalue())
    payloads[3] = payloads[3][:40]
    path = os.path.join(tmp_path, "corrupt_fps.mcap")
    write_payloads(
        path, payloads, schema_name="sensor_msgs/msg/CompressedImage", chunk_size=1
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=10))
    (manifest,) = list_manifests(datasource)

    def frames(rows):
        """The frame number of each row."""
        return [row["log_time"] // FRAME_NS for row in rows]

    whole = read_rows(datasource, [manifest])
    # The second task owns frames 4 and 5.
    split = read_rows(datasource, split_after(manifest, 4))

    assert frames(whole) == frames(split) == [0, 5]


# -- fps slots across tasks ----------------------------------------------------

# A 30 fps camera from a random start on 2026-10-10, with 3 ms of capture
# jitter: 45 frames, one per chunk.
JITTERED_FRAMES = 45
JITTERED_START = 1_791_590_400 * SECOND + 123_456_789
NTSC = Fraction(30000, 1001)


def write_jittered_camera(path: str) -> List[int]:
    """Write the jittered camera's H.264 frames, one per chunk, and return their
    log times."""
    times = camera_times(
        Fraction(30), JITTERED_FRAMES, JITTERED_START, jitter_ns=3_000_000, seed=11
    )
    write_payloads(
        path,
        encode_h264(JITTERED_FRAMES),
        schema_name="foxglove.CompressedVideo",
        chunk_size=1,
        log_times=times,
    )
    return times


def kept_frames(rows) -> Dict[int, Any]:
    """Each kept frame by log time, ``None`` when the frames are pruned."""
    return {row["log_time"]: row.get("frame") for row in rows}


@pytest.mark.parametrize("fps", [Fraction(15), NTSC / 2], ids=["15", "ntsc-half"])
@pytest.mark.parametrize("columns", [None, ["log_time"]], ids=["decoded", "pruned"])
def test_every_split_keeps_the_frames_of_the_whole_read(
    tmp_path, monkeypatch, columns, fps
):
    """A jittered camera keeps the frames ``centred_nearest`` names whether it is
    read whole, as one task per chunk packed as the datasource packs it, or as
    two tasks cut at any chunk. Each kept frame decodes the same."""
    import numpy as np

    path = os.path.join(tmp_path, "jittered.mcap")
    times = write_jittered_camera(path)
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=fps))
    (manifest,) = list_manifests(datasource)
    monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", 1)
    one_per_chunk = pack(datasource)
    assert len(one_per_chunk) == JITTERED_FRAMES

    whole = kept_frames(read_rows(datasource, [manifest], columns))
    splits = [one_per_chunk] + [
        split_after(manifest, cut) for cut in range(1, JITTERED_FRAMES)
    ]

    assert sorted(whole) == [times[i] for i in centred_nearest(times, fps)]
    for tasks in splits:
        split = kept_frames(read_rows(datasource, tasks, columns))
        assert sorted(split) == sorted(whole)
        for log_time, frame in split.items():
            assert np.array_equal(frame, whole[log_time])


@pytest.mark.parametrize("pad_after_ms", [0, 50], ids=["pad-at-frame", "pad-after"])
@pytest.mark.parametrize(
    "owned, index_reads",
    # Frame 3 waits for the next picture to settle its slot centred on 0.5 s,
    # and frame 7, past that centre, does not.
    [(4, 1), (8, 0)],
)
def test_a_task_looks_ahead_only_for_a_frame_waiting_for_its_slot(
    tmp_path, monkeypatch, owned, index_reads, pad_after_ms
):
    """A task ending on a frame before its slot's centre reads one message
    index, the next chunk's, for the picture after it. One ending past the
    centre reads none. A ``/pad`` message after the frame stretches the task's
    own last chunk past it, and that chunk is not searched: the task read it."""
    path = os.path.join(tmp_path, "tenths.mcap")
    times = [i * TENTH_S for i in range(30)]
    pads = {k: t + pad_after_ms * 1_000_000 for k, t in enumerate(times)}
    write_chunks(path, encode_h264(30), [[i] for i in range(30)], times, pads)
    datasource = MCAPDatasourceV2([path], topics=["/camera"], video=VideoOptions(fps=2))
    (manifest,) = list_manifests(datasource)
    task = split_after(manifest, owned)[0]
    reads = []
    real_read = mcap_chunks.read_message_index

    def counted_read(f, chunk_index, channel_id):
        reads.append(chunk_index.message_start_time)
        return real_read(f, chunk_index, channel_id)

    monkeypatch.setattr(mcap_chunks, "read_message_index", counted_read)
    rows = read_rows(datasource, [task])

    assert rows
    assert reads == [owned * TENTH_S] * index_reads


# -- parameter sets written once ----------------------------------------------

ENCODERS = {"h264": encode_h264, "h265": encode_h265}
# A 0.2 s look-back reaches back from frame 25 to keyframe 20, not to frame 0.
SHORT_LOOK_BACK_NS = 200_000_000


def write_parameter_sets_once(path, codec, keep_first=True):
    """Write a ``codec`` stream of ``H264_FILE_FRAMES`` frames, one per chunk,
    with its parameter sets on the first keyframe only, or nowhere."""
    packets = ENCODERS[codec](H264_FILE_FRAMES)
    write_payloads(
        path,
        parameter_sets_once(packets, codec, keep_first),
        schema_name="foxglove.CompressedVideo",
        chunk_size=1,
    )
    return path


@pytest.fixture(params=["h264", "h265"])
def parameter_sets_once_file(tmp_path, request):
    """An H.264 or H.265 stream whose parameter sets come with its first keyframe
    only, one frame per chunk.

        frame   0    1 .. 9    10   11 .. 19   20   21 .. 29
                K*   P .. P    K    P .. P     K    P .. P

    K* carries the parameter sets (SPS and PPS, and VPS for H.265) and K does
    not, as from an encoder that writes them once. A task that starts after
    frame 9 decodes from a K without them. Frame ``i`` is logged at
    ``i * FRAME_NS`` with sequence ``i``.
    """
    if not has_encoder({"h264": "libx264", "h265": "libx265"}[request.param]):
        pytest.skip(f"this build of av has no {request.param} encoder")
    path = os.path.join(tmp_path, f"{request.param}_once.mcap")
    return write_parameter_sets_once(path, request.param)


def one_task_per_chunk(manifest):
    """The listing manifest's chunks, each a task of its own."""
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    return [FileManifest(block.slice(i, 1)) for i in range(block.num_rows)]


def frames_by_log_time(rows):
    """``{log_time: frame}`` of decoded message rows."""
    return {row["log_time"]: row["frame"] for row in rows}


def test_parameter_sets_written_once_reach_every_task(parameter_sets_once_file):
    """A stream whose parameter sets come only with its first keyframe decodes
    every frame with one task per chunk, as a whole-file read does. A task that
    starts after them reads them where the stream starts."""
    path = parameter_sets_once_file
    datasource = MCAPDatasourceV2([path], video=VideoOptions())
    (manifest,) = list_manifests(datasource)

    whole = frames_by_log_time(read_rows(datasource, [manifest]))
    split = frames_by_log_time(read_rows(datasource, one_task_per_chunk(manifest)))

    assert sorted(split) == [i * FRAME_NS for i in range(H264_FILE_FRAMES)]
    for log_time, frame in split.items():
        assert (frame == whole[log_time]).all()


def test_time_range_after_parameter_sets_written_once(parameter_sets_once_file):
    """A time range that starts after the only keyframe with parameter sets plans
    the frame shape and decodes every frame in it, as a whole-file read does."""
    path = parameter_sets_once_file
    whole = MCAPDatasourceV2([path], video=VideoOptions())
    # Frame 12 decodes from keyframe 10, which lacks the parameter sets.
    ranged = MCAPDatasourceV2(
        [path],
        video=VideoOptions(),
        time_range=TimeRange(12 * FRAME_NS, H264_FILE_FRAMES * FRAME_NS),
    )

    truth = frames_by_log_time(read_rows(whole, list_manifests(whole)))
    rows = read_rows(ranged, list_manifests(ranged))

    assert infer_schema(ranged).field("frame").type.shape == FRAME_SHAPE
    assert [row["sequence"] for row in rows] == list(range(12, H264_FILE_FRAMES))
    for row in rows:
        assert (row["frame"] == truth[row["log_time"]]).all()


def test_unindexed_file_reads_its_parameter_sets_from_its_start(tmp_path):
    """A file without a chunk index whose parameter sets come with its first
    keyframe only decodes a time range that starts after it: the task reads them
    from the start of the file."""
    from mcap.writer import IndexType

    path = os.path.join(tmp_path, "unindexed_once.mcap")
    packets = parameter_sets_once(encode_h264(H264_FILE_FRAMES), "h264")
    write_payloads(
        path,
        packets,
        schema_name="foxglove.CompressedVideo",
        index_types=IndexType.NONE,
        use_chunking=False,
        use_statistics=False,
        use_summary_offsets=False,
    )
    whole = MCAPDatasourceV2([path], video=VideoOptions())
    ranged = MCAPDatasourceV2(
        [path],
        video=VideoOptions(),
        time_range=TimeRange(12 * FRAME_NS, H264_FILE_FRAMES * FRAME_NS),
    )

    truth = frames_by_log_time(read_rows(whole, list_manifests(whole)))
    rows = read_rows(ranged, list_manifests(ranged))

    assert [row["sequence"] for row in rows] == list(range(12, H264_FILE_FRAMES))
    for row in rows:
        assert (row["frame"] == truth[row["log_time"]]).all()


def chunks_read_by_frame_task(path, frame):
    """The chunks a task owning only ``frame`` reads, with a short look-back,
    and the sequences of its rows."""
    datasource = MCAPDatasourceV2([path], video=VideoOptions())
    (manifest,) = list_manifests(datasource)
    seek_log = SeekLog()
    scanner = dataclasses.replace(
        scanner_for(datasource),
        filesystem=seek_log,
        max_lead_in_ns=SHORT_LOOK_BACK_NS,
    )
    task = one_task_per_chunk(manifest)[frame]
    table = read_all(datasource, task, scanner=scanner)
    return chunks_read(seek_log, path), table.column("sequence").to_pylist()


def first_chunk(path):
    """The offset of the first chunk of ``path``."""
    return min(c.chunk_start_offset for c in summary_of(path).chunk_indexes)


def test_parameter_sets_are_read_only_for_a_keyframe_without_them(tmp_path):
    """A stream that repeats its parameter sets on every keyframe reads nothing
    more for them. One that writes them once reads its first chunk once more in
    a task that starts after its first keyframe."""
    packets = encode_h264(H264_FILE_FRAMES)
    repeated = os.path.join(tmp_path, "repeated.mcap")
    write_payloads(
        repeated, packets, schema_name="foxglove.CompressedVideo", chunk_size=1
    )
    once = write_parameter_sets_once(os.path.join(tmp_path, "once.mcap"), "h264")

    repeated_reads, repeated_rows = chunks_read_by_frame_task(repeated, 25)
    once_reads, once_rows = chunks_read_by_frame_task(once, 25)

    assert repeated_rows == once_rows == [25]
    assert first_chunk(repeated) not in repeated_reads
    # The same chunks, then the first one, read when keyframe 20 lacks them.
    assert len(once_reads) == len(repeated_reads) + 1
    assert once_reads[-1] == first_chunk(once)


def test_stream_without_parameter_sets_warns_naming_file_and_topic(tmp_path, caplog):
    """A stream with no parameter sets at all still decodes no frame. It warns
    once per process, naming the file and the topic, however many tasks read it."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_video_source
    from ray.util.debug import reset_log_once

    path = os.path.join(tmp_path, "none.mcap")
    write_parameter_sets_once(path, "h264", keep_first=False)
    datasource = MCAPDatasourceV2([path], video=VideoOptions())
    (manifest,) = list_manifests(datasource)
    reset_log_once(f"mcap_missing_parameter_sets:{path}:/camera")

    # Ray's loggers do not propagate to the root logger caplog listens on.
    mcap_video_source.logger.addHandler(caplog.handler)
    try:
        rows = read_rows(datasource, one_task_per_chunk(manifest))
    finally:
        mcap_video_source.logger.removeHandler(caplog.handler)

    warnings = [r.message for r in caplog.records if "parameter sets" in r.message]
    assert rows == []
    assert len(warnings) == 1
    assert f"'/camera' in {path!r}" in warnings[0] and "H264" in warnings[0]


# -- where decoding starts ----------------------------------------------------


@pytest.fixture
def frame_per_chunk_file(tmp_path):
    """The test encoder's 30 H.264 frames, one per chunk.

        frame   0    1 .. 9    10   11 .. 19   20   21 .. 29
                K    P .. P    K    P .. P     K    P .. P

    Frame ``i`` is logged at ``i * FRAME_NS`` with sequence ``i``.
    """
    path = os.path.join(tmp_path, "frame_per_chunk.mcap")
    write_payloads(
        path,
        encode_h264(H264_FILE_FRAMES),
        schema_name="foxglove.CompressedVideo",
        chunk_size=1,
    )
    return path


def task_from(manifest, first):
    """A task owning the manifest's chunks from chunk ``first`` on."""
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    return FileManifest(block.slice(first))


def count_decoding(monkeypatch):
    """Record the log time of each packet fed to a ``FrameDecoder``, and of each
    frame it converts to RGB."""
    counts: Dict[str, List[int]] = {"fed": [], "converted": []}
    real_decode_packet = FrameDecoder._decode_packet
    real_to_array = FrameDecoder._to_array

    def decode_packet(self, message):
        counts["fed"].append(message.log_time)
        return real_decode_packet(self, message)

    def to_array(self, frame):
        counts["converted"].append(frame.pts)
        return real_to_array(self, frame)

    monkeypatch.setattr(FrameDecoder, "_decode_packet", decode_packet)
    monkeypatch.setattr(FrameDecoder, "_to_array", to_array)
    return counts


def frame_log_times(first, last=H264_FILE_FRAMES):
    """The log times of frames ``first`` to ``last - 1``."""
    return [i * FRAME_NS for i in range(first, last)]


def test_a_task_opening_on_a_keyframe_decodes_no_look_back(
    frame_per_chunk_file, monkeypatch, caplog
):
    """A task whose first frame is keyframe 20 starts decoding there, not at
    keyframe 10 in its look-back, and emits the frames a whole-file read does.
    With a look-back too short to reach keyframe 10 it does not warn either,
    since no frame is skipped."""
    from ray.util.debug import reset_log_once

    datasource = MCAPDatasourceV2([frame_per_chunk_file], video=VideoOptions())
    (manifest,) = list_manifests(datasource)
    truth = frames_by_log_time(read_rows(datasource, [manifest]))
    task = task_from(manifest, 20)
    scanner = scanner_for(datasource)
    # A 0.1 s look-back reaches frames 17-19, and no keyframe.
    short = dataclasses.replace(scanner, max_lead_in_ns=100_000_000)
    counts = count_decoding(monkeypatch)
    reset_log_once("mcap_cold_channel:/camera")

    rows = read_all(datasource, task, scanner=scanner).to_pylist()
    fed = counts["fed"][:]
    # Ray's loggers do not propagate to the root logger caplog listens on.
    mcap_decode.logger.addHandler(caplog.handler)
    try:
        short_rows = read_all(datasource, task, scanner=short).to_pylist()
    finally:
        mcap_decode.logger.removeHandler(caplog.handler)

    assert fed == frame_log_times(20)
    for frames in (frames_by_log_time(rows), frames_by_log_time(short_rows)):
        assert sorted(frames) == frame_log_times(20)
        for log_time, frame in frames.items():
            assert (frame == truth[log_time]).all()
    assert not [r for r in caplog.records if "look-back cap" in r.message]


def test_look_back_frames_are_decoded_but_not_converted(
    frame_per_chunk_file, monkeypatch
):
    """A task whose first frame is 15 decodes its look-back from keyframe 10,
    and converts to RGB only the frames it emits, 15 to 29."""
    datasource = MCAPDatasourceV2([frame_per_chunk_file], video=VideoOptions())
    (manifest,) = list_manifests(datasource)
    task = task_from(manifest, 15)
    scanner = scanner_for(datasource)
    counts = count_decoding(monkeypatch)

    rows = read_all(datasource, task, scanner=scanner).to_pylist()

    assert sorted(frames_by_log_time(rows)) == frame_log_times(15)
    assert counts["fed"] == frame_log_times(10)
    assert counts["converted"] == frame_log_times(15)


# -- length-prefixed NAL units (AVCC) -----------------------------------------


def write_compressed_video(path, packets, codec="h264"):
    """Write ``packets`` as CDR ``foxglove_msgs/msg/CompressedVideo`` messages on
    ``/camera``, one per chunk, frame ``i`` logged at ``i * FRAME_NS``."""
    write_payloads(
        path,
        [cdr_video(packet, codec) for packet in packets],
        schema_name="foxglove_msgs/msg/CompressedVideo",
        chunk_size=1,
    )


@pytest.mark.parametrize("parameter_sets", ["every-keyframe", "first-keyframe"])
@pytest.mark.parametrize("codec", ["h264", "h265"])
def test_length_prefixed_nal_units_decode_as_annex_b(tmp_path, parameter_sets, codec):
    """An H.264 or H.265 stream whose NAL units are length-prefixed (AVCC, HVCC)
    decodes every frame, whole and with one task per chunk, as its Annex-B form
    does. With parameter sets on the first keyframe only, a later task reads them
    where the stream starts. Planning gets the frame shape."""
    if not has_encoder({"h264": "libx264", "h265": "libx265"}[codec]):
        pytest.skip(f"this build of av has no {codec} encoder")
    packets = ENCODERS[codec](H264_FILE_FRAMES)
    if parameter_sets == "first-keyframe":
        packets = parameter_sets_once(packets, codec)
    annex_b_path = os.path.join(tmp_path, "annex_b.mcap")
    avcc_path = os.path.join(tmp_path, "avcc.mcap")
    write_compressed_video(annex_b_path, packets, codec)
    write_compressed_video(avcc_path, [length_prefixed(p) for p in packets], codec)
    annex_b = MCAPDatasourceV2([annex_b_path], video=VideoOptions())
    truth = frames_by_log_time(read_rows(annex_b, list_manifests(annex_b)))
    datasource = MCAPDatasourceV2([avcc_path], video=VideoOptions())
    (manifest,) = list_manifests(datasource)

    whole = frames_by_log_time(read_rows(datasource, [manifest]))
    split = frames_by_log_time(read_rows(datasource, one_task_per_chunk(manifest)))

    for frames in (whole, split):
        assert sorted(frames) == frame_log_times(0)
        for log_time, frame in frames.items():
            assert (frame == truth[log_time]).all()
    assert infer_schema(datasource).field("frame").type.shape == FRAME_SHAPE


def test_length_prefixed_h264_without_parameter_sets_warns(tmp_path, caplog):
    """An AVCC stream whose parameter sets live outside the recording, as in an
    MP4 ``avcC`` record, decodes no frame. It warns once, naming the file and the
    topic, however many tasks read it."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_video_source
    from ray.util.debug import reset_log_once

    path = os.path.join(tmp_path, "avcc_no_sets.mcap")
    packets = parameter_sets_once(encode_h264(H264_FILE_FRAMES), "h264", False)
    write_compressed_video(path, [length_prefixed(p) for p in packets])
    datasource = MCAPDatasourceV2([path], video=VideoOptions())
    (manifest,) = list_manifests(datasource)
    reset_log_once(f"mcap_missing_parameter_sets:{path}:/camera")

    # Ray's loggers do not propagate to the root logger caplog listens on.
    mcap_video_source.logger.addHandler(caplog.handler)
    try:
        rows = read_rows(datasource, one_task_per_chunk(manifest))
    finally:
        mcap_video_source.logger.removeHandler(caplog.handler)

    warnings = [r.message for r in caplog.records if "parameter sets" in r.message]
    assert rows == []
    assert len(warnings) == 1
    assert f"'/camera' in {path!r}" in warnings[0]


@pytest.mark.parametrize("layout", ["three-byte-start-codes", "leading-zero-bytes"])
def test_annex_b_that_opens_like_length_prefixed_decodes(tmp_path, layout):
    """Annex-B access units can open as length-prefixed ones would: with zero
    bytes before the first start code, or with a three-byte start code that
    reads, with the NAL header, as a plausible first length. Their lengths do
    not tile the data, so the stream stays Annex B and every frame decodes."""
    packets = encode_h264(H264_FILE_FRAMES)
    if layout == "three-byte-start-codes":
        # Emulation prevention keeps 00 00 00 01 out of the NAL units.
        variant = [p.replace(b"\x00\x00\x00\x01", b"\x00\x00\x01") for p in packets]
    else:
        variant = [bytes(2) + p for p in packets]
    plain_path = os.path.join(tmp_path, "plain.mcap")
    variant_path = os.path.join(tmp_path, "variant.mcap")
    write_compressed_video(plain_path, packets)
    write_compressed_video(variant_path, variant)
    plain = MCAPDatasourceV2([plain_path], video=VideoOptions())
    truth = frames_by_log_time(read_rows(plain, list_manifests(plain)))
    datasource = MCAPDatasourceV2([variant_path], video=VideoOptions())

    frames = frames_by_log_time(read_rows(datasource, list_manifests(datasource)))

    assert sorted(frames) == frame_log_times(0)
    for log_time, frame in frames.items():
        assert (frame == truth[log_time]).all()


# -- the frame inside a message -----------------------------------------------


def test_header_bytes_that_read_as_a_start_code_are_not_decoded(tmp_path):
    """Each message's CDR header reads as a start code and an IDR slice header,
    and its ``format`` string trails the data. Only the ``data`` field reaches
    the decoder and the keyframe rules, so every frame decodes as from the bare
    stream, whole and with one task per chunk."""
    packets = encode_h264(H264_FILE_FRAMES)
    log_times = [
        START_CODE_SECOND * SECOND + i * FRAME_NS for i in range(H264_FILE_FRAMES)
    ]
    path = os.path.join(tmp_path, "header_start_code.mcap")
    write_payloads(
        path,
        [cdr_video(p, "h264", stamp_ns=t) for p, t in zip(packets, log_times)],
        schema_name="foxglove_msgs/msg/CompressedVideo",
        chunk_size=1,
        log_times=log_times,
    )
    bare_path = os.path.join(tmp_path, "bare.mcap")
    write_payloads(
        bare_path, packets, schema_name="foxglove.CompressedVideo", log_times=log_times
    )
    bare = MCAPDatasourceV2([bare_path], video=VideoOptions())
    truth = frames_by_log_time(read_rows(bare, list_manifests(bare)))
    datasource = MCAPDatasourceV2([path], video=VideoOptions())
    (manifest,) = list_manifests(datasource)

    whole = frames_by_log_time(read_rows(datasource, [manifest]))
    split = frames_by_log_time(read_rows(datasource, one_task_per_chunk(manifest)))

    for frames in (whole, split):
        assert sorted(frames) == log_times
        for log_time, frame in frames.items():
            assert (frame == truth[log_time]).all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
