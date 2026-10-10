"""Unit tests for decoded MCAP reads split across tasks.

A task that owns part of a video stream primes its decoder from a lead-in,
feeds it the frames other tasks own inside its span, and seeds ``fps``
thinning, so a split read keeps the frames a whole-file read keeps. No Ray
cluster: the indexer, scanner and reader are driven directly.
"""

import dataclasses
import importlib.util
import io
import os
from fractions import Fraction
from typing import Dict, List, Optional

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap import mcap_decoded_messages
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import (
    FrameDecoder,
    FrameThinner,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import VideoOptions
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
    SECOND,
    VP9_KEYFRAME,
    VP9_PFRAME,
    encode_h264,
    list_manifests,
    listed_if_custom,
    manifest_for_frames,
    read_rows,
    scanner_for,
    split_after,
    split_at_first_idr,
    summary_of,
    window_datasource,
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


def test_frame_thinner_never_rewinds():
    """A late frame from an interval thinning has passed is dropped, and does not
    reopen the current interval for the frame after it."""
    thinner = FrameThinner(100)
    thinner.observe(250)  # the lead-in already kept a frame in interval 2

    assert thinner.keep(260) is False
    assert thinner.keep(150) is False
    assert thinner.keep(270) is False
    assert thinner.keep(310) is True
    assert FrameThinner(None).keep(5) is True


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

    def decode(self, message):
        ready, self._held = self._held, list(super().decode(message))
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
    # Frames 100 ms apart: with fps=2, frame 15 opens the 1.5-2 s interval.
    write_chunks(path, encode_h264(30), chunks, [i * TENTH_S for i in range(30)])
    datasource = MCAPDatasourceV2(
        [path], topics=["/camera"], video=VideoOptions(fps=fps)
    )
    (manifest,) = list_manifests(datasource)
    assert len(manifest) == 2

    whole = read_rows(datasource, [manifest])
    kept = [r["sequence"] for r in whole]
    assert kept == (list(range(30)) if fps is None else [0, 5, 10, 15, 20, 25])
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


def test_lead_in_parameter_sets_do_not_take_an_fps_interval(tmp_path):
    """A parameter-set message in the lead-in is not a frame, so it must not use
    up the ``fps`` interval of the owned keyframe just after it."""
    packets = encode_h264(10)
    parameter_sets, keyframe = split_at_first_idr(packets[0])
    path = os.path.join(tmp_path, "ps.mcap")
    # Parameter sets at 1 ms and the keyframe at 50 ms share one 100 ms interval.
    log_times = [1_000_000, 50_000_000] + [i * 100_000_000 for i in range(1, 10)]
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


def test_cold_frames_still_hold_their_fps_interval(h264_file):
    """Frames skipped while a channel is cold still use up their ``fps``
    interval, so a split read keeps the same frames as the whole-file read."""
    datasource = MCAPDatasourceV2([h264_file], video=VideoOptions(fps=5))
    (manifest,) = list_manifests(datasource)
    whole = read_rows(datasource, [manifest])
    assert [r["sequence"] for r in whole] == [0, 7, 13, 19, 25]
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    scanner = scanner_for(datasource)
    scanner = dataclasses.replace(scanner, max_lead_in_ns=50_000_000)
    # The task owns chunks 2-5, from frame 8, with keyframe 0 out of reach.
    part = FileManifest(block.slice(2))
    # Cold 8 and 9 and keyframe 10 share frame 7's 200 ms interval, so 13 comes first.
    expected = [13, 19, 25]

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
def test_fps_interval_longer_than_the_look_back_does_not_depend_on_the_split(
    tmp_path, columns
):
    """With 20 s ``fps`` intervals and the default 10 s look-back, a task that
    starts after a 14 s pause learns from the message index that the frame at
    1 s already took its first interval."""
    path = os.path.join(tmp_path, "pause.mcap")
    write_stills(path, [1] + list(range(15, 23)))
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=0.05))
    (manifest,) = list_manifests(datasource)

    whole = read_rows(datasource, [manifest], columns)
    split = read_rows(datasource, split_after(manifest, 1), columns)

    assert [r["log_time"] for r in whole] == [1 * SECOND, 20 * SECOND]
    assert [r["log_time"] for r in split] == [r["log_time"] for r in whole]


@pytest.mark.parametrize("columns", [None, ["log_time"]], ids=["decoded", "pruned"])
def test_fps_seed_covers_a_gap_longer_than_the_look_back(tmp_path, columns):
    """A task owns the frames at 18 s and from 35 s on, another task the one at
    22 s. The task learns from the message index that 22 s took the interval
    of its frame at 35 s, which lies beyond the look-back."""
    path = os.path.join(tmp_path, "gap.mcap")
    write_stills(path, [18, 22] + list(range(35, 42)))
    datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=0.05))
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    ours = FileManifest(block.take([0] + list(range(2, 9))))
    theirs = FileManifest(block.slice(1, 1))

    whole = read_rows(datasource, [manifest], columns)
    split = read_rows(datasource, [ours, theirs], columns)

    assert [r["log_time"] for r in whole] == [18 * SECOND, 22 * SECOND, 40 * SECOND]
    assert sorted(r["log_time"] for r in split) == [r["log_time"] for r in whole]


class LateContext:
    """A codec context that releases frames ``delay`` packets late, as libdav1d may."""

    def __init__(self, context, delay=1):
        self._context = context
        self._delay = delay
        # The frames of each packet fed and not released yet, oldest first.
        self._held = []

    def decode(self, packet):
        if packet is None:
            frames = [frame for held in self._held for frame in held]
            self._held = []
            return frames + list(self._context.decode(None))
        self._held.append(list(self._context.decode(packet)))
        return self._held.pop(0) if len(self._held) > self._delay else []


class UnstampedAtFlushContext(LateContext):
    """Releases each frame one packet late, and those left at the end unstamped."""

    def decode(self, packet):
        frames = super().decode(packet)
        if packet is None:
            for frame in frames:
                frame.pts = None
        return frames


@pytest.mark.parametrize("granularity", ["message", "window"])
def test_a_frame_released_unstamped_at_the_end_is_thinned(
    tmp_path, monkeypatch, granularity
):
    """A frame the decoder releases without a stamp when the task ends takes the
    time of the last packet fed, and ``fps`` thins it like any other.

    All 30 frames fall in one 1 s interval, so only frame 0 is kept.
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

    if granularity == "message":
        datasource = MCAPDatasourceV2([path], video=VideoOptions(fps=1))
        rows = read_rows(datasource, list_manifests(datasource))
        assert [row["sequence"] for row in rows] == [0]
    else:
        datasource = window_datasource(path, length_s=2, fps=1)
        rows = read_rows(datasource, list_manifests(datasource))
        assert [row["frame_times:/camera"] for row in rows] == [[0]]


def test_fps_seed_at_a_gap_comes_after_a_frame_released_late(tmp_path, monkeypatch):
    """A task's frame before a gap that the decoder releases late is thinned
    before the other task's frame in the gap, as in a whole-file read."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_decode

    real_context = mcap_decode._codec_context
    monkeypatch.setattr(
        mcap_decode, "_codec_context", lambda codec: LateContext(real_context(codec))
    )
    # Frame 9 opens the 20-40 s interval alone. The other task owns GOP 10-19
    # from 41 s. Our GOP 20-29 follows a pause, from 55 s.
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

    assert [r["sequence"] for r in whole] == [0, 9, 10]
    assert sorted(r["sequence"] for r in split) == [0, 9, 10]


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
def test_frames_held_at_a_gap_keep_their_fps_interval(tmp_path, monkeypatch, gap):
    """Frames of ours that the AV1 decoder still holds at a gap keep their ``fps``
    intervals, so a split read keeps the frames a whole-file read keeps."""
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
    # Frame 8 opens the 5 s interval alone.
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


def test_fps_seed_falls_back_to_the_lead_in_without_message_indexes(tmp_path):
    """A file written without message indexes still reads. A task then sees only
    the frames in its look-back, so after a long pause it keeps one frame more
    than a whole-file read."""
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
    split = read_rows(datasource, split_after(manifest, 1))

    assert [r["log_time"] for r in whole] == [1 * SECOND, 20 * SECOND]
    assert [r["log_time"] for r in split] == [1 * SECOND, 15 * SECOND, 20 * SECOND]


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


@pytest.mark.parametrize("granularity", ["message", "window"])
def test_corrupt_still_in_the_lead_in_leaves_its_fps_interval_free(
    tmp_path, granularity
):
    """A rejected still in a task's lead-in does not take its ``fps`` interval.

    With ``fps=10``, frames 4 and 5 share an interval and frame 4 is corrupt. In
    a whole-file read frame 5 takes the interval, so it must in a split read
    whose second task has frame 4 in its lead-in.
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
    payloads[4] = payloads[4][:40]
    path = os.path.join(tmp_path, "corrupt_fps.mcap")
    write_payloads(
        path, payloads, schema_name="sensor_msgs/msg/CompressedImage", chunk_size=1
    )
    if granularity == "message":
        # The second task owns frame 5 alone.
        datasource, first = MCAPDatasourceV2([path], video=VideoOptions(fps=10)), 5
    else:
        # The second task owns the window opening at 150 ms.
        datasource, first = window_datasource(path, length_s=0.15, fps=10), 4
    (manifest,) = list_manifests(datasource)

    def frames(rows):
        """The frame numbers each row holds."""
        if granularity == "message":
            return [[row["log_time"] // FRAME_NS] for row in rows]
        return [[t // FRAME_NS for t in row["frame_times:/camera"]] for row in rows]

    whole = read_rows(datasource, [manifest])
    split = read_rows(datasource, split_after(manifest, first))

    assert frames(whole) == frames(split) == [[0], [5]]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
