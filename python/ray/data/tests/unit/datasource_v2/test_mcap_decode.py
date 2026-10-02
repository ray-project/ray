"""Unit tests for in-task video decoding on the V2 MCAP reader (``VideoOptions.decode``)."""

import importlib.util
import io
import os

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.formats.mcap import mcap_reader
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import (
    FrameDecoder,
    FrameThinner,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    TimeRange,
    VideoOptions,
    WindowSpec,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import read_summary
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    detect_codec,
    is_keyframe,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest

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

GOP = 10
FRAME_NS = 33_000_000


def encode_h264(num_frames, width=64, height=48):
    """H.264 Annex-B access units, one per frame, a keyframe every ``GOP``."""
    import av
    import numpy as np

    container = av.open(io.BytesIO(), mode="w", format="h264")
    # A fixed GOP, no B-frames and no scene-cut keyframes keep the keyframe
    # positions predictable; without a global header libx264 repeats SPS/PPS
    # before every keyframe, as recorders do.
    stream = container.add_stream(
        "libx264",
        rate=30,
        options={"g": str(GOP), "bf": "0", "sc_threshold": "0", "tune": "zerolatency"},
    )
    stream.width, stream.height, stream.pix_fmt = width, height, "yuv420p"
    packets = []
    for i in range(num_frames):
        array = np.full((height, width, 3), i * 8 % 256, dtype=np.uint8)
        array[:, :, 1] = (i * 3) % 256
        frame = av.VideoFrame.from_ndarray(array, format="rgb24")
        packets.extend(bytes(p) for p in stream.encode(frame))
    packets.extend(bytes(p) for p in stream.encode())
    return packets


def write_payloads(
    path,
    payloads,
    *,
    schema_name,
    topic="/camera",
    chunk_size=1 << 20,
    log_times=None,
    extra_topic=None,
):
    """One channel of ``payloads`` at 30 fps (or ``log_times``), plus an optional
    ``extra_topic`` as ``(topic, schema_name, payload)`` written at every step."""
    from mcap.writer import CompressionType, Writer

    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=chunk_size, compression=CompressionType.ZSTD)
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name=schema_name, encoding="ros2msg", data=b"video\n"
        )
        channel_id = writer.register_channel(
            schema_id=schema_id, topic=topic, message_encoding="cdr"
        )
        extra_channel = None
        extra_payload = b""
        if extra_topic is not None:
            extra_schema = writer.register_schema(
                name=extra_topic[1], encoding="ros2msg", data=b"x\n"
            )
            extra_channel = writer.register_channel(
                schema_id=extra_schema, topic=extra_topic[0], message_encoding="cdr"
            )
            extra_payload = extra_topic[2]
        for i, payload in enumerate(payloads):
            log_time = log_times[i] if log_times is not None else i * FRAME_NS
            writer.add_message(
                channel_id=channel_id,
                log_time=log_time,
                publish_time=log_time,
                data=payload,
                sequence=i,
            )
            if extra_channel is not None:
                writer.add_message(
                    channel_id=extra_channel,
                    log_time=log_time,
                    publish_time=log_time,
                    data=extra_payload,
                    sequence=i,
                )
        writer.finish()


@pytest.fixture
def h264_file(tmp_path):
    """30 H.264 access units in several chunks, so chunk cuts fall mid-GOP."""
    path = os.path.join(tmp_path, "h264.mcap")
    write_payloads(
        path, encode_h264(30), schema_name="foxglove.CompressedVideo", chunk_size=400
    )
    summary = read_summary(LocalFileSystem(), path)
    assert summary is not None and len(summary.chunk_indexes) >= 4
    return path


@pytest.fixture
def jpeg_file(tmp_path):
    """10 JPEGs behind a header, the ROS 2 ``sensor_msgs/CompressedImage`` layout."""
    import numpy as np
    from PIL import Image

    payloads = []
    for i in range(10):
        array = np.full((48, 64, 3), i * 20 % 256, dtype=np.uint8)
        buffer = io.BytesIO()
        Image.fromarray(array).save(buffer, format="JPEG")
        payloads.append(b"\x00\x01\x00\x00" + b"header" * 4 + buffer.getvalue())
    path = os.path.join(tmp_path, "jpeg.mcap")
    write_payloads(path, payloads, schema_name="sensor_msgs/msg/CompressedImage")
    return path


def list_manifests(datasource):
    indexer = datasource._get_file_indexer()
    return list(
        indexer.list_files(pa.array(datasource.paths), filesystem=datasource.filesystem)
    )


def read_rows(datasource, manifests, columns=None):
    indexer = datasource._get_file_indexer()
    sample = sample_files(indexer, datasource.paths, datasource.filesystem, [])
    scanner = datasource.create_scanner(
        datasource.infer_schema(sample), datasource.filesystem
    )
    if columns is not None:
        scanner = scanner.prune_columns(columns)
    rows = []
    for manifest in manifests:
        for table in scanner.create_reader().read(manifest):
            assert table.schema.names == scanner.read_schema().names
            rows.extend(table.to_pylist())
    return rows, scanner


def test_encoder_keyframes_are_detected():
    packets = encode_h264(30)
    assert all(detect_codec(p) is VideoCodec.H264 for p in packets)
    keyframes = [i for i, p in enumerate(packets) if is_keyframe(p, VideoCodec.H264)]
    assert keyframes == list(range(0, 30, GOP))


def test_decode_yields_one_frame_per_message(h264_file):
    datasource = MCAPDatasourceV2(
        [h264_file], video=VideoOptions(decode=True), include_row_id=True
    )
    rows, scanner = read_rows(datasource, list_manifests(datasource))
    assert len(rows) == 30
    assert [row["sequence"] for row in rows] == list(range(30))
    frame = rows[0]["frame"]
    assert frame.shape == (48, 64, 3) and frame.dtype.name == "uint8"
    assert "data" not in rows[0]
    assert {"topic", "log_time", "publish_time", "channel_id", "row_id"} <= set(rows[0])
    assert str(scanner.read_schema().field("frame").type).startswith(
        "ArrowTensorTypeV2"
    )


def test_decode_primes_a_task_that_starts_mid_gop(h264_file):
    datasource = MCAPDatasourceV2([h264_file], video=VideoOptions(decode=True))
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    n = len(manifest)
    assert n >= 3
    # Three tasks; the second and third start inside a group of pictures and
    # must read back to the previous keyframe to decode their first frames.
    parts = [
        FileManifest(block.slice(0, n // 3)),
        FileManifest(block.slice(n // 3, n // 3)),
        FileManifest(block.slice(2 * (n // 3))),
    ]
    whole, _ = read_rows(datasource, [manifest])
    split, _ = read_rows(datasource, parts)
    assert sorted(r["sequence"] for r in split) == sorted(r["sequence"] for r in whole)
    assert len(split) == 30
    by_seq = {r["sequence"]: r["frame"] for r in whole}
    for row in split:
        assert (row["frame"] == by_seq[row["sequence"]]).all()


def test_decode_attributes_frames_of_messages_sharing_a_log_time(tmp_path):
    """Two messages stamped alike each get their own frame, in order."""
    log_times = [i * FRAME_NS for i in range(30)]
    log_times[6] = log_times[5]  # frames 5 and 6 share a timestamp
    log_times[29] = log_times[28]  # and so do the last two, drained by flush
    path = os.path.join(tmp_path, "dup.mcap")
    write_payloads(
        path,
        encode_h264(30),
        schema_name="foxglove.CompressedVideo",
        chunk_size=400,
        log_times=log_times,
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions(decode=True))
    rows, _ = read_rows(datasource, list_manifests(datasource))
    assert sorted(row["sequence"] for row in rows) == list(range(30))
    assert [row["log_time"] for row in rows] == sorted(log_times)
    by_seq = {row["sequence"]: row["frame"] for row in rows}
    reference = MCAPDatasourceV2(
        [path], video=VideoOptions(decode=True), include_row_id=True
    )
    (manifest,) = list_manifests(reference)
    whole, _ = read_rows(reference, [manifest])
    for row in whole:
        assert (by_seq[row["sequence"]] == row["frame"]).all()


def test_planning_checks_every_selected_topic(tmp_path):
    """A non-video topic after the video one, or with ``resize`` set, still fails
    at planning rather than in a read task."""
    path = os.path.join(tmp_path, "mixed.mcap")
    write_payloads(
        path,
        encode_h264(10),
        schema_name="foxglove.CompressedVideo",
        extra_topic=("/imu", "sensor_msgs/msg/Imu", b"\x01\x02\x03\x04" * 8),
    )
    for video in (
        VideoOptions(decode=True),
        VideoOptions(decode=True, resize=(24, 32)),
    ):
        datasource = MCAPDatasourceV2([path], video=video)
        with pytest.raises(ValueError, match="'/imu'.*not a video topic"):
            infer_frame_schema(datasource)
    only_camera = MCAPDatasourceV2(
        [path], topics=["/camera"], video=VideoOptions(decode=True)
    )
    rows, _ = read_rows(only_camera, list_manifests(only_camera))
    assert len(rows) == 10


def infer_frame_schema(datasource):
    indexer = datasource._get_file_indexer()
    sample = sample_files(indexer, datasource.paths, datasource.filesystem, [])
    return datasource.infer_schema(sample)


def test_frame_thinner_never_rewinds():
    thinner = FrameThinner(100)
    thinner.observe(250)  # the lead-in already kept a frame in interval 2
    assert thinner.keep(260) is False
    # A frame released late from an earlier interval neither survives nor
    # reopens interval 2 for the frame after it.
    assert thinner.keep(150) is False
    assert thinner.keep(270) is False
    assert thinner.keep(310) is True
    assert FrameThinner(None).keep(5) is True


def write_two_cameras(path, offset_frames):
    """Two H.264 cameras, one chunk per message; ``/b`` runs ``offset_frames``
    behind ``/a`` with the same encoded stream."""
    from mcap.writer import CompressionType, Writer

    packets = encode_h264(30)
    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=1, compression=CompressionType.ZSTD)
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name="foxglove.CompressedVideo", encoding="ros2msg", data=b"video\n"
        )
        channels = {
            topic: writer.register_channel(
                schema_id=schema_id, topic=topic, message_encoding="cdr"
            )
            for topic in ("/a", "/b")
        }
        for i, payload in enumerate(packets):
            for topic, shift in (("/a", 0), ("/b", offset_frames)):
                log_time = (i + shift) * FRAME_NS
                writer.add_message(
                    channel_id=channels[topic],
                    log_time=log_time,
                    publish_time=log_time,
                    data=payload,
                    sequence=i,
                )
        writer.finish()
    return channels


def test_each_video_channel_gets_its_own_lead_in(tmp_path):
    """A task owning frames 26-29 of both cameras, with ``/b`` eight frames
    behind: ``/b``'s keyframe (frame 20) lies after ``/a``'s first owned frame
    and in chunks the task does not own, and must still prime ``/b``."""
    path = os.path.join(tmp_path, "two.mcap")
    channels = write_two_cameras(path, offset_frames=8)
    datasource = MCAPDatasourceV2([path], video=VideoOptions(decode=True))
    (manifest,) = list_manifests(datasource)
    summary = read_summary(LocalFileSystem(), path)
    assert summary is not None and len(summary.chunk_indexes) == 60

    def frame_of(chunk):  # one message per chunk
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

    split, _ = read_rows(datasource, [part])
    assert sorted((r["topic"], r["sequence"]) for r in split) == [
        (topic, seq) for topic in ("/a", "/b") for seq in range(26, 30)
    ]
    whole, _ = read_rows(datasource, [manifest])
    reference = {(r["topic"], r["sequence"]): r["frame"] for r in whole}
    for row in split:
        assert (row["frame"] == reference[(row["topic"], row["sequence"])]).all()


def manifest_for_frames(datasource, path, channels, offset_frames, frames):
    """A manifest owning exactly ``frames`` (``(topic, index)`` pairs) of a file
    written with one chunk per message."""
    (manifest,) = list_manifests(datasource)
    summary = read_summary(LocalFileSystem(), path)
    assert summary is not None
    by_channel = {cid: topic for topic, cid in channels.items()}
    wanted_offsets = set()
    for chunk in summary.chunk_indexes:
        (channel_id,) = chunk.message_index_offsets
        topic = by_channel[channel_id]
        shift = offset_frames if topic == "/b" else 0
        if (topic, chunk.message_start_time // FRAME_NS - shift) in frames:
            wanted_offsets.add(chunk.chunk_start_offset)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    rows = [
        i
        for i, md in enumerate(manifest.file_chunk_metadatas)
        if md is not None and int(md["unit_ids"][0]) in wanted_offsets
    ]
    assert len(rows) == len(frames)
    return FileManifest(block.take(rows)), manifest


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
    """Frames another task owns (or a checkpoint excluded) in the middle of a
    channel's stream are decoded for state so the frames after them are right,
    and a frame of ours the decoder held back across the gap is still emitted."""
    if held_back:
        monkeypatch.setattr(mcap_reader, "FrameDecoder", HeldBackDecoder)
    path = os.path.join(tmp_path, "gaps.mcap")
    channels = write_two_cameras(path, offset_frames=0)
    datasource = MCAPDatasourceV2(
        [path], topics=["/a"], video=VideoOptions(decode=True)
    )
    # A gap inside a GOP (frames 15-16 missing) and one spanning a keyframe
    # (frames 10-21 missing, keyframe 20 among them).
    for owned_frames in (
        list(range(0, 15)) + list(range(17, 30)),
        list(range(0, 10)) + list(range(22, 30)),
    ):
        part, whole_manifest = manifest_for_frames(
            datasource, path, channels, 0, {("/a", i) for i in owned_frames}
        )
        rows, _ = read_rows(datasource, [part])
        assert [r["sequence"] for r in rows] == owned_frames
        whole, _ = read_rows(datasource, [whole_manifest])
        reference = {r["sequence"]: r["frame"] for r in whole}
        for row in rows:
            assert (row["frame"] == reference[row["sequence"]]).all()


def split_at_first_idr(packet):
    """Cut an access unit before its IDR NAL unit (type 5), start code included."""
    i = 0
    while True:
        j = packet.find(b"\x00\x00\x01", i)
        assert j >= 0, "no IDR NAL unit"
        if packet[j + 3] & 0x1F == 5:
            cut = j - 1 if j > 0 and packet[j - 1] == 0 else j
            return packet[:cut], packet[cut:]
        i = j + 3


def test_planning_decodes_parameter_sets_written_separately(tmp_path):
    """SPS and PPS in a message of their own ahead of the keyframe: planning
    plays the stream head in order instead of the lone keyframe."""
    packets = encode_h264(10)
    parameter_sets, keyframe = split_at_first_idr(packets[0])
    assert detect_codec(parameter_sets) is VideoCodec.H264
    assert not is_keyframe(parameter_sets, VideoCodec.H264)
    assert is_keyframe(keyframe, VideoCodec.H264)
    path = os.path.join(tmp_path, "split.mcap")
    write_payloads(
        path,
        [parameter_sets, keyframe] + packets[1:],
        schema_name="foxglove.CompressedVideo",
        log_times=[0, 0] + [i * FRAME_NS for i in range(1, 10)],
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions(decode=True))
    schema = infer_frame_schema(datasource)
    assert "shape=(48, 64, 3)" in str(schema.field("frame").type)
    rows, _ = read_rows(datasource, list_manifests(datasource))
    assert all(row["frame"].shape == (48, 64, 3) for row in rows)
    # The keyframe's frame belongs to the keyframe message (sequence 1), not to
    # the parameter-set message stamped alike (sequence 0), which is no row.
    assert [row["sequence"] for row in rows] == list(range(1, 11))
    pruned, _ = read_rows(datasource, list_manifests(datasource), columns=["sequence"])
    assert [row["sequence"] for row in pruned] == list(range(1, 11))


def test_pruned_projection_counts_the_frames_a_decoder_would_emit(h264_file):
    """``count()`` (no ``frame`` column) and a full read agree on the rows: a
    channel that starts without a keyframe in reach yields nothing until its
    next keyframe either way."""
    datasource = MCAPDatasourceV2(
        [h264_file], video=VideoOptions(decode=True, max_lead_in_s=0.05)
    )
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    summary = read_summary(LocalFileSystem(), h264_file)
    assert summary is not None
    # Chunks starting mid-GOP (frames 8 and 15 in this fixture): the first
    # GOP's keyframe (frame 0) is further back than 50 ms, so decoding can only
    # start at the keyframe at frame 10.
    rows = [
        i
        for i, c in enumerate(summary.chunk_indexes)
        if 2 <= c.message_start_time // FRAME_NS < 20
    ]
    assert rows
    part = FileManifest(block.take(rows))
    decoded, _ = read_rows(datasource, [part])
    counted, _ = read_rows(datasource, [part], columns=["sequence"])
    assert [r["sequence"] for r in decoded] == [r["sequence"] for r in counted]
    assert decoded and all(r["sequence"] >= 10 for r in decoded)


def test_lead_in_parameter_sets_do_not_take_an_fps_interval(tmp_path):
    """A parameter-set message in the lead-in is no frame, so it must not use
    up the interval of the first owned frame stamped just after it."""
    packets = encode_h264(10)
    parameter_sets, keyframe = split_at_first_idr(packets[0])
    path = os.path.join(tmp_path, "ps.mcap")
    # Parameter sets at 1 ms, the keyframe at 50 ms: the same 100 ms interval.
    log_times = [1_000_000, 50_000_000] + [i * 100_000_000 for i in range(1, 10)]
    write_payloads(
        path,
        [parameter_sets, keyframe] + packets[1:],
        schema_name="foxglove.CompressedVideo",
        chunk_size=1,
        log_times=log_times,
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions(decode=True, fps=10))
    (manifest,) = list_manifests(datasource)
    whole, _ = read_rows(datasource, [manifest])
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    # A task owning everything but the parameter-set message: it is lead-in.
    split, _ = read_rows(datasource, [FileManifest(block.slice(1))])
    assert [r["sequence"] for r in whole] == list(range(1, 11))
    assert [r["sequence"] for r in split] == [r["sequence"] for r in whole]


def test_gap_longer_than_the_lead_in_goes_cold_until_a_keyframe(tmp_path):
    """A gap that cannot be fed whole and holds no keyframe in its tail leaves
    the decoder without references: the frames after it are skipped until the
    next keyframe rather than decoded against the wrong pictures."""
    path = os.path.join(tmp_path, "longgap.mcap")
    channels = write_two_cameras(path, offset_frames=0)
    datasource = MCAPDatasourceV2(
        [path], topics=["/a"], video=VideoOptions(decode=True, max_lead_in_s=0.1)
    )
    # Frames 5-17 belong to another task; only 15-17 fit in a 100 ms lead-in and
    # none of them is a keyframe (10 lies outside it), so 18 and 19 are skipped.
    owned_frames = list(range(0, 5)) + list(range(18, 30))
    part, whole_manifest = manifest_for_frames(
        datasource, path, channels, 0, {("/a", i) for i in owned_frames}
    )
    rows, _ = read_rows(datasource, [part])
    expected = list(range(0, 5)) + list(range(20, 30))
    assert [r["sequence"] for r in rows] == expected
    whole, _ = read_rows(datasource, [whole_manifest])
    reference = {r["sequence"]: r["frame"] for r in whole}
    for row in rows:
        assert (row["frame"] == reference[row["sequence"]]).all()
    counted, _ = read_rows(datasource, [part], columns=["sequence"])
    assert [r["sequence"] for r in counted] == expected


def test_decode_time_range_starting_mid_gop(h264_file):
    start, end = GOP + GOP // 2, GOP + GOP // 2 + 5
    datasource = MCAPDatasourceV2(
        [h264_file],
        time_range=TimeRange(start * FRAME_NS, end * FRAME_NS),
        video=VideoOptions(decode=True),
    )
    rows, _ = read_rows(datasource, list_manifests(datasource))
    assert [row["sequence"] for row in rows] == list(range(start, end))


def test_decode_resize_and_fps(h264_file):
    resized = MCAPDatasourceV2(
        [h264_file], video=VideoOptions(decode=True, resize=(24, 32))
    )
    rows, scanner = read_rows(resized, list_manifests(resized))
    assert len(rows) == 30 and rows[0]["frame"].shape == (24, 32, 3)
    assert "shape=(24, 32, 3)" in str(scanner.read_schema().field("frame").type)

    # 30 frames at 33 ms span ten 100 ms intervals: one frame survives in each.
    thinned = MCAPDatasourceV2([h264_file], video=VideoOptions(decode=True, fps=10))
    rows, _ = read_rows(thinned, list_manifests(thinned))
    assert len(rows) == 10
    assert [row["log_time"] // 100_000_000 for row in rows] == list(range(10))
    # The same frames survive however the read is split, and whether or not
    # ``frame`` is projected.
    (manifest,) = list_manifests(thinned)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    parts = [FileManifest(block.slice(0, 2)), FileManifest(block.slice(2))]
    split, _ = read_rows(thinned, parts)
    assert sorted(r["sequence"] for r in split) == [r["sequence"] for r in rows]
    pruned, _ = read_rows(thinned, [manifest], columns=["sequence"])
    assert [r["sequence"] for r in pruned] == [r["sequence"] for r in rows]


def test_decode_embedded_jpeg(jpeg_file):
    datasource = MCAPDatasourceV2([jpeg_file], video=VideoOptions(decode=True))
    rows, _ = read_rows(datasource, list_manifests(datasource))
    assert len(rows) == 10
    assert rows[0]["frame"].shape == (48, 64, 3)
    assert rows[3]["frame"][0, 0, 0] in range(55, 66)  # JPEG is lossy

    resized = MCAPDatasourceV2(
        [jpeg_file], video=VideoOptions(decode=True, resize=(24, 32))
    )
    rows, _ = read_rows(resized, list_manifests(resized))
    assert [row["frame"].shape for row in rows] == [(24, 32, 3)] * 10


def test_corrupt_still_is_skipped_not_fatal(tmp_path):
    """One truncated JPEG costs one frame, not the task."""
    import numpy as np
    from PIL import Image

    payloads = []
    for i in range(6):
        buffer = io.BytesIO()
        Image.fromarray(np.full((48, 64, 3), i * 20, dtype=np.uint8)).save(
            buffer, format="JPEG"
        )
        payloads.append(buffer.getvalue())
    payloads[2] = payloads[2][:40]  # still sniffs as JPEG, cannot be decoded
    assert detect_codec(payloads[2]) is VideoCodec.JPEG
    path = os.path.join(tmp_path, "corrupt.mcap")
    write_payloads(path, payloads, schema_name="sensor_msgs/msg/CompressedImage")
    datasource = MCAPDatasourceV2([path], video=VideoOptions(decode=True))
    rows, _ = read_rows(datasource, list_manifests(datasource))
    assert [row["sequence"] for row in rows] == [0, 1, 3, 4, 5]


def test_frame_pruned_away_skips_decoding(h264_file):
    datasource = MCAPDatasourceV2([h264_file], video=VideoOptions(decode=True))
    rows, _ = read_rows(
        datasource, list_manifests(datasource), columns=["topic", "sequence"]
    )
    assert len(rows) == 30 and set(rows[0]) == {"topic", "sequence"}


def test_decode_rejects_non_video_topics(tmp_path):
    path = os.path.join(tmp_path, "imu.mcap")
    write_payloads(
        path,
        [b"\x01\x02\x03\x04" * 8] * 5,
        schema_name="sensor_msgs/msg/Imu",
        topic="/imu",
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions(decode=True))
    with pytest.raises(ValueError, match="'/imu'.*not a video topic"):
        read_rows(datasource, list_manifests(datasource))

    forced = MCAPDatasourceV2([path], video=VideoOptions(decode=True, topics=["/imu"]))
    with pytest.raises(ValueError, match="not JPEG, PNG or H.264/H.265"):
        read_rows(forced, list_manifests(forced))


def test_video_option_validation(h264_file):
    with pytest.raises(ValueError, match="only apply with decode=True"):
        VideoOptions(fps=5)
    with pytest.raises(ValueError, match="only apply with decode=True"):
        VideoOptions(resize=(10, 10))
    with pytest.raises(ValueError, match="positive number"):
        VideoOptions(decode=True, fps=0)
    with pytest.raises(ValueError, match="height, width"):
        VideoOptions(decode=True, resize=(10, 0))
    with pytest.raises(ValueError, match="decode=True.*'message'"):
        MCAPDatasourceV2(
            [h264_file],
            read_granularity="window",
            window=WindowSpec(length_s=1),
            video=VideoOptions(decode=True),
        )
    with pytest.raises(ValueError, match="video applies to"):
        MCAPDatasourceV2([h264_file], video=VideoOptions())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
