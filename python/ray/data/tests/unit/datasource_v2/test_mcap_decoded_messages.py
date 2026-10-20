"""Unit tests for decoded MCAP message rows (``VideoOptions``).

They cover the decoded ``frame`` column, the codec checks at planning,
``resize``, stills, the planned frame type, and how much a decoding task
reads at a time. No Ray cluster: the indexer, scanner and reader are driven
directly.
"""

import dataclasses
import importlib.util
import io
import os
import shutil
import subprocess
import sys
from collections import defaultdict
from typing import Dict, List, Tuple

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.formats.mcap import (
    mcap_datasource_v2,
    mcap_decoded_messages,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import FrameDecoder
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    _MessageTableBuilder,
    decoded_frame_type,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    TimeRange,
    VideoOptions,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    detect_codec,
    is_keyframe,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video_planning import (
    PLANNED_FILES,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.util import KiB, MiB
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    FRAME_NS,
    FRAME_SHAPE,
    GOP,
    H264_FILE_FRAMES,
    HALF_SIZE,
    HEIGHT,
    VP9_KEYFRAME,
    VP9_PFRAME,
    WIDTH,
    SeekLog,
    as_task,
    chunks_read,
    encode_h264,
    infer_schema,
    list_manifests,
    listed_if_custom,
    pack,
    read_rows,
    scanner_for,
    split_at_first_idr,
    summary_of,
    write_cut_short,
    write_payloads,
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

JPEG_FILE_FRAMES = 10  # stills in the ``jpeg_file`` fixture


def encode_jpeg(height, width):
    """A black JPEG of the given size."""
    import numpy as np
    from PIL import Image

    buffer = io.BytesIO()
    Image.fromarray(np.zeros((height, width, 3), dtype=np.uint8)).save(
        buffer, format="JPEG"
    )
    return buffer.getvalue()


@pytest.fixture
def jpeg_file(tmp_path):
    """Ten JPEGs, each after a header as in ROS 2 ``sensor_msgs/CompressedImage``.

    Still ``i`` is a flat image of value ``i * 20``, logged at ``i * FRAME_NS``.
    """
    import numpy as np
    from PIL import Image

    payloads = []
    for i in range(JPEG_FILE_FRAMES):
        array = np.full(FRAME_SHAPE, i * 20 % 256, dtype=np.uint8)
        buffer = io.BytesIO()
        Image.fromarray(array).save(buffer, format="JPEG")
        payloads.append(b"\x00\x01\x00\x00" + b"header" * 4 + buffer.getvalue())
    path = os.path.join(tmp_path, "jpeg.mcap")
    write_payloads(path, payloads, schema_name="sensor_msgs/msg/CompressedImage")
    return path


def test_encoder_keyframes_are_detected():
    """The test encoder's keyframes, one every ``GOP`` frames, are the ones
    ``is_keyframe`` finds."""
    packets = encode_h264(30)

    assert all(detect_codec(p) is VideoCodec.H264 for p in packets)
    keyframes = [i for i, p in enumerate(packets) if is_keyframe(p, VideoCodec.H264)]
    assert keyframes == list(range(0, 30, GOP))


def test_decode_yields_one_frame_per_message(h264_file):
    """Each video message becomes a row whose decoded ``frame`` replaces ``data``."""
    datasource = MCAPDatasourceV2(
        [h264_file], video=VideoOptions(), include_row_id=True
    )
    scanner = scanner_for(datasource)

    rows = read_rows(datasource, list_manifests(datasource))

    assert len(rows) == H264_FILE_FRAMES
    assert [row["sequence"] for row in rows] == list(range(H264_FILE_FRAMES))
    frame = rows[0]["frame"]
    assert frame.shape == FRAME_SHAPE and frame.dtype.name == "uint8"
    assert "data" not in rows[0]
    assert {"topic", "log_time", "publish_time", "channel_id", "row_id"} <= set(rows[0])
    assert scanner.read_schema().field("frame").type == decoded_frame_type(None)


def test_decode_attributes_frames_of_messages_sharing_a_log_time(tmp_path):
    """Two messages with the same log time each get their own frame, in order."""
    log_times = [i * FRAME_NS for i in range(30)]
    log_times[6] = log_times[5]  # frames 5 and 6 share a timestamp
    log_times[29] = log_times[28]  # and so do the last two
    path = os.path.join(tmp_path, "dup.mcap")
    write_payloads(
        path,
        encode_h264(30),
        schema_name="foxglove.CompressedVideo",
        chunk_size=400,
        log_times=log_times,
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions())

    rows = read_rows(datasource, list_manifests(datasource))

    assert sorted(row["sequence"] for row in rows) == list(range(30))
    assert [row["log_time"] for row in rows] == sorted(log_times)
    by_seq = {row["sequence"]: row["frame"] for row in rows}
    reference = MCAPDatasourceV2([path], video=VideoOptions(), include_row_id=True)
    (manifest,) = list_manifests(reference)
    whole = read_rows(reference, [manifest])
    for row in whole:
        assert (by_seq[row["sequence"]] == row["frame"]).all()


def b_frame_reference(packets):
    """Each packet's own frame: a plain decode, stamped with the packet's index."""
    import av  # pyrefly: ignore[missing-import]

    context = av.CodecContext.create("h264", "r")
    expected = {}
    for index, payload in enumerate(packets + [None]):
        packet = None
        if payload is not None:
            packet = av.Packet(payload)
            packet.pts = index
        for frame in context.decode(packet):
            assert frame.pts is not None
            expected[frame.pts] = frame.to_ndarray(format="rgb24")
    return expected


def test_decode_attributes_b_frames_to_their_own_messages(tmp_path):
    """A stream with B-frames releases its frames in display order, not in
    log-time order. Each message still becomes one row, with its own frame."""
    packets = encode_h264(30, b_frames=2)
    path = os.path.join(tmp_path, "bframes.mcap")
    write_payloads(path, packets, schema_name="foxglove.CompressedVideo")
    expected = b_frame_reference(packets)
    datasource = MCAPDatasourceV2([path], video=VideoOptions())

    rows = read_rows(datasource, list_manifests(datasource))

    assert sorted(row["sequence"] for row in rows) == list(range(len(packets)))
    for row in rows:
        assert (row["frame"] == expected[row["sequence"]]).all()


def test_frames_held_past_the_owed_cap_never_land_on_another_row(tmp_path, monkeypatch):
    """A decoder that holds back more frames than a channel waits for gives the
    oldest waiting messages null frames. Their frames, released later, are
    dropped rather than given to other messages."""
    packets = encode_h264(30, b_frames=3)
    path = os.path.join(tmp_path, "bframes.mcap")
    write_payloads(path, packets, schema_name="foxglove.CompressedVideo")
    expected = b_frame_reference(packets)
    # The B-frame decoder holds two frames back, so a cap of one overflows.
    monkeypatch.setattr(mcap_decoded_messages, "_MAX_OWED", 1)
    datasource = MCAPDatasourceV2([path], video=VideoOptions())

    rows = read_rows(datasource, list_manifests(datasource))

    assert sorted(row["sequence"] for row in rows) == list(range(len(packets)))
    decoded = [row for row in rows if row["frame"] is not None]
    assert 0 < len(decoded) < len(rows)
    for row in decoded:
        assert (row["frame"] == expected[row["sequence"]]).all()


def test_planning_checks_every_selected_topic(tmp_path):
    """Planning rejects a selected non-video topic, also when it comes after the
    video topic or ``resize`` is set. Selecting only the video topic works."""
    path = os.path.join(tmp_path, "mixed.mcap")
    write_payloads(
        path,
        encode_h264(10),
        schema_name="foxglove.CompressedVideo",
        extra_topic=("/imu", "sensor_msgs/msg/Imu", b"\x01\x02\x03\x04" * 8),
    )

    for video in (
        VideoOptions(),
        VideoOptions(resize=HALF_SIZE),
    ):
        datasource = MCAPDatasourceV2([path], video=video)
        with pytest.raises(ValueError, match="'/imu'.*not a video topic"):
            infer_schema(datasource)
    only_camera = MCAPDatasourceV2([path], topics=["/camera"], video=VideoOptions())
    rows = read_rows(only_camera, list_manifests(only_camera))
    assert len(rows) == 10


def test_parameter_sets_written_separately_are_no_row(tmp_path):
    """SPS and PPS in their own message before the keyframe reach the decoder,
    so every frame decodes. That message yields no row, with or without
    ``frame``."""
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
    datasource = MCAPDatasourceV2([path], video=VideoOptions())

    rows = read_rows(datasource, list_manifests(datasource))

    assert all(row["frame"].shape == FRAME_SHAPE for row in rows)
    # The frame belongs to the keyframe message (sequence 1), not to the
    # parameter-set message with the same log time (sequence 0).
    assert [row["sequence"] for row in rows] == list(range(1, 11))
    pruned = read_rows(datasource, list_manifests(datasource), columns=["sequence"])
    assert [row["sequence"] for row in pruned] == list(range(1, 11))


@pytest.mark.parametrize(
    "schema_name",
    ["foxglove.CompressedVideo", "custom_msgs/msg/Frame"],
    ids=["video-schema", "custom-schema"],
)
def test_planning_tells_the_codec_from_the_stream_head(tmp_path, schema_name):
    """Planning accepts a VP9 stream that opens on inter frames, taking the codec
    from its first keyframe. A custom schema name needs ``video_topics``."""
    payloads = [VP9_KEYFRAME if i % GOP == 2 else VP9_PFRAME for i in range(30)]
    path = os.path.join(tmp_path, "midgop.mcap")
    write_payloads(path, payloads, schema_name=schema_name, chunk_size=1)
    datasource = MCAPDatasourceV2(
        [path], video=VideoOptions(), video_topics=listed_if_custom(schema_name)
    )

    schema = infer_schema(datasource)

    assert "ArrowVariableShapedTensorType" in str(schema.field("frame").type)


def test_custom_schema_topic_decodes_only_when_listed(tmp_path):
    """An unlisted topic with a custom schema name fails decoded planning.
    Listed in ``video_topics``, it is decoded, its codec told from its bytes."""
    path = os.path.join(tmp_path, "custom.mcap")
    write_payloads(
        path, encode_h264(30), schema_name="custom_msgs/msg/Frame", chunk_size=400
    )
    unlisted = MCAPDatasourceV2([path], video=VideoOptions())
    with pytest.raises(
        ValueError, match=r"'/camera'.*'custom_msgs/msg/Frame'.*video_topics"
    ):
        infer_schema(unlisted)

    listed = MCAPDatasourceV2([path], video=VideoOptions(), video_topics=["/camera"])
    rows = read_rows(listed, list_manifests(listed))
    assert [row["sequence"] for row in rows] == list(range(30))
    assert rows[0]["frame"].shape == FRAME_SHAPE


def test_planning_reads_video_schemas_written_only_in_chunks(tmp_path):
    """A file written with ``repeat_schemas=False`` keeps its schema records in
    the chunks. Planning reads them there, so a known video schema needs no
    ``video_topics`` entry."""
    path = os.path.join(tmp_path, "chunk_schemas.mcap")
    write_payloads(
        path,
        encode_h264(10),
        schema_name="foxglove.CompressedVideo",
        chunk_size=1,
        repeat_schemas=False,
    )
    summary = summary_of(path)
    assert summary.channels and not summary.schemas
    datasource = MCAPDatasourceV2([path], video=VideoOptions())

    rows = read_rows(datasource, list_manifests(datasource))
    assert len(rows) == 10


def test_reader_applies_the_rule_to_files_planning_did_not_sample(tmp_path):
    """A file planning did not sample is held to the same rule: an unlisted topic
    with a custom schema name fails the read with the planning error, and a
    listed one is decoded."""
    vp9 = [VP9_KEYFRAME if i % GOP == 0 else VP9_PFRAME for i in range(30)]
    sampled = os.path.join(tmp_path, "sampled.mcap")
    write_payloads(sampled, vp9, schema_name="foxglove.CompressedVideo", chunk_size=1)
    other = os.path.join(tmp_path, "other.mcap")
    write_payloads(other, vp9, schema_name="custom_msgs/msg/Frame", chunk_size=1)
    (manifest,) = list_manifests(MCAPDatasourceV2([other], video=VideoOptions()))

    for listed in (None, ["/camera"]):
        planner = MCAPDatasourceV2([sampled], video=VideoOptions(), video_topics=listed)
        # A decoder rejects the synthetic payloads, so ``frame`` is pruned.
        scanner = scanner_for(planner).prune_columns(["sequence"])
        tables = scanner.create_reader().read(as_task(manifest))
        if listed is None:
            with pytest.raises(
                ValueError, match=r"'/camera'.*'custom_msgs/msg/Frame'.*video_topics"
            ):
                list(tables)
        else:
            rows = [row["sequence"] for table in tables for row in table.to_pylist()]
            assert rows == list(range(30))


def test_decode_time_range_starting_mid_gop(h264_file):
    """A time range starting mid-GOP yields exactly the frames in the range."""
    start, end = GOP + GOP // 2, GOP + GOP // 2 + 5
    datasource = MCAPDatasourceV2(
        [h264_file],
        time_range=TimeRange(start * FRAME_NS, end * FRAME_NS),
        video=VideoOptions(),
    )

    rows = read_rows(datasource, list_manifests(datasource))

    assert [row["sequence"] for row in rows] == list(range(start, end))


def test_decode_resize(h264_file):
    """``resize`` sets the frame shape, and every frame is a row however the
    read is split, with or without ``frame``."""
    resized = MCAPDatasourceV2([h264_file], video=VideoOptions(resize=HALF_SIZE))
    scanner = scanner_for(resized)
    rows = read_rows(resized, list_manifests(resized))
    assert len(rows) == H264_FILE_FRAMES and rows[0]["frame"].shape == (*HALF_SIZE, 3)
    assert f"shape={(*HALF_SIZE, 3)}" in str(scanner.read_schema().field("frame").type)

    (manifest,) = list_manifests(resized)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    parts = [FileManifest(block.slice(0, 2)), FileManifest(block.slice(2))]
    split = read_rows(resized, parts)
    assert sorted(r["sequence"] for r in split) == [r["sequence"] for r in rows]
    pruned = read_rows(resized, [manifest], columns=["sequence"])
    assert [r["sequence"] for r in pruned] == [r["sequence"] for r in rows]


def test_decode_embedded_jpeg(jpeg_file):
    """JPEGs behind a message header are decoded, and ``resize`` scales them."""
    datasource = MCAPDatasourceV2([jpeg_file], video=VideoOptions())
    rows = read_rows(datasource, list_manifests(datasource))
    assert len(rows) == JPEG_FILE_FRAMES
    assert rows[0]["frame"].shape == FRAME_SHAPE
    assert rows[3]["frame"][0, 0, 0] in range(55, 66)  # JPEG is lossy

    resized = MCAPDatasourceV2([jpeg_file], video=VideoOptions(resize=HALF_SIZE))
    rows = read_rows(resized, list_manifests(resized))
    assert [row["frame"].shape for row in rows] == [(*HALF_SIZE, 3)] * JPEG_FILE_FRAMES


def test_stills_resize_with_a_pillow_older_than_9_1(jpeg_file, monkeypatch):
    """Pillow before 9.1 has no ``Image.Resampling``. Stills still resize there,
    as in Ray's image reader."""
    import types

    import PIL
    from PIL import Image

    class PillowBefore91(types.ModuleType):
        """``PIL.Image`` without ``Resampling``."""

        def __getattr__(self, name):
            if name == "Resampling":
                raise AttributeError(name)
            return getattr(Image, name)

    monkeypatch.setattr(PIL, "Image", PillowBefore91("PIL.Image"))
    datasource = MCAPDatasourceV2([jpeg_file], video=VideoOptions(resize=HALF_SIZE))

    rows = read_rows(datasource, list_manifests(datasource))

    assert [row["frame"].shape for row in rows] == [(*HALF_SIZE, 3)] * JPEG_FILE_FRAMES


def test_corrupt_still_gets_a_null_frame(tmp_path):
    """One truncated JPEG costs its frame, not the task or its row: the row has a
    null ``frame``, and a read without ``frame`` gives the same rows."""
    import numpy as np
    from PIL import Image

    payloads = []
    for i in range(6):
        buffer = io.BytesIO()
        Image.fromarray(np.full(FRAME_SHAPE, i * 20, dtype=np.uint8)).save(
            buffer, format="JPEG"
        )
        payloads.append(buffer.getvalue())
    payloads[2] = payloads[2][:40]  # still sniffs as JPEG, cannot be decoded
    assert detect_codec(payloads[2]) is VideoCodec.JPEG
    path = os.path.join(tmp_path, "corrupt.mcap")
    write_payloads(path, payloads, schema_name="sensor_msgs/msg/CompressedImage")
    datasource = MCAPDatasourceV2([path], video=VideoOptions())

    rows = read_rows(datasource, list_manifests(datasource))
    counted = read_rows(datasource, list_manifests(datasource), columns=["sequence"])

    assert [row["sequence"] for row in rows] == list(range(6))
    assert [row["sequence"] for row in rows if row["frame"] is None] == [2]
    assert [row["sequence"] for row in counted] == list(range(6))


def test_an_undecodable_still_warns_once_per_topic(tmp_path, caplog):
    """A still the codec rejects warns naming its topic and file. Each topic
    warns once, so a second camera's warning is not lost behind the first's."""
    import logging

    import numpy as np
    from PIL import Image

    from ray.util.debug import reset_log_once

    buffer = io.BytesIO()
    Image.fromarray(np.zeros(FRAME_SHAPE, dtype=np.uint8)).save(buffer, format="JPEG")
    # Still sniffs as JPEG, cannot be decoded.
    payloads = [buffer.getvalue(), buffer.getvalue()[:40]]
    paths = {}
    for topic in ("/rejecting/a", "/rejecting/b"):
        paths[topic] = os.path.join(tmp_path, f"{topic.rsplit('/', 1)[1]}.mcap")
        write_payloads(
            paths[topic],
            payloads,
            schema_name="sensor_msgs/msg/CompressedImage",
            topic=topic,
        )
    decode_logger = logging.getLogger(FrameDecoder.__module__)
    # Ray's loggers do not propagate to the root logger caplog listens on.
    decode_logger.addHandler(caplog.handler)
    reset_log_once()
    try:
        for path in paths.values():
            datasource = MCAPDatasourceV2([path], video=VideoOptions())
            read_rows(datasource, list_manifests(datasource))
    finally:
        decode_logger.removeHandler(caplog.handler)

    warnings = [r.getMessage() for r in caplog.records if "undecodable" in r.message]
    assert len(warnings) == 2
    for (topic, path), warning in zip(paths.items(), warnings):
        assert repr(topic) in warning and repr(path) in warning


def test_cameras_of_different_sizes_plan_a_variable_shaped_frame(tmp_path):
    """Without ``resize``, cameras of different sizes give the variable-shaped
    tensor type rather than the first camera's shape."""
    schema_name = "sensor_msgs/msg/CompressedImage"
    path = os.path.join(tmp_path, "two_cameras.mcap")
    write_payloads(
        path,
        [encode_jpeg(HEIGHT, WIDTH)] * 3,
        schema_name=schema_name,
        extra_topic=("/camera_b", schema_name, encode_jpeg(*HALF_SIZE)),
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions())
    scanner = scanner_for(datasource)

    rows = read_rows(datasource, list_manifests(datasource))

    assert "ArrowVariableShapedTensorType" in str(
        scanner.read_schema().field("frame").type
    )
    assert {row["frame"].shape for row in rows} == {FRAME_SHAPE, (*HALF_SIZE, 3)}


def test_planning_checks_distinct_files(tmp_path):
    """A schema sample that names one file once per chunk still checks the files
    after it: a second file whose topic is not video fails planning."""
    schema_name = "sensor_msgs/msg/CompressedImage"
    video = os.path.join(tmp_path, "video.mcap")
    other = os.path.join(tmp_path, "other.mcap")
    write_payloads(video, [encode_jpeg(HEIGHT, WIDTH)] * 3, schema_name=schema_name)
    write_payloads(other, [b"imu"] * 3, schema_name="sensor_msgs/msg/Imu")
    datasource = MCAPDatasourceV2([video, other], video=VideoOptions())
    # One row per chunk: the video file fills the first four sampled rows.
    paths = [video] * 4 + [other]
    sample = FileManifest.construct_manifest(
        paths=paths, sizes=[1] * len(paths), chunk_metadatas=[None] * len(paths)
    )

    with pytest.raises(ValueError, match="not a video topic"):
        datasource.infer_schema(sample)


def test_planning_checks_a_named_topic_past_the_first_files(tmp_path):
    """A named topic that only a file past the first planned ones records is
    still checked: planning reads on until each named topic is found, so a
    non-video one fails planning rather than its read task."""
    schema_name = "sensor_msgs/msg/CompressedImage"
    paths = []
    for i in range(PLANNED_FILES):
        path = os.path.join(tmp_path, f"camera_{i}.mcap")
        write_payloads(path, [encode_jpeg(HEIGHT, WIDTH)] * 3, schema_name=schema_name)
        paths.append(path)
    imu = os.path.join(tmp_path, "imu.mcap")
    write_payloads(imu, [b"imu"] * 3, schema_name="sensor_msgs/msg/Imu", topic="/imu")
    paths.append(imu)
    datasource = MCAPDatasourceV2(
        paths, topics=["/camera", "/imu"], video=VideoOptions()
    )
    sample = FileManifest.construct_manifest(
        paths=paths, sizes=[1] * len(paths), chunk_metadatas=[None] * len(paths)
    )

    with pytest.raises(ValueError, match="'/imu'.*not a video topic"):
        datasource.infer_schema(sample)


# More files than planning samples (``PLANNED_FILES``), the last one of
# another size.
CAMERA_FILES = 6


@pytest.mark.parametrize("resize", [None, HALF_SIZE], ids=["coded", "resized"])
@pytest.mark.parametrize("tensor_format", ["v1", "v2", "native"])
def test_planned_frame_type_matches_every_block(
    tmp_path, resize, tensor_format, restore_tensor_format
):
    """The planned ``frame`` type is the type of every block, in each tensor
    format, even for a file planning did not sample whose frames are another
    size: variable-shaped without ``resize``, fixed-shaped with it."""
    from ray.data._internal.tensor_extensions.arrow import FixedShapeTensorFormat
    from ray.data.context import DataContext

    DataContext.get_current().arrow_fixed_shape_tensor_format = FixedShapeTensorFormat(
        tensor_format
    )
    paths = []
    for i in range(CAMERA_FILES):
        width, height = (WIDTH, HEIGHT) if i < CAMERA_FILES - 1 else HALF_SIZE[::-1]
        path = os.path.join(tmp_path, f"camera{i}.mcap")
        packets = encode_h264(4, width=width, height=height)
        write_payloads(path, packets, schema_name="foxglove.CompressedVideo")
        paths.append(path)
    datasource = MCAPDatasourceV2(paths, video=VideoOptions(resize=resize))

    # ``read_rows`` checks each block's schema against the planned one.
    rows = read_rows(datasource, list_manifests(datasource))

    assert len(rows) == 4 * CAMERA_FILES
    planned = infer_schema(datasource).field("frame").type
    assert planned == decoded_frame_type(resize)


@pytest.mark.parametrize("index", [True, False], ids=["indexed", "unindexed"])
def test_frames_in_the_time_range_keep_their_size(tmp_path, index):
    """Frames in ``time_range`` decode at their own size, decoded from the
    keyframe before the range.

    The stream switches from 64x48 to 32x24 at frame 10, and the range starts
    at frame 12.
    """
    from mcap.writer import IndexType

    packets = encode_h264(10, width=64, height=48) + encode_h264(
        20, width=32, height=24
    )
    path = os.path.join(tmp_path, "resized.mcap")
    unindexed = dict(
        index_types=IndexType.NONE,
        use_chunking=False,
        use_statistics=False,
        use_summary_offsets=False,
    )
    write_payloads(
        path,
        packets,
        schema_name="foxglove.CompressedVideo",
        chunk_size=1,
        **({} if index else unindexed),
    )
    datasource = MCAPDatasourceV2(
        [path],
        video=VideoOptions(),
        time_range=TimeRange(12 * FRAME_NS, 30 * FRAME_NS),
    )

    rows = read_rows(datasource, list_manifests(datasource))

    assert [row["sequence"] for row in rows] == list(range(12, 30))
    assert {row["frame"].shape for row in rows} == {(24, 32, 3)}


@pytest.mark.parametrize("resize", [None, HALF_SIZE], ids=["coded", "resized"])
def test_decoded_builder_keeps_a_frame_that_did_not_decode_as_null(resize):
    """The decoded table builder gives a row added without its frame a null
    ``frame``, of the planned type even when no frame of the table decoded.
    With ``frame`` pruned, no frame is needed."""
    import numpy as np

    class Stub:
        """An object with the given attributes."""

        def __init__(self, **kw):
            self.__dict__.update(kw)

    selected = (
        None,
        Stub(id=1, topic="/cam", message_encoding="cdr", metadata={}),
        Stub(channel_id=1, log_time=0, publish_time=0, sequence=0, data=b"x"),
        "row",
    )
    frame_type = decoded_frame_type(resize)
    shape = FRAME_SHAPE if resize is None else (*resize, 3)

    def build(frames, columns=None):
        """A table of one row per entry of ``frames``."""
        builder = _MessageTableBuilder(
            columns=columns,
            include_metadata=False,
            include_row_id=False,
            decode_json=False,
            frame_type=frame_type,
        )
        for frame in frames:
            builder.add(selected, frame)  # pyrefly: ignore[bad-argument-type]
        return builder.build()

    mixed = build([np.zeros(shape, np.uint8), None])
    undecoded = build([None, None])
    pruned = build([None], columns={"topic"})

    for table in (mixed, undecoded):
        assert table.schema.field("frame").type == frame_type
    assert mixed.column("frame").null_count == 1
    assert undecoded.column("frame").null_count == 2
    assert pruned.column_names == ["topic"]


@pytest.mark.parametrize(
    "sizes", [[(48, 64)], [(48, 64), (24, 32)]], ids=["same", "mixed"]
)
def test_decoded_builder_estimates_its_frames_and_empties_on_build(h264_file, sizes):
    """The decoded table builder's ``estimated_bytes`` is within 1% of the
    table it builds, frames included, and ``build`` empties it, so a caller
    needs no reset between tables."""
    import numpy as np

    from ray.data.tests.unit.datasource_v2.mcap_testing import selected_messages

    builder = _MessageTableBuilder(
        columns=None,
        include_metadata=True,
        include_row_id=True,
        decode_json=False,
        frame_type=decoded_frame_type(None),
    )
    for index, selected in enumerate(selected_messages(h264_file)):
        height, width = sizes[index % len(sizes)]
        builder.add(selected, np.full((height, width, 3), index, np.uint8))
    estimated = builder.estimated_bytes

    table = builder.build()

    assert table.num_rows == H264_FILE_FRAMES
    assert estimated <= table.nbytes <= 1.01 * estimated
    assert (builder.num_rows, builder.estimated_bytes) == (0, 0)


def zero_chunk(path: str, index: int) -> None:
    """Zero the compressed bytes of the file's chunk ``index``, in file order."""
    chunks = sorted(summary_of(path).chunk_indexes, key=lambda c: c.chunk_start_offset)
    chunk = chunks[index]
    with open(path, "r+b") as f:
        f.seek(chunk.chunk_start_offset + chunk.chunk_length - chunk.compressed_size)
        f.write(bytes(chunk.compressed_size))


def test_planning_skips_sampled_files_it_cannot_read(h264_file, tmp_path):
    """Planning skips a sampled file whose summary or first chunk cannot be
    read, as ``inspect_sample`` does, and plans from the others. Such a file
    then fails its own read task, not the whole read."""
    folder = os.path.join(tmp_path, "recordings")
    os.makedirs(folder)
    write_cut_short(os.path.join(folder, "a.mcap"), h264_file)
    shutil.copy(h264_file, os.path.join(folder, "b.mcap"))
    zero_chunk(os.path.join(folder, "b.mcap"), 0)
    shutil.copy(h264_file, os.path.join(folder, "c.mcap"))
    datasource = MCAPDatasourceV2([folder], video=VideoOptions())

    schema = infer_schema(datasource)

    assert schema.field("frame").type == decoded_frame_type(None)


@pytest.fixture
def restore_tensor_format():
    """Put ``DataContext.arrow_fixed_shape_tensor_format`` back after the test."""
    from ray.data.context import DataContext

    saved = DataContext.get_current().arrow_fixed_shape_tensor_format
    yield
    DataContext.get_current().arrow_fixed_shape_tensor_format = saved


@pytest.fixture
def restore_max_errored_blocks():
    """Put ``DataContext.max_errored_blocks`` back after the test."""
    from ray.data.context import DataContext

    saved = DataContext.get_current().max_errored_blocks
    yield
    DataContext.get_current().max_errored_blocks = saved


@pytest.mark.parametrize("max_errored_blocks", [1, 0])
def test_a_decoding_task_reads_past_a_file_with_a_corrupt_chunk(
    h264_file, tmp_path, max_errored_blocks, restore_max_errored_blocks
):
    """A decoding task holding three files, the middle one with a corrupt chunk,
    also decodes the file after it when errors can be skipped, then raises
    naming the bad file. With ``max_errored_blocks=0`` it stops at the bad
    file."""
    from ray.data.context import DataContext

    a, b, c = (os.path.join(tmp_path, f"{name}.mcap") for name in "abc")
    for path in (a, b, c):
        shutil.copy(h264_file, path)
    # Not the first chunk, which planning reads.
    zero_chunk(b, 2)
    DataContext.get_current().max_errored_blocks = max_errored_blocks
    datasource = MCAPDatasourceV2([a, b, c], video=VideoOptions(), include_paths=True)
    # The three small files share one read task.
    (task,) = pack(datasource, preserve_order=True)

    tables = []
    with pytest.raises(Exception) as error:
        for table in scanner_for(datasource).create_reader().read(task):
            tables.append(table)

    read = [a, c] if max_errored_blocks else [a]
    paths = [path for table in tables for path in table.column("path").to_pylist()]
    assert paths == [path for path in read for _ in range(H264_FILE_FRAMES)]
    assert "b.mcap" in str(error.value)


def test_decoded_rows_are_cut_at_the_limit(h264_file, monkeypatch):
    """With a pushed-down limit a decoding task yields that many rows, and
    decodes no message after the frames that fill it."""
    converted = []
    real_to_array = FrameDecoder._to_array

    def to_array(self, frame):
        converted.append(frame.pts)
        return real_to_array(self, frame)

    monkeypatch.setattr(FrameDecoder, "_to_array", to_array)
    datasource = MCAPDatasourceV2([h264_file], video=VideoOptions())
    reader = scanner_for(datasource).push_limit(2).create_reader()

    tables = [table for task in pack(datasource) for table in reader.read(task)]

    assert sum(table.num_rows for table in tables) == 2
    # A stream without B-frames releases each frame as its message is fed.
    assert converted == [0, FRAME_NS]


def test_decoders_close_when_the_limit_stops_mid_file(h264_file, monkeypatch):
    """A limit reached at a full block stops the decoded rows mid-file. The
    task's decoders still close before its read returns."""
    from ray.data.context import DataContext

    events = []
    real_close = FrameDecoder.close

    def close(self):
        events.append("decoder closed")
        return real_close(self)

    monkeypatch.setattr(FrameDecoder, "close", close)
    # One-row blocks put the limit on a yielded table, not between messages.
    monkeypatch.setattr(DataContext.get_current(), "target_max_block_size", 1)
    datasource = MCAPDatasourceV2([h264_file], video=VideoOptions())
    reader = scanner_for(datasource).push_limit(2).create_reader()

    for task in pack(datasource):
        for table in reader.read(task):
            events.append(f"{table.num_rows} row")
    events.append("read returned")

    assert events == ["1 row", "1 row", "decoder closed", "read returned"]


def test_planning_reads_each_chunk_in_one_read(h264_file):
    """Planning reads a sampled file's summary once and each chunk it needs in
    one read, rather than record field by field."""
    from ray.data._internal.datasource_v2.common.listing_utils import sample_files

    datasource = MCAPDatasourceV2([h264_file], video=VideoOptions())
    sample = sample_files(
        datasource._get_file_indexer(), datasource.paths, datasource.filesystem, []
    )
    seek_log = SeekLog()
    datasource._filesystem = seek_log

    datasource.infer_schema(sample)

    chunks = chunks_read(seek_log, h264_file)
    # The first chunk, for the first message; a summary takes one read in a
    # small file.
    assert len(chunks) == 1
    assert seek_log.reads[h264_file] == 1 + len(chunks)


def test_a_decoding_task_reads_its_lead_in_back_to_the_keyframe(h264_file, monkeypatch):
    """A task that owns only the last chunk reads its lead-in back from it,
    nearest chunk first, in rounds of one and then two chunks. It stops at
    keyframe 20, so it never reads chunks 0 and 1. A round's chunks share one
    ranged read, as a task's own chunks do (``mcap_io.read_chunks``)."""
    datasource = MCAPDatasourceV2([h264_file], video=VideoOptions())
    # One chunk per read task.
    monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", 1)
    tasks = pack(datasource)
    seek_log = SeekLog()
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=seek_log)

    tables = list(scanner.create_reader().read(tasks[-1]))

    starts = sorted(c.chunk_start_offset for c in summary_of(h264_file).chunk_indexes)
    (owned,) = tasks[-1].file_chunk_metadatas
    assert len(tasks) > 1 and list(owned["unit_ids"]) == [starts[-1]]
    # Its own chunk 5, then chunk 4, then chunks 2-3, which hold keyframe 20.
    assert chunks_read(seek_log, h264_file) == [starts[5], starts[4], starts[2]]
    assert [s for t in tables for s in t.column("sequence").to_pylist()] == [28, 29]


DECODING_READ_BYTES = 4 * MiB  # a decoding task's largest read
DEFAULT_READ_BYTES = 16 * MiB  # the default RAY_DATA_MCAP_CHUNK_READ_BYTES
LARGE_FILE_FRAMES = 60
PAD_BYTES = 512 * KiB  # one ``/pad`` message per frame: two frames fill a chunk


class ReadSizes:
    """A local filesystem that records the size of every read, per path."""

    def __init__(self):
        self._filesystem = LocalFileSystem()
        self.sizes: Dict[str, List[int]] = defaultdict(list)

    def open_input_file(self, path):
        return _SizeLoggingFile(
            self._filesystem.open_input_file(path), self.sizes[path]
        )


class _SizeLoggingFile:
    """A file that appends the size of every read to ``sizes``."""

    def __init__(self, f, sizes: List[int]):
        self._f = f
        self._sizes = sizes

    def read(self, nbytes=-1):
        data = self._f.read(nbytes)
        self._sizes.append(len(data))
        return data

    def readinto(self, buffer):
        size = self._f.readinto(buffer)
        self._sizes.append(size)
        return size

    def __getattr__(self, name):
        return getattr(self._f, name)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self._f.close()


@pytest.fixture(scope="module")
def large_chunks_file(tmp_path_factory):
    """60 H.264 frames on ``/camera``, each followed by 512 KiB on ``/pad``, in
    uncompressed chunks of about 1 MiB. Its 30 MiB of chunks take reads of 4, 8
    and 16 MiB in a row at the default read size (``mcap_io.ChunkReads``)::

        chunk      0              1              ...  29
        messages   f0 p0 f1 p1    f2 p2 f3 p3    ...  f58 p58 f59 p59
    """
    from mcap.writer import CompressionType

    path = str(tmp_path_factory.mktemp("large") / "large.mcap")
    write_payloads(
        path,
        encode_h264(LARGE_FILE_FRAMES),
        schema_name="foxglove.CompressedVideo",
        extra_topic=("/pad", "pad", bytes(PAD_BYTES)),
        compression=CompressionType.NONE,
    )
    return path


def read_with_sizes(datasource, path) -> Tuple[List[pa.Table], List[int]]:
    """Every table of ``datasource``'s read tasks, and the size of each read of
    ``path`` they made."""
    read_sizes = ReadSizes()
    scanner = dataclasses.replace(scanner_for(datasource), filesystem=read_sizes)
    reader = scanner.create_reader()
    tables = [table for task in pack(datasource) for table in reader.read(task)]
    return tables, read_sizes.sizes[path]


def test_decoding_tasks_read_at_most_4_mib_at_a_time(large_chunks_file):
    """Decoded message rows read a file in reads of at most 4 MiB, each still
    several chunks long. Message rows without video read the same file in
    reads of up to 16 MiB."""
    decoded = MCAPDatasourceV2(
        [large_chunks_file], topics=["/camera"], video=VideoOptions()
    )
    plain = MCAPDatasourceV2([large_chunks_file])

    tables, decoded_reads = read_with_sizes(decoded, large_chunks_file)
    _, plain_reads = read_with_sizes(plain, large_chunks_file)

    assert sum(table.num_rows for table in tables) == LARGE_FILE_FRAMES
    # Three chunks of about 1 MiB fit in a read, a fourth does not.
    assert 2 * MiB < max(decoded_reads) <= DECODING_READ_BYTES
    # The third read of the file may span 16 MiB.
    assert 8 * MiB < max(plain_reads) <= DEFAULT_READ_BYTES


def test_a_lower_chunk_read_size_also_lowers_decoding_reads():
    """``RAY_DATA_MCAP_CHUNK_READ_BYTES`` below 4 MiB lowers a decoding task's
    largest read too. The setting is read at import, so a fresh interpreter
    checks it."""
    code = (
        "from ray.data._internal.datasource_v2.formats.mcap import mcap_reader\n"
        "print(mcap_reader._DECODING_CHUNK_READ_BYTES)"
    )
    env = {**os.environ, "RAY_DATA_MCAP_CHUNK_READ_BYTES": str(1 * MiB)}

    out = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert int(out.stdout.split()[-1]) == 1 * MiB


def write_unindexed(tmp_path) -> str:
    """The H.264 camera in a file without chunks, an index or a summary."""
    from mcap.writer import IndexType

    path = os.path.join(tmp_path, "unindexed.mcap")
    write_payloads(
        path,
        encode_h264(H264_FILE_FRAMES),
        schema_name="foxglove.CompressedVideo",
        index_types=IndexType.NONE,
        use_chunking=False,
        use_statistics=False,
        use_summary_offsets=False,
    )
    return path


def test_unindexed_file_is_held_once_for_decoding(tmp_path, monkeypatch):
    """A decoded read of a file without an index streams it in file order and
    sorts what it keeps, rather than having the scan sort it too."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_chunks

    path = write_unindexed(tmp_path)
    orders = []
    real_scan = mcap_chunks.SelectedMessageReader.iter_unindexed

    def iter_unindexed(self, *args, **kwargs):
        orders.append(kwargs.get("log_time_order"))
        return real_scan(self, *args, **kwargs)

    monkeypatch.setattr(
        mcap_chunks.SelectedMessageReader, "iter_unindexed", iter_unindexed
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions())

    rows = read_rows(datasource, list_manifests(datasource))

    assert [row["sequence"] for row in rows] == list(range(H264_FILE_FRAMES))
    assert orders == [False]


def test_unindexed_file_is_read_through_the_buffer_for_decoding(tmp_path):
    """A decoded read of a file without an index reads it through the read
    buffer, in as few reads as a read without video."""
    path = write_unindexed(tmp_path)

    tables, decoded_reads = read_with_sizes(
        MCAPDatasourceV2([path], video=VideoOptions()), path
    )
    _, plain_reads = read_with_sizes(MCAPDatasourceV2([path]), path)

    assert sum(table.num_rows for table in tables) == H264_FILE_FRAMES
    # The whole file fits in one buffered read. Unbuffered, the scan reads it
    # record by record.
    assert len(decoded_reads) == len(plain_reads) == 1


def test_frame_pruned_away_skips_decoding(h264_file):
    """With ``frame`` pruned, a decoded read yields the other columns."""
    datasource = MCAPDatasourceV2([h264_file], video=VideoOptions())

    rows = read_rows(
        datasource, list_manifests(datasource), columns=["topic", "sequence"]
    )

    assert len(rows) == H264_FILE_FRAMES and set(rows[0]) == {"topic", "sequence"}


def test_decode_rejects_non_video_topics(tmp_path):
    """Decoding fails on a non-video topic, and on a video topic whose bytes match
    no codec, naming the topic."""
    path = os.path.join(tmp_path, "imu.mcap")
    write_payloads(
        path,
        [b"\x01\x02\x03\x04" * 8] * 5,
        schema_name="sensor_msgs/msg/Imu",
        topic="/imu",
    )
    datasource = MCAPDatasourceV2([path], video=VideoOptions())
    with pytest.raises(ValueError, match="'/imu'.*not a video topic"):
        read_rows(datasource, list_manifests(datasource))

    opaque = os.path.join(tmp_path, "opaque.mcap")
    write_payloads(
        opaque,
        [b"\x01\x02\x03\x04" * 8] * 5,
        schema_name="foxglove.CompressedVideo",
        topic="/cam",
    )
    datasource = MCAPDatasourceV2([opaque], video=VideoOptions())
    with pytest.raises(ValueError, match="'/cam'.*not JPEG, PNG, H.264"):
        read_rows(datasource, list_manifests(datasource))


def test_decode_planning_ignores_topics_outside_the_time_range(tmp_path):
    """A non-video topic with no message in ``time_range`` does not fail planning,
    since the read never returns its messages."""
    import numpy as np
    from mcap.writer import CompressionType, Writer
    from PIL import Image

    buffer = io.BytesIO()
    Image.fromarray(np.zeros((24, 32, 3), dtype=np.uint8)).save(buffer, format="JPEG")
    jpeg = buffer.getvalue()
    path = os.path.join(tmp_path, "camera_then_imu.mcap")
    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=1, compression=CompressionType.ZSTD)
        writer.start(profile="", library="ray-test")
        camera = writer.register_channel(
            schema_id=writer.register_schema(
                name="sensor_msgs/msg/CompressedImage", encoding="ros2msg", data=b"x"
            ),
            topic="/camera",
            message_encoding="cdr",
        )
        imu = writer.register_channel(
            schema_id=writer.register_schema(
                name="sensor_msgs/msg/Imu", encoding="ros2msg", data=b"x"
            ),
            topic="/imu",
            message_encoding="cdr",
        )
        for i in range(10):
            t = i * FRAME_NS
            writer.add_message(channel_id=camera, log_time=t, publish_time=t, data=jpeg)
            if i < 3:
                writer.add_message(
                    channel_id=imu, log_time=t, publish_time=t, data=bytes(32)
                )
        writer.finish()

    ranged = MCAPDatasourceV2(
        [path],
        video=VideoOptions(),
        time_range=TimeRange(5 * FRAME_NS, 10 * FRAME_NS),
    )
    rows = read_rows(ranged, list_manifests(ranged))
    assert [row["log_time"] for row in rows] == [i * FRAME_NS for i in range(5, 10)]

    whole = MCAPDatasourceV2([path], video=VideoOptions())
    with pytest.raises(ValueError, match="'/imu'.*not a video topic"):
        read_rows(whole, list_manifests(whole))


def test_video_option_validation(h264_file):
    """``VideoOptions`` rejects a bad ``resize``. Decoding applies to message
    rows, not to attachment or metadata rows."""
    with pytest.raises(ValueError, match="height, width"):
        VideoOptions(resize=(10, 0))
    assert VideoOptions(resize=(24, 32)).resize == (24, 32)

    MCAPDatasourceV2([h264_file], video=VideoOptions())
    for granularity in ("attachment", "metadata"):
        with pytest.raises(ValueError, match="does not apply"):
            MCAPDatasourceV2(
                [h264_file], read_granularity=granularity, video=VideoOptions()
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
