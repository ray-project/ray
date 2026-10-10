"""Unit tests for decoded MCAP message rows (``VideoOptions``).

They cover the decoded ``frame`` column, the codec checks at planning,
``resize``, stills, and the planned frame shape. No Ray cluster: the indexer,
scanner and reader are driven directly.
"""

import importlib.util
import io
import os

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    TimeRange,
    VideoOptions,
    WindowSpec,
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
    HALF_SIZE,
    HEIGHT,
    VP9_KEYFRAME,
    VP9_PFRAME,
    WIDTH,
    encode_h264,
    infer_schema,
    list_manifests,
    listed_if_custom,
    read_rows,
    scanner_for,
    split_at_first_idr,
    summary_of,
    window_datasource,
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
    assert str(scanner.read_schema().field("frame").type).startswith(
        "ArrowTensorTypeV2"
    )


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


def test_planning_decodes_parameter_sets_written_separately(tmp_path):
    """Planning decodes the first frame when SPS and PPS sit in their own message
    before the keyframe. That message yields no row, with or without ``frame``."""
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

    schema = infer_schema(datasource)
    assert f"shape={FRAME_SHAPE}" in str(schema.field("frame").type)
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
    """An unlisted topic with a custom schema name fails decoded planning, and a
    window read keeps it in the message lists. Listed in ``video_topics``, it is
    decoded at message and window granularity, its codec told from its bytes."""
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

    windows = window_datasource(path, length_s=0.33, video_topics=["/camera"])
    scanner = scanner_for(windows)
    rows = read_rows(windows, list_manifests(windows))
    assert scanner.read_schema().names[-2:] == ["frames:/camera", "frame_times:/camera"]
    assert sum(len(row["frame_times:/camera"]) for row in rows) == 30

    plain = window_datasource(path, length_s=0.33)
    scanner = scanner_for(plain)
    rows = read_rows(plain, list_manifests(plain))
    assert not any(name.startswith("frames:") for name in scanner.read_schema().names)
    assert sum(row["num_messages"] for row in rows) == 30


def test_planning_reads_video_schemas_written_only_in_chunks(tmp_path):
    """A file written with ``repeat_schemas=False`` keeps its schema records in
    the chunks. Planning reads them there, so a known video schema needs no
    ``video_topics`` entry, at message and window granularity."""
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

    assert infer_schema(datasource).field("frame").type.shape == FRAME_SHAPE
    rows = read_rows(datasource, list_manifests(datasource))
    assert len(rows) == 10

    windows = window_datasource(path, length_s=1.0)
    assert infer_schema(windows).names[-2:] == [
        "frames:/camera",
        "frame_times:/camera",
    ]


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
        tables = scanner.create_reader().read(manifest)
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


def test_decode_resize_and_fps(h264_file):
    """``resize`` sets the frame shape, and ``fps`` keeps one frame per interval
    however the read is split."""
    resized = MCAPDatasourceV2([h264_file], video=VideoOptions(resize=HALF_SIZE))
    scanner = scanner_for(resized)
    rows = read_rows(resized, list_manifests(resized))
    assert len(rows) == H264_FILE_FRAMES and rows[0]["frame"].shape == (*HALF_SIZE, 3)
    assert f"shape={(*HALF_SIZE, 3)}" in str(scanner.read_schema().field("frame").type)

    # 30 frames at 33 ms span ten 100 ms intervals: one frame survives in each.
    thinned = MCAPDatasourceV2([h264_file], video=VideoOptions(fps=10))
    rows = read_rows(thinned, list_manifests(thinned))
    assert len(rows) == 10
    assert [row["log_time"] // 100_000_000 for row in rows] == list(range(10))

    # The same frames are kept however the read is split, with or without ``frame``.
    (manifest,) = list_manifests(thinned)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    parts = [FileManifest(block.slice(0, 2)), FileManifest(block.slice(2))]
    split = read_rows(thinned, parts)
    assert sorted(r["sequence"] for r in split) == [r["sequence"] for r in rows]
    pruned = read_rows(thinned, [manifest], columns=["sequence"])
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


def test_corrupt_still_is_skipped_not_fatal(tmp_path):
    """One truncated JPEG costs one frame, not the task."""
    import numpy as np
    from PIL import Image

    payloads = []
    for i in range(6):
        buffer = io.BytesIO()
        Image.fromarray(np.full(FRAME_SHAPE, i * 20, dtype=np.uint8)).save(
            buffer, format="JPEG"
        )
        payloads.append(buffer.getvalue())
    good = list(payloads)
    payloads[2] = payloads[2][:40]  # still sniffs as JPEG, cannot be decoded
    assert detect_codec(payloads[2]) is VideoCodec.JPEG
    path = os.path.join(tmp_path, "corrupt.mcap")
    write_payloads(path, payloads, schema_name="sensor_msgs/msg/CompressedImage")
    datasource = MCAPDatasourceV2([path], video=VideoOptions())
    rows = read_rows(datasource, list_manifests(datasource))
    assert [row["sequence"] for row in rows] == [0, 1, 3, 4, 5]

    # With fps=10 frames 4 and 5 share an interval, which a corrupt 4 leaves to 5.
    payloads[2], payloads[4] = good[2], payloads[4][:40]
    path = os.path.join(tmp_path, "corrupt_fps.mcap")
    write_payloads(path, payloads, schema_name="sensor_msgs/msg/CompressedImage")
    thinned = MCAPDatasourceV2([path], video=VideoOptions(fps=10))
    rows = read_rows(thinned, list_manifests(thinned))
    assert [row["sequence"] for row in rows] == [0, 5]


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


@pytest.mark.parametrize("index", [True, False], ids=["indexed", "unindexed"])
def test_frame_shape_is_planned_from_the_time_range(tmp_path, index):
    """The planned frame shape is that of the frames in ``time_range``.

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

    assert infer_schema(datasource).field("frame").type.shape == (24, 32, 3)
    rows = read_rows(datasource, list_manifests(datasource))
    assert {row["frame"].shape for row in rows} == {(24, 32, 3)}


@pytest.mark.parametrize("codec", ["h264", "jpeg"])
def test_frame_shape_is_planned_from_a_keyframe_that_opens_the_range(tmp_path, codec):
    """The stream switches from 64x48 to 32x24 at keyframe 10, just inside the
    range, so the planned shape is its own, not that of the keyframe before."""
    if codec == "h264":
        payloads = encode_h264(10, width=64, height=48) + encode_h264(
            20, width=32, height=24
        )
        schema_name = "foxglove.CompressedVideo"
    else:
        payloads = [encode_jpeg(48, 64)] * 10 + [encode_jpeg(24, 32)] * 20
        schema_name = "sensor_msgs/msg/CompressedImage"
    path = os.path.join(tmp_path, "resized.mcap")
    write_payloads(path, payloads, schema_name=schema_name, chunk_size=1)
    datasource = MCAPDatasourceV2(
        [path],
        video=VideoOptions(),
        time_range=TimeRange(10 * FRAME_NS - 1, 30 * FRAME_NS),
    )

    assert infer_schema(datasource).field("frame").type.shape == (24, 32, 3)
    rows = read_rows(datasource, list_manifests(datasource))
    assert {row["frame"].shape for row in rows} == {(24, 32, 3)}


def test_decoded_builder_refuses_a_row_without_its_frame():
    """The decoded table builder refuses a row that has no frame, unless
    ``frame`` is pruned."""
    from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
        _MessageTableBuilder,
    )

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
    wanted = _MessageTableBuilder(
        columns=None,
        include_metadata=False,
        include_row_id=False,
        decode_json=False,
        decoded=True,
    )
    with pytest.raises(AssertionError, match="without its frame"):
        wanted.add(selected)  # pyrefly: ignore[bad-argument-type]

    pruned = _MessageTableBuilder(
        columns={"topic"},
        include_metadata=False,
        include_row_id=False,
        decode_json=False,
        decoded=True,
    )
    pruned.add(selected)  # pyrefly: ignore[bad-argument-type]
    assert pruned.build().column_names == ["topic"]


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
    """``VideoOptions`` rejects a bad ``fps`` or ``resize``, and decoding applies
    only to message and window rows."""
    with pytest.raises(ValueError, match="positive number"):
        VideoOptions(fps=0)
    # Each would give a 0 ns interval, or none, and break thinning.
    for fps in (float("inf"), float("nan"), 2e9):
        with pytest.raises(ValueError, match="at least one nanosecond"):
            VideoOptions(fps=fps)
    with pytest.raises(ValueError, match="height, width"):
        VideoOptions(resize=(10, 0))
    assert VideoOptions(fps=5).fps_interval_ns == 200_000_000
    assert VideoOptions(resize=(24, 32)).resize == (24, 32)

    # Only message and window rows bound the number of frames a row holds.
    MCAPDatasourceV2(
        [h264_file],
        read_granularity="window",
        window=WindowSpec(length_s=1),
        video=VideoOptions(),
    )
    for granularity in ("topic", "file", "attachment", "metadata"):
        with pytest.raises(ValueError, match="cannot be cut into blocks"):
            MCAPDatasourceV2(
                [h264_file], read_granularity=granularity, video=VideoOptions()
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
