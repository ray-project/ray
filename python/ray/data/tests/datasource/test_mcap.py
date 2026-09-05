import importlib.util
import json
import os

import pytest

import ray
from ray.data._internal.datasource.mcap_datasource import (
    _annexb_nal_types,
    _is_annexb_keyframe,
)
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# Skip all tests if mcap is not available
MCAP_AVAILABLE = importlib.util.find_spec("mcap") is not None
AV_AVAILABLE = importlib.util.find_spec("av") is not None
requires_av = pytest.mark.skipif(
    not AV_AVAILABLE, reason="av not available. Install with: pip install av"
)
pytestmark = pytest.mark.skipif(
    not MCAP_AVAILABLE,
    reason="mcap module not available. Install with: pip install mcap",
)


def create_test_mcap_file(file_path: str, messages: list) -> None:
    """Create a test MCAP file with given messages."""
    from mcap.writer import Writer

    with open(file_path, "wb") as stream:
        writer = Writer(stream)
        writer.start(profile="", library="ray-test")

        # Register schema
        schema_id = writer.register_schema(
            name="test_schema",
            encoding="jsonschema",
            data=json.dumps(
                {
                    "type": "object",
                    "properties": {
                        "value": {"type": "number"},
                        "name": {"type": "string"},
                    },
                }
            ).encode(),
        )

        # Register channels and write messages
        channels = {}
        for msg in messages:
            topic = msg["topic"]
            if topic not in channels:
                channels[topic] = writer.register_channel(
                    schema_id=schema_id,
                    topic=topic,
                    message_encoding="json",
                )

            writer.add_message(
                channel_id=channels[topic],
                log_time=msg["log_time"],
                publish_time=msg.get("publish_time", msg["log_time"]),
                data=json.dumps(msg["data"]).encode(),
            )

        writer.finish()


@pytest.fixture
def simple_mcap_file(tmp_path):
    """Fixture providing a simple MCAP file with one message."""
    path = os.path.join(tmp_path, "test.mcap")
    messages = [
        {
            "topic": "/test",
            "data": {"value": 1},
            "log_time": 1000000000,
        }
    ]
    create_test_mcap_file(path, messages)
    return path


@pytest.fixture
def basic_mcap_file(tmp_path):
    """Fixture providing a basic MCAP file with two different topics."""
    path = os.path.join(tmp_path, "test.mcap")
    messages = [
        {
            "topic": "/camera/image",
            "data": {"frame_id": 1, "timestamp": 1000},
            "log_time": 1000000000,
        },
        {
            "topic": "/lidar/points",
            "data": {"point_count": 1024, "timestamp": 2000},
            "log_time": 2000000000,
        },
    ]
    create_test_mcap_file(path, messages)
    return path


@pytest.fixture
def multi_topic_mcap_file(tmp_path):
    """Fixture providing an MCAP file with 9 messages across 3 topics."""
    path = os.path.join(tmp_path, "multi_topic.mcap")
    base_time = 1000000000
    messages = []
    for i in range(9):
        topics = ["/topic_a", "/topic_b", "/topic_c"]
        topic = topics[i % 3]
        messages.append(
            {
                "topic": topic,
                "data": {"seq": i, "topic": topic},
                "log_time": base_time + i * 1000000,
            }
        )
    create_test_mcap_file(path, messages)
    return path


@pytest.fixture
def time_series_mcap_file(tmp_path):
    """Fixture providing an MCAP file with 10 time-sequenced messages."""
    path = os.path.join(tmp_path, "time_test.mcap")
    base_time = 1000000000
    messages = [
        {
            "topic": "/test_topic",
            "data": {"seq": i},
            "log_time": base_time + i * 1000000,
        }
        for i in range(10)
    ]
    create_test_mcap_file(path, messages)
    return path, base_time


def test_read_mcap_basic(ray_start_regular_shared, basic_mcap_file):
    """Test basic MCAP file reading."""
    ds = ray.data.read_mcap(basic_mcap_file)

    # Test metadata operations
    assert ds.count() == 2
    assert ds.input_files() == [_unwrap_protocol(basic_mcap_file)]

    # Verify basic fields are present
    rows = ds.take_all()
    for row in rows:
        assert "data" in row
        assert "topic" in row
        assert "log_time" in row
        assert "publish_time" in row


def test_read_mcap_multiple_files(ray_start_regular_shared, tmp_path):
    """Test reading multiple MCAP files."""
    paths = []
    for i in range(2):
        path = os.path.join(tmp_path, f"test_{i}.mcap")
        messages = [
            {
                "topic": f"/test_{i}",
                "data": {"file_id": i},
                "log_time": 1000000000 + i * 1000000,
            }
        ]
        create_test_mcap_file(path, messages)
        paths.append(path)

    ds = ray.data.read_mcap(paths)
    assert ds.count() == 2
    assert set(ds.input_files()) == {_unwrap_protocol(p) for p in paths}

    rows = ds.take_all()
    file_ids = {row["data"]["file_id"] for row in rows}
    assert file_ids == {0, 1}


def test_read_mcap_directory(ray_start_regular_shared, tmp_path):
    """Test reading MCAP files from a directory."""
    # Create MCAP files in directory
    for i in range(2):
        path = os.path.join(tmp_path, f"data_{i}.mcap")
        messages = [
            {
                "topic": f"/dir_test_{i}",
                "data": {"index": i},
                "log_time": 1000000000 + i * 1000000,
            }
        ]
        create_test_mcap_file(path, messages)

    ds = ray.data.read_mcap(tmp_path)
    assert ds.count() == 2


def test_read_mcap_topic_filtering(ray_start_regular_shared, multi_topic_mcap_file):
    """Test filtering by topics."""
    # Test topic filtering
    topics = {"/topic_a", "/topic_b"}
    ds = ray.data.read_mcap(multi_topic_mcap_file, topics=topics)

    rows = ds.take_all()
    actual_topics = {row["topic"] for row in rows}
    assert actual_topics.issubset(topics)
    assert len(rows) == 6  # 2/3 of messages


def test_read_mcap_time_range_filtering(
    ray_start_regular_shared, time_series_mcap_file
):
    """Test filtering by time range."""
    path, base_time = time_series_mcap_file

    # Filter to first 5 messages
    time_range = (base_time, base_time + 5000000)
    ds = ray.data.read_mcap(path, time_range=time_range)

    rows = ds.take_all()
    assert len(rows) <= 5
    for row in rows:
        assert base_time <= row["log_time"] <= base_time + 5000000


def test_read_mcap_message_type_filtering(ray_start_regular_shared, simple_mcap_file):
    """Test filtering by message types."""
    # Filter with existing schema
    ds = ray.data.read_mcap(simple_mcap_file, message_types={"test_schema"})
    assert ds.count() == 1

    # Filter with non-existent schema
    ds = ray.data.read_mcap(simple_mcap_file, message_types={"nonexistent"})
    assert ds.count() == 0


@pytest.mark.parametrize("include_metadata", [True, False])
def test_read_mcap_include_metadata(
    ray_start_regular_shared, simple_mcap_file, include_metadata
):
    """Test include_metadata option."""
    ds = ray.data.read_mcap(simple_mcap_file, include_metadata=include_metadata)
    rows = ds.take_all()

    if include_metadata:
        assert "schema_name" in rows[0]
        assert "channel_id" in rows[0]
    else:
        assert "schema_name" not in rows[0]
        assert "channel_id" not in rows[0]


def test_read_mcap_include_paths(ray_start_regular_shared, simple_mcap_file):
    """Test include_paths option."""
    ds = ray.data.read_mcap(simple_mcap_file, include_paths=True)
    rows = ds.take_all()

    for row in rows:
        assert "path" in row
        assert simple_mcap_file in row["path"]


def test_read_mcap_invalid_time_range(ray_start_regular_shared, simple_mcap_file):
    """Test validation of time range parameters."""
    # Start time >= end time
    with pytest.raises(ValueError, match="start_time must be less than end_time"):
        ray.data.read_mcap(simple_mcap_file, time_range=(2000, 1000))

    # Negative times
    with pytest.raises(ValueError, match="time values must be non-negative"):
        ray.data.read_mcap(simple_mcap_file, time_range=(-1000, 2000))


def test_read_mcap_missing_dependency(ray_start_regular_shared, simple_mcap_file):
    """Test graceful failure when mcap library is missing."""
    from unittest.mock import patch

    with patch.dict("sys.modules", {"mcap": None}):
        with pytest.raises(ImportError, match="MCAPDatasource.*depends on 'mcap'"):
            ray.data.read_mcap(simple_mcap_file)


def test_read_mcap_nonexistent_file(ray_start_regular_shared):
    """Test handling of nonexistent files."""
    with pytest.raises(Exception):  # FileNotFoundError or similar
        ds = ray.data.read_mcap("/nonexistent/file.mcap")
        ds.materialize()  # Force execution


@pytest.mark.parametrize("override_num_blocks", [1, 2])
def test_read_mcap_override_num_blocks(
    ray_start_regular_shared, tmp_path, override_num_blocks
):
    """Test override_num_blocks parameter."""
    path = os.path.join(tmp_path, "blocks_test.mcap")
    messages = [
        {
            "topic": "/test",
            "data": {"seq": i},
            "log_time": 1000000000 + i * 1000000,
        }
        for i in range(3)
    ]
    create_test_mcap_file(path, messages)

    ds = ray.data.read_mcap(path, override_num_blocks=override_num_blocks)

    # Should still read all the data
    assert ds.count() == 3
    rows = ds.take_all()
    assert len(rows) == 3


def test_read_mcap_file_extensions(ray_start_regular_shared, tmp_path):
    """Test file extension filtering."""
    # Create MCAP file
    mcap_path = os.path.join(tmp_path, "data.mcap")
    messages = [
        {
            "topic": "/test",
            "data": {"test": "mcap_data"},
            "log_time": 1000000000,
        }
    ]
    create_test_mcap_file(mcap_path, messages)

    # Create non-MCAP file
    other_path = os.path.join(tmp_path, "data.txt")
    with open(other_path, "w") as f:
        f.write("not mcap data")

    # Should only read .mcap files by default
    ds = ray.data.read_mcap(tmp_path)
    assert ds.count() == 1
    rows = ds.take_all()
    assert rows[0]["data"]["test"] == "mcap_data"


@pytest.mark.parametrize("ignore_missing_paths", [True, False])
def test_read_mcap_ignore_missing_paths(
    ray_start_regular_shared, simple_mcap_file, ignore_missing_paths
):
    """Test ignore_missing_paths parameter."""
    paths = [simple_mcap_file, "/nonexistent/missing.mcap"]

    if ignore_missing_paths:
        ds = ray.data.read_mcap(paths, ignore_missing_paths=ignore_missing_paths)
        assert ds.count() == 1
        assert ds.input_files() == [_unwrap_protocol(simple_mcap_file)]
    else:
        with pytest.raises(Exception):  # FileNotFoundError or similar
            ds = ray.data.read_mcap(paths, ignore_missing_paths=ignore_missing_paths)
            ds.materialize()


def test_read_mcap_json_decoding(ray_start_regular_shared, tmp_path):
    """Test that JSON-encoded messages are properly decoded."""
    path = os.path.join(tmp_path, "json_test.mcap")

    # Test data with nested JSON structure
    test_data = {
        "sensor_data": {
            "temperature": 23.5,
            "humidity": 45.0,
            "readings": [1, 2, 3, 4, 5],
        },
        "metadata": {"device_id": "sensor_001", "location": "room_a"},
    }

    messages = [
        {
            "topic": "/sensor/data",
            "data": test_data,
            "log_time": 1000000000,
        }
    ]

    create_test_mcap_file(path, messages)
    assert os.path.exists(path), f"Test MCAP file was not created at {path}"

    ds = ray.data.read_mcap(path)
    rows = ds.take_all()

    assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
    row = rows[0]

    # Verify the data field is properly decoded as a Python dict, not bytes
    assert isinstance(row["data"], dict), f"Expected dict, got {type(row['data'])}"
    assert row["data"]["sensor_data"]["temperature"] == 23.5
    assert row["data"]["metadata"]["device_id"] == "sensor_001"
    assert row["data"]["sensor_data"]["readings"] == [1, 2, 3, 4, 5]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))


# ---------------------------------------------------------------------------
# Video decoding
# ---------------------------------------------------------------------------

GOP_SIZE = 10


def _encode_h264(num_frames: int, width: int = 64, height: int = 48):
    """Encode frames to H.264 Annex-B access units, one per returned packet."""
    import io

    import av
    import numpy as np

    container = av.open(io.BytesIO(), mode="w", format="h264")
    stream = container.add_stream("libx264", rate=30)
    stream.width, stream.height, stream.pix_fmt = width, height, "yuv420p"
    # A fixed GOP with no B-frames keeps the keyframe positions predictable, so
    # a test can open a window at a known offset from one.
    stream.options = {"g": str(GOP_SIZE), "bf": "0", "tune": "zerolatency"}

    packets = []
    for i in range(num_frames):
        array = np.full((height, width, 3), i * 8 % 256, dtype=np.uint8)
        array[:, :, 1] = (i * 3) % 256
        frame = av.VideoFrame.from_ndarray(array, format="rgb24")
        packets.extend(bytes(p) for p in stream.encode(frame))
    packets.extend(bytes(p) for p in stream.encode())
    return packets


def _write_payload_mcap(path, payloads, *, schema_name, topic="/camera"):
    from mcap.writer import Writer

    with open(path, "wb") as stream:
        writer = Writer(stream)
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name=schema_name, encoding="ros2msg", data=b"video\n"
        )
        channel_id = writer.register_channel(
            schema_id=schema_id, topic=topic, message_encoding="cdr"
        )
        for i, payload in enumerate(payloads):
            writer.add_message(
                channel_id=channel_id,
                log_time=i * 33000000,
                publish_time=i * 33000000,
                data=payload,
                sequence=i,
            )
        writer.finish()


@pytest.fixture
def h264_mcap_file(tmp_path):
    """30 H.264 access units with a keyframe every `GOP_SIZE` frames."""
    path = os.path.join(tmp_path, "h264.mcap")
    _write_payload_mcap(path, _encode_h264(30), schema_name="foxglove.CompressedVideo")
    return path


@pytest.fixture
def jpeg_mcap_file(tmp_path):
    """10 JPEGs behind a header, the ROS 2 sensor_msgs/CompressedImage layout."""
    import io

    import numpy as np
    from PIL import Image

    payloads = []
    for i in range(10):
        array = np.full((48, 64, 3), i * 20 % 256, dtype=np.uint8)
        buffer = io.BytesIO()
        Image.fromarray(array).save(buffer, format="JPEG")
        # A CDR header in front of the JPEG, so the SOI marker is not at offset 0.
        payloads.append(b"\x00\x01\x00\x00" + b"header" * 4 + buffer.getvalue())
    _write_payload_mcap(
        path=os.path.join(tmp_path, "jpeg.mcap"),
        payloads=payloads,
        schema_name="sensor_msgs/msg/CompressedImage",
    )
    return os.path.join(tmp_path, "jpeg.mcap")


@requires_av
def test_annexb_keyframe_detection(ray_start_regular_shared):
    """Keyframes must be found at the encoder's GOP boundaries.

    Everything about seeking depends on this, so it is asserted directly rather
    than only through a decode.
    """
    packets = _encode_h264(30)
    keyframes = [i for i, p in enumerate(packets) if _is_annexb_keyframe(p)]

    assert keyframes == list(range(0, len(packets), GOP_SIZE))
    # An IDR access unit carries the parameter sets; an inter frame does not.
    assert set(_annexb_nal_types(packets[0])) >= {5, 7, 8}
    assert not _is_annexb_keyframe(packets[1])


@requires_av
def test_read_mcap_decode_video_yields_frames(ray_start_regular_shared, h264_mcap_file):
    """`decode_video` replaces the undecoded payload with a decoded frame."""
    raw = ray.data.read_mcap(h264_mcap_file).materialize()
    assert "data" in raw.take(1)[0]

    decoded = ray.data.read_mcap(h264_mcap_file, decode_video=True).materialize()
    row = decoded.take(1)[0]

    assert decoded.count() == 30
    assert "frame" in row and "data" not in row
    assert row["frame"].shape == (48, 64, 3)
    assert row["frame"].dtype.name == "uint8"
    # The message columns survive; only the payload is replaced.
    assert {"topic", "log_time", "publish_time", "sequence"} <= set(row)


@requires_av
def test_read_mcap_decode_video_seeks_to_keyframe(
    ray_start_regular_shared, h264_mcap_file
):
    """A `time_range` opening mid-GOP must still decode.

    The first access unit at or after `start_time` is an inter frame, which
    cannot be decoded alone. Decoding has to begin at the preceding keyframe and
    discard the frames before the window. Without that, this window yields
    nothing -- which is exactly what a downstream decoder handed a mid-GOP block
    would produce, silently.
    """
    start_frame = GOP_SIZE + GOP_SIZE // 2  # deliberately not a keyframe
    end_frame = start_frame + 5
    time_range = (start_frame * 33000000, end_frame * 33000000)

    decoded = ray.data.read_mcap(
        h264_mcap_file, decode_video=True, time_range=time_range
    ).materialize()
    sequences = sorted(row["sequence"] for row in decoded.take_all())

    assert sequences == list(range(start_frame, end_frame))


@requires_av
def test_read_mcap_decode_video_resize(ray_start_regular_shared, h264_mcap_file):
    """`resize` takes (height, width), matching `read_videos`."""
    decoded = ray.data.read_mcap(
        h264_mcap_file, decode_video=True, resize=(24, 32)
    ).materialize()

    assert decoded.take(1)[0]["frame"].shape == (24, 32, 3)
    assert decoded.count() == 30


@requires_av
def test_read_mcap_decode_video_fps(ray_start_regular_shared, h264_mcap_file):
    """`fps` subsamples against log time, not frame index."""
    full = ray.data.read_mcap(h264_mcap_file, decode_video=True).materialize()
    # 30 frames 33 ms apart is just under a second, so 10 fps keeps roughly ten.
    thinned = ray.data.read_mcap(
        h264_mcap_file, decode_video=True, fps=10
    ).materialize()

    assert 0 < thinned.count() < full.count()
    times = sorted(row["log_time"] for row in thinned.take_all())
    assert all(b - a >= 1e9 / 10 for a, b in zip(times, times[1:]))


@requires_av
def test_read_mcap_decode_video_embedded_jpeg(ray_start_regular_shared, jpeg_mcap_file):
    """A JPEG behind a header decodes; sniffing finds the marker past offset 0."""
    decoded = ray.data.read_mcap(jpeg_mcap_file, decode_video=True).materialize()

    assert decoded.count() == 10
    assert decoded.take(1)[0]["frame"].shape == (48, 64, 3)


@requires_av
def test_read_mcap_decode_video_rejects_non_video(
    ray_start_regular_shared, simple_mcap_file
):
    """A payload that is not video must fail loudly, naming the way out."""
    with pytest.raises(Exception, match="Cannot decode this payload as video"):
        ray.data.read_mcap(simple_mcap_file, decode_video=True).materialize()


def test_read_mcap_fps_and_resize_require_decode_video(
    ray_start_regular_shared, simple_mcap_file
):
    """`fps` and `resize` are meaningless without decoding, so they are refused."""
    with pytest.raises(ValueError, match="only apply when `decode_video=True`"):
        ray.data.read_mcap(simple_mcap_file, fps=5)
    with pytest.raises(ValueError, match="only apply when `decode_video=True`"):
        ray.data.read_mcap(simple_mcap_file, resize=(10, 10))


@requires_av
def test_read_mcap_decode_video_validates_arguments(
    ray_start_regular_shared, h264_mcap_file
):
    with pytest.raises(ValueError, match="positive integer"):
        ray.data.read_mcap(h264_mcap_file, decode_video=True, fps=0)
    with pytest.raises(ValueError, match="height, width"):
        ray.data.read_mcap(h264_mcap_file, decode_video=True, resize=(10, 0))
