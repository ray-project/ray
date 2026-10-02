import importlib.util
import json
import os

import pytest

import ray
from ray.data.datasource.path_util import (
    _resolve_paths_and_filesystem,
    _unwrap_protocol,
)
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# Skip all tests if mcap is not available
MCAP_AVAILABLE = importlib.util.find_spec("mcap") is not None
pytestmark = pytest.mark.skipif(
    not MCAP_AVAILABLE,
    reason="mcap module not available. Install with: pip install mcap",
)


@pytest.fixture(autouse=True, params=[False, True], ids=["v1", "v2"])
def datasource_v2(request, restore_data_context):
    """Run every test against both read paths of ``read_mcap``.

    ``True`` routes through ``MCAPDatasourceV2`` (``ListFiles`` -> ``ReadFiles``),
    ``False`` through the legacy ``MCAPDatasource``. The two must agree on every
    row; tests that exercise one path's internals say so by name.
    """
    ray.data.DataContext.get_current().use_datasource_v2 = request.param
    return request.param


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


def test_read_mcap_basic(ray_start_regular_shared, basic_mcap_file, datasource_v2):
    """Test basic MCAP file reading."""
    ds = ray.data.read_mcap(basic_mcap_file)

    # Test metadata operations
    assert ds.count() == 2
    if not datasource_v2:
        # The V2 read path lists files inside the plan and does not report
        # them through the dataset's metadata.
        assert ds.input_files() == [_unwrap_protocol(basic_mcap_file)]

    # Verify basic fields are present
    rows = ds.take_all()
    for row in rows:
        assert "data" in row
        assert "topic" in row
        assert "log_time" in row
        assert "publish_time" in row


def test_read_mcap_multiple_files(ray_start_regular_shared, tmp_path, datasource_v2):
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
    if not datasource_v2:
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
    with pytest.raises(ValueError, match="must be less than"):
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
    ray_start_regular_shared, simple_mcap_file, ignore_missing_paths, datasource_v2
):
    """Test ignore_missing_paths parameter."""
    paths = [simple_mcap_file, "/nonexistent/missing.mcap"]

    if ignore_missing_paths:
        ds = ray.data.read_mcap(paths, ignore_missing_paths=ignore_missing_paths)
        assert ds.count() == 1
        if not datasource_v2:
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


def _read_stream_blocks(path, **datasource_kwargs):
    """Run MCAPDatasource's own read path against ``path`` and return its blocks.

    Exercises `_open_input_source` and `_read_stream` together, which is where the
    seeking and block-sizing behaviour lives, without standing up a Ray cluster.
    """
    from ray.data._internal.datasource.mcap_datasource import MCAPDatasource

    datasource = MCAPDatasource(path, **datasource_kwargs)
    resolved, filesystem = _resolve_paths_and_filesystem(path)
    with datasource._open_input_source(filesystem, resolved[0]) as f:
        return list(datasource._read_stream(f, resolved[0]))


def _read_stream_rows(path, **datasource_kwargs):
    from ray.data.block import BlockAccessor

    rows = []
    for block in _read_stream_blocks(path, **datasource_kwargs):
        rows.extend(BlockAccessor.for_block(block).iter_rows(True))
    return rows


def test_mcap_opens_file_for_random_access(ray_start_regular_shared, simple_mcap_file):
    """The read path must get a seekable handle, so mcap uses the file's index.

    `mcap.reader.make_reader` picks `SeekingReader` only when `stream.seekable()` is
    true. Without it the datasource silently falls back to a full scan, which is not
    something the output rows reveal.
    """
    from mcap.reader import SeekingReader, make_reader

    from ray.data._internal.datasource.mcap_datasource import MCAPDatasource

    datasource = MCAPDatasource(simple_mcap_file)
    resolved, filesystem = _resolve_paths_and_filesystem(simple_mcap_file)

    with datasource._open_input_source(filesystem, resolved[0]) as f:
        assert f.seekable()
        assert isinstance(make_reader(f), SeekingReader)


def test_read_mcap_file_without_index(ray_start_regular_shared, tmp_path):
    """A file written with no summary or chunk index still reads.

    Streaming writers omit both. `SeekingReader` falls back to a linear scan in that
    case, so opening for random access must not make such files unreadable.
    """
    from mcap.writer import IndexType, Writer

    path = os.path.join(tmp_path, "unindexed.mcap")
    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            index_types=IndexType.NONE,
            use_chunking=False,
            use_statistics=False,
            use_summary_offsets=False,
        )
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name="test_schema", encoding="jsonschema", data=b"{}"
        )
        channel_id = writer.register_channel(
            schema_id=schema_id, topic="/test", message_encoding="json"
        )
        for i in range(3):
            writer.add_message(
                channel_id=channel_id,
                log_time=1000 + i,
                publish_time=1000 + i,
                data=json.dumps({"seq": i}).encode(),
            )
        writer.finish()

    rows = _read_stream_rows(path)
    assert [row["data"]["seq"] for row in rows] == [0, 1, 2]

    # And through the public API, which is the path users take.
    assert ray.data.read_mcap(path).count() == 3


def test_read_mcap_log_time_order(ray_start_regular_shared, tmp_path):
    """``log_time_order`` picks between log-time order and file order.

    The messages are written out of log-time order on purpose. The default merges
    them by ``log_time``; ``log_time_order=False`` hands them back as written.
    """
    path = os.path.join(tmp_path, "unordered.mcap")
    messages = [
        {"topic": "/test", "data": {"seq": i}, "log_time": log_time}
        for i, log_time in enumerate([3_000_000_000, 1_000_000_000, 2_000_000_000])
    ]
    create_test_mcap_file(path, messages)

    ordered = _read_stream_rows(path)
    assert [row["log_time"] for row in ordered] == [
        1_000_000_000,
        2_000_000_000,
        3_000_000_000,
    ]

    as_written = _read_stream_rows(path, log_time_order=False)
    assert [row["log_time"] for row in as_written] == [
        3_000_000_000,
        1_000_000_000,
        2_000_000_000,
    ]

    ds = ray.data.read_mcap(path, log_time_order=False)
    assert [row["data"]["seq"] for row in ds.take_all()] == [0, 1, 2]


def test_read_mcap_yields_blocks_at_target_max_block_size(
    ray_start_regular_shared, restore_data_context, multi_topic_mcap_file
):
    """A file is streamed out in blocks of about ``target_max_block_size``.

    With a one-byte target every message overflows the block it is added to, so
    each message becomes its own block, and the rows across blocks are exactly the
    rows of a single-block read, in the same order.
    """
    from ray.data.block import BlockAccessor

    ctx = ray.data.DataContext.get_current()

    ctx.target_max_block_size = None
    (whole,) = _read_stream_blocks(multi_topic_mcap_file)
    expected = list(BlockAccessor.for_block(whole).iter_rows(True))
    assert len(expected) == 9

    ctx.target_max_block_size = 1
    blocks = _read_stream_blocks(multi_topic_mcap_file)
    assert len(blocks) == 9
    rows = []
    for block in blocks:
        rows.extend(BlockAccessor.for_block(block).iter_rows(True))
    assert rows == expected


def _write_chunked_mcap(path, num_messages, topics=("/a", "/b", "/c")):
    """One message per chunk, so every message boundary is a split point."""
    from mcap.writer import CompressionType, Writer

    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=1, compression=CompressionType.ZSTD)
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name="test_schema", encoding="jsonschema", data=b"{}"
        )
        channels = {
            topic: writer.register_channel(
                schema_id=schema_id, topic=topic, message_encoding="json"
            )
            for topic in topics
        }
        for i in range(num_messages):
            writer.add_message(
                channel_id=channels[topics[i % len(topics)]],
                log_time=1_000_000_000 + i * 1_000_000,
                publish_time=1_000_000_000 + i * 1_000_000,
                data=json.dumps({"seq": i}).encode(),
            )
        writer.finish()


def test_read_mcap_v2_splits_a_file_across_read_tasks(
    ray_start_regular_shared, tmp_path, monkeypatch, datasource_v2
):
    """On the V2 path a file is read by as many tasks as its chunks call for.

    With a one-byte packing budget every chunk is its own read task, so a
    nine-chunk file comes out as nine blocks; the legacy path always reads a
    file in one task.
    """
    path = os.path.join(tmp_path, "chunked.mcap")
    _write_chunked_mcap(path, 9)
    monkeypatch.setenv("RAY_DATA_MCAP_BIN_PACKING_BYTES", "1")

    ds = ray.data.read_mcap(path).materialize()

    assert ds.count() == 9
    assert sorted(row["data"]["seq"] for row in ds.take_all()) == list(range(9))
    if datasource_v2:
        assert ds.num_blocks() == 9


def test_read_mcap_include_row_id(
    ray_start_regular_shared, tmp_path, monkeypatch, datasource_v2
):
    """``row_id`` names a message the same way however the read is split."""
    paths = []
    for i in range(2):
        path = os.path.join(tmp_path, f"f{i}.mcap")
        _write_chunked_mcap(path, 6)
        paths.append(path)

    if not datasource_v2:
        with pytest.raises(NotImplementedError, match="include_row_id"):
            ray.data.read_mcap(paths, include_row_id=True)
        return

    one_task_per_chunk = ray.data.read_mcap(paths, include_row_id=True)
    by_id = {row["row_id"]: row["log_time"] for row in one_task_per_chunk.take_all()}
    assert len(by_id) == 12

    monkeypatch.setenv("RAY_DATA_MCAP_BIN_PACKING_BYTES", "1")
    resplit = ray.data.read_mcap(paths, include_row_id=True)
    assert {row["row_id"]: row["log_time"] for row in resplit.take_all()} == by_id

    # The topic filter drops rows but never renames the ones it keeps.
    filtered = ray.data.read_mcap(paths, topics=["/b"], include_row_id=True)
    for row in filtered.take_all():
        assert by_id[row["row_id"]] == row["log_time"]


def test_read_mcap_checkpoint_config_turns_on_row_id(
    ray_start_regular_shared, simple_mcap_file, datasource_v2
):
    """A checkpoint keyed on ``row_id`` gets the column without asking for it."""
    from ray.data.checkpoint import CheckpointConfig

    if not datasource_v2:
        pytest.skip("row_id is a V2 column")
    ctx = ray.data.DataContext.get_current()
    ctx.checkpoint_config = CheckpointConfig(
        id_column="row_id", checkpoint_path="/tmp/ray_data_mcap_ckpt_unused"
    )
    assert "row_id" in ray.data.read_mcap(simple_mcap_file).schema().names


def test_read_mcap_v2_schema_matches_rows(
    ray_start_regular_shared, multi_topic_mcap_file, datasource_v2
):
    """The planning-time schema is the schema of the blocks that come out."""
    if not datasource_v2:
        pytest.skip("the legacy path infers its schema from the first block")
    ds = ray.data.read_mcap(multi_topic_mcap_file, include_paths=True)
    planned = ds.schema()
    (block,) = ray.get(ds.materialize().get_internal_block_refs())
    assert block.schema.names == planned.names
    assert block.schema.types == planned.types


def _write_video_recording(path, seconds=3):
    """A 10 fps camera with a keyframe every ten frames (synthetic H.264 Annex-B)
    and a 50 Hz IMU, in small chunks."""
    from mcap.writer import CompressionType, Writer

    start = b"\x00\x00\x00\x01"
    keyframe = (
        (
            start
            + bytes([0x67, 0x42, 0x00, 0x1F])
            + start
            + bytes([0x68, 0xCE, 0x38, 0x80])
        )
        + start
        + bytes([0x65, 0x88, 0x84, 0x00])
    )
    pframe = start + bytes([0x41, 0x9A, 0x02, 0x04])
    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=600, compression=CompressionType.ZSTD)
        writer.start(profile="ros2", library="ray-test")
        cam_schema = writer.register_schema(
            name="foxglove_msgs/msg/CompressedVideo", encoding="ros2msg", data=b""
        )
        imu_schema = writer.register_schema(
            name="sensor_msgs/msg/Imu", encoding="ros2msg", data=b""
        )
        cam = writer.register_channel(
            schema_id=cam_schema, topic="/cam", message_encoding="cdr"
        )
        imu = writer.register_channel(
            schema_id=imu_schema, topic="/imu", message_encoding="cdr"
        )
        messages = []
        for frame in range(10 * seconds):
            payload = (keyframe if frame % 10 == 0 else pframe) + frame.to_bytes(
                4, "big"
            )
            messages.append((frame * 100_000_000, cam, payload))
        for i in range(50 * seconds):
            messages.append((i * 20_000_000, imu, i.to_bytes(8, "big")))
        for log_time, channel_id, payload in sorted(messages):
            writer.add_message(
                channel_id=channel_id,
                log_time=log_time,
                publish_time=log_time,
                data=payload,
            )
        writer.finish()


def test_read_mcap_window_granularity(
    ray_start_regular_shared, tmp_path, datasource_v2
):
    """Window rows: one decodable clip per row, with the video lead-in."""
    from ray.data.datasource import WindowSpec

    path = os.path.join(tmp_path, "video.mcap")
    _write_video_recording(path)

    if not datasource_v2:
        with pytest.raises(NotImplementedError, match="read_granularity"):
            ray.data.read_mcap(
                path, read_granularity="window", window=WindowSpec(length_s=1.0)
            )
        return

    ds = ray.data.read_mcap(
        path,
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=550_000_000),
        include_row_id=True,
    )
    rows = sorted(ds.take_all(), key=lambda row: row["window_start"])
    assert [row["window_start"] for row in rows] == [
        -450_000_000,
        550_000_000,
        1_550_000_000,
        2_550_000_000,
    ]
    clip = rows[1]
    assert clip["num_lead_in"] == 6  # frames at 0.0 .. 0.5 s back to the keyframe
    assert clip["num_messages"] == 6 + 10 + 50
    assert clip["log_time"] == sorted(clip["log_time"])
    assert {c["topic"] for c in clip["channels"]} == {"/cam", "/imu"}
    assert clip["row_id"].startswith(f"{path}#[550000000,1550000000)@")
    assert ds.schema().names == [
        "path",
        "row_id",
        "window_start",
        "window_end",
        "num_messages",
        "num_lead_in",
        "topic",
        "channel_id",
        "log_time",
        "publish_time",
        "sequence",
        "data",
        "channels",
    ]


def test_read_mcap_topic_and_file_granularity(
    ray_start_regular_shared, tmp_path, datasource_v2
):
    """Topic rows: one row and one task per (file, topic). File rows: one per file."""
    if not datasource_v2:
        pytest.skip("coarse granularities are V2 only")
    paths = []
    for i in range(2):
        path = os.path.join(tmp_path, f"run{i}.mcap")
        _write_video_recording(path, seconds=2)
        paths.append(path)

    topics = ray.data.read_mcap(paths, read_granularity="topic").materialize()
    assert topics.count() == 4
    assert topics.num_blocks() == 4
    by_key = {(row["path"], row["topic"]): row for row in topics.take_all()}
    assert set(by_key) == {(p, t) for p in paths for t in ("/cam", "/imu")}
    assert by_key[(paths[0], "/cam")]["num_messages"] == 20
    assert by_key[(paths[0], "/imu")]["num_messages"] == 100

    files = ray.data.read_mcap(paths, read_granularity="file", include_row_id=True)
    rows = sorted(files.take_all(), key=lambda row: row["path"])
    assert [row["path"] for row in rows] == paths
    assert all(row["num_messages"] == 120 for row in rows)
    assert all(set(row["topic"]) == {"/cam", "/imu"} for row in rows)
    assert len({row["row_id"] for row in rows}) == 2


def test_read_mcap_granularity_validation(
    ray_start_regular_shared, simple_mcap_file, datasource_v2
):
    from ray.data.datasource import VideoOptions, WindowSpec

    if not datasource_v2:
        pytest.skip("validated on the V2 path")
    with pytest.raises(ValueError, match="needs a WindowSpec"):
        ray.data.read_mcap(simple_mcap_file, read_granularity="window")
    with pytest.raises(ValueError, match="window applies to"):
        ray.data.read_mcap(simple_mcap_file, window=WindowSpec(length_s=1))
    with pytest.raises(ValueError, match="video applies to"):
        ray.data.read_mcap(simple_mcap_file, video=VideoOptions())
    with pytest.raises(ValueError, match="read_granularity must be one of"):
        ray.data.read_mcap(simple_mcap_file, read_granularity="clip")


def _optimized(ds):
    from ray.data._internal.logical.optimizers import LogicalOptimizer

    return LogicalOptimizer().optimize(ds._logical_plan).dag


def _walk(op):
    yield op
    for child in op.input_dependencies:
        yield from _walk(child)


def test_read_mcap_filter_pushdown(ray_start_regular_shared, tmp_path, datasource_v2):
    """``ds.filter`` on ``topic`` or ``log_time`` folds into the read.

    On the V2 path the ``Filter`` disappears from the plan and the pushed
    predicate reaches ``ListFiles``, so files and chunks are pruned by it; a
    predicate on another column stays a ``Filter``. Rows are the same either way.
    """
    from ray.data._internal.logical.operators import ListFiles
    from ray.data._internal.logical.operators.map_operator import Filter
    from ray.data.expressions import col

    path = os.path.join(tmp_path, "chunked.mcap")
    _write_chunked_mcap(path, 9)
    base = 1_000_000_000

    ds = ray.data.read_mcap(path).filter(
        expr=(col("topic") == "/a") & (col("log_time") < base + 4_000_000)
    )
    rows = ds.take_all()
    assert [row["data"]["seq"] for row in rows] == [0, 3]
    if datasource_v2:
        plan = list(_walk(_optimized(ds)))
        assert not any(isinstance(op, Filter) for op in plan)
        (list_files,) = [op for op in plan if isinstance(op, ListFiles)]
        assert list_files.predicate is not None

    mixed = ray.data.read_mcap(path).filter(
        expr=(col("topic") == "/b") & (col("sequence") >= 0)
    )
    assert sorted(row["data"]["seq"] for row in mixed.take_all()) == [1, 4, 7]
    if datasource_v2:
        plan = list(_walk(_optimized(mixed)))
        (filter_op,) = [op for op in plan if isinstance(op, Filter)]
        assert "sequence" in str(filter_op.predicate_expr)


def test_read_mcap_count_from_statistics(
    ray_start_regular_shared, tmp_path, datasource_v2
):
    """``count()`` under a topic selection is answered from the summaries."""
    from ray.data._internal.logical.interfaces import LogicalPlan
    from ray.data._internal.logical.operators.count_operator import Count
    from ray.data._internal.logical.operators.map_operator import MapBatches, Project
    from ray.data._internal.logical.optimizers import LogicalOptimizer
    from ray.data.expressions import col

    paths = []
    for i in range(2):
        path = os.path.join(tmp_path, f"f{i}.mcap")
        _write_chunked_mcap(path, 6)
        paths.append(path)

    def optimized_count_plan(ds):
        count = Count(
            input_dependencies=[
                Project(exprs=[], input_dependencies=[ds._logical_plan.dag])
            ]
        )
        return LogicalOptimizer().optimize(LogicalPlan(count, ds.context)).dag

    ds = ray.data.read_mcap(paths, topics=["/a"])
    assert ds.count() == 4
    pushed = ray.data.read_mcap(paths).filter(expr=col("topic").is_in(["/a", "/b"]))
    assert pushed.count() == 8
    if datasource_v2:
        assert isinstance(optimized_count_plan(ds), MapBatches)
        assert isinstance(optimized_count_plan(pushed), MapBatches)

    # A time range has to be read.
    ranged = ray.data.read_mcap(paths, time_range=(1_000_000_000, 1_002_000_000))
    assert ranged.count() == 4
    if datasource_v2:
        assert not isinstance(optimized_count_plan(ranged), MapBatches)


def test_read_mcap_attachment_and_metadata_granularity(
    ray_start_regular_shared, tmp_path, datasource_v2
):
    """Attachment and Metadata records come out one per row, counted from statistics."""
    from mcap.writer import CompressionType, Writer

    paths = []
    for i in range(2):
        path = os.path.join(tmp_path, f"run{i}.mcap")
        with open(path, "wb") as stream:
            writer = Writer(stream, chunk_size=1, compression=CompressionType.ZSTD)
            writer.start(profile="", library="ray-test")
            writer.add_metadata("recorder", {"version": "1.2", "run": str(i)})
            schema_id = writer.register_schema(
                name="test_schema", encoding="jsonschema", data=b"{}"
            )
            channel = writer.register_channel(
                schema_id=schema_id, topic="/t", message_encoding="json"
            )
            writer.add_message(
                channel_id=channel, log_time=1_000, publish_time=1_000, data=b"{}"
            )
            writer.add_attachment(
                create_time=900,
                log_time=1_000 + i,
                name=f"calib{i}.yaml",
                media_type="text/yaml",
                data=f"fx: {i}\n".encode(),
            )
            writer.finish()
        paths.append(path)

    if not datasource_v2:
        with pytest.raises(NotImplementedError, match="read_granularity"):
            ray.data.read_mcap(paths, read_granularity="attachment")
        return

    attachments = ray.data.read_mcap(
        paths, read_granularity="attachment", include_row_id=True
    )
    assert attachments.schema().names == [
        "path",
        "row_id",
        "name",
        "media_type",
        "log_time",
        "create_time",
        "data",
    ]
    rows = sorted(attachments.take_all(), key=lambda row: row["name"])
    assert [row["name"] for row in rows] == ["calib0.yaml", "calib1.yaml"]
    assert [row["data"] for row in rows] == [b"fx: 0\n", b"fx: 1\n"]
    assert attachments.count() == 2
    assert (
        ray.data.read_mcap(
            paths, read_granularity="attachment", time_range=(1_001, 1_002)
        ).count()
        == 1
    )

    metadata = ray.data.read_mcap(paths, read_granularity="metadata")
    rows = sorted(metadata.take_all(), key=lambda row: row["path"])
    assert [dict(row["metadata"])["run"] for row in rows] == ["0", "1"]
    assert metadata.count() == 2
    with pytest.raises(ValueError, match="do not apply"):
        ray.data.read_mcap(paths, read_granularity="metadata", topics=["/t"])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
