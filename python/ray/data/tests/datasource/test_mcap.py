import importlib.util
import json
import os

import pytest

import ray
from ray.data._internal.datasource.mcap_datasource import (
    MCAP_ENCODING_RATIO_ESTIMATE_DEFAULT,
    MCAPDatasource,
    TimeRange,
)
from ray.data.context import DataContext
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# Skip all tests if mcap is not available
MCAP_AVAILABLE = importlib.util.find_spec("mcap") is not None
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


# A schema definition of the size real ROS 2 / Foxglove schemas reach. It is
# registered once per file but, with `include_metadata`, materialized on every
# row, which is what makes the in-memory size dwarf the on-disk size.
LARGE_SCHEMA_DEFINITION = b"# msgdef\n" + b"uint8[] data\nstring format\n" * 100


def create_binary_mcap_file(
    file_path: str,
    num_messages: int,
    *,
    topics: tuple = ("/camera/image",),
    payload_size: int = 512,
    use_statistics: bool = True,
    descending: bool = False,
) -> None:
    """Create an MCAP file with compressible binary payloads.

    Unlike `create_test_mcap_file`, this writes opaque binary messages against a
    large schema, which is the shape of a real robotics recording: the chunks
    compress well on disk while every row carries the schema in memory.

    Args:
        file_path: Where to write the file.
        num_messages: Number of messages to write.
        topics: Topics to round-robin the messages across.
        payload_size: Size in bytes of each message payload.
        use_statistics: Whether to write the summary Statistics record. When
            False, the file carries no message count to estimate from.
        descending: Write messages in descending log time, so that ascending
            read order has to come from the reader rather than the file layout.
    """
    from mcap.writer import Writer

    with open(file_path, "wb") as stream:
        writer = Writer(stream, use_statistics=use_statistics)
        writer.start(profile="", library="ray-test")

        schema_id = writer.register_schema(
            name="foxglove.CompressedVideo",
            encoding="ros2msg",
            data=LARGE_SCHEMA_DEFINITION,
        )
        channels = {
            topic: writer.register_channel(
                schema_id=schema_id, topic=topic, message_encoding="cdr"
            )
            for topic in topics
        }

        indices = range(num_messages)
        if descending:
            indices = reversed(indices)
        for i in indices:
            log_time = 1000000000 + i * 33000000
            writer.add_message(
                channel_id=channels[topics[i % len(topics)]],
                log_time=log_time,
                publish_time=log_time,
                data=bytes([i % 256]) * payload_size,
                sequence=i,
            )

        writer.finish()


@pytest.fixture
def binary_mcap_file(tmp_path):
    """Fixture providing an MCAP file with 360 binary messages."""
    path = os.path.join(tmp_path, "binary.mcap")
    create_binary_mcap_file(path, 360)
    return path


def test_estimate_inmemory_data_size_exceeds_on_disk_size(
    ray_start_regular_shared, binary_mcap_file
):
    """In-memory size estimates must not report the on-disk size.

    `FileBasedDatasource` estimates in-memory size as the sum of the on-disk
    file sizes. MCAP chunks are compressed and every row additionally carries
    metadata that has no per-message on-disk equivalent, so that default
    understates the materialized size by more than an order of magnitude. Ray
    Data sizes both read parallelism and memory provisioning from this number.
    """
    on_disk_size = os.path.getsize(binary_mcap_file)
    actual_size = ray.data.read_mcap(binary_mcap_file).materialize().size_bytes()

    # Establish that on-disk size is not a usable proxy for this format.
    assert actual_size > 10 * on_disk_size

    estimate = MCAPDatasource(paths=[binary_mcap_file]).estimate_inmemory_data_size()
    assert 0.5 * actual_size <= estimate <= 2 * actual_size


def test_estimate_inmemory_data_size_tracks_include_metadata(
    ray_start_regular_shared, binary_mcap_file
):
    """Dropping the metadata columns must shrink the estimate."""
    with_metadata = MCAPDatasource(
        paths=[binary_mcap_file], include_metadata=True
    ).estimate_inmemory_data_size()
    without_metadata = MCAPDatasource(
        paths=[binary_mcap_file], include_metadata=False
    ).estimate_inmemory_data_size()

    assert without_metadata < with_metadata

    actual = (
        ray.data.read_mcap(binary_mcap_file, include_metadata=False)
        .materialize()
        .size_bytes()
    )
    assert 0.5 * actual <= without_metadata <= 2 * actual


def test_estimate_inmemory_data_size_respects_topic_filter(
    ray_start_regular_shared, tmp_path
):
    """A topic filter must be reflected in the estimate."""
    path = os.path.join(tmp_path, "multi_topic_binary.mcap")
    create_binary_mcap_file(path, 300, topics=("/topic_a", "/topic_b", "/topic_c"))

    unfiltered = MCAPDatasource(paths=[path]).estimate_inmemory_data_size()
    filtered = MCAPDatasource(
        paths=[path], topics={"/topic_a"}
    ).estimate_inmemory_data_size()

    # One of three topics is selected, so the estimate should fall roughly
    # threefold rather than staying flat.
    assert filtered < unfiltered
    assert 0.2 * unfiltered <= filtered <= 0.5 * unfiltered

    actual = ray.data.read_mcap(path, topics={"/topic_a"}).materialize().size_bytes()
    assert 0.5 * actual <= filtered <= 2 * actual


def test_estimate_inmemory_data_size_multiple_files(ray_start_regular_shared, tmp_path):
    """The estimate must aggregate across files, including unsampled ones."""
    paths = []
    for i in range(12):
        path = os.path.join(tmp_path, f"part_{i:02d}.mcap")
        create_binary_mcap_file(path, 60)
        paths.append(path)

    estimate = MCAPDatasource(paths=paths).estimate_inmemory_data_size()
    actual = ray.data.read_mcap(paths).materialize().size_bytes()

    assert 0.5 * actual <= estimate <= 2 * actual


def test_estimate_inmemory_data_size_without_statistics(
    ray_start_regular_shared, tmp_path
):
    """A file with no summary statistics falls back instead of failing.

    Message counts come from the MCAP summary, which is optional. Without it
    the datasource must still return a usable overestimate.
    """
    path = os.path.join(tmp_path, "no_stats.mcap")
    create_binary_mcap_file(path, 200, use_statistics=False)

    estimate = MCAPDatasource(paths=[path]).estimate_inmemory_data_size()

    assert estimate == os.path.getsize(path) * MCAP_ENCODING_RATIO_ESTIMATE_DEFAULT


def test_estimate_inmemory_data_size_sampling_disabled(
    ray_start_regular_shared, binary_mcap_file
):
    """`decoding_size_estimation=False` must skip sampling entirely."""
    ctx = DataContext.get_current()
    original = ctx.decoding_size_estimation
    ctx.decoding_size_estimation = False
    try:
        estimate = MCAPDatasource(
            paths=[binary_mcap_file]
        ).estimate_inmemory_data_size()
    finally:
        ctx.decoding_size_estimation = original

    on_disk_size = os.path.getsize(binary_mcap_file)
    assert estimate == on_disk_size * MCAP_ENCODING_RATIO_ESTIMATE_DEFAULT


def test_estimate_inmemory_data_size_accounts_for_include_paths(
    ray_start_regular_shared, binary_mcap_file
):
    """`include_paths` adds a column downstream of `_read_stream`.

    Sampling builds its block from `_read_stream`'s output, so it has to add
    that column itself or the estimate misses it.
    """
    without_paths = MCAPDatasource(
        paths=[binary_mcap_file]
    ).estimate_inmemory_data_size()
    with_paths = MCAPDatasource(
        paths=[binary_mcap_file], include_paths=True
    ).estimate_inmemory_data_size()

    assert with_paths > without_paths

    actual = (
        ray.data.read_mcap(binary_mcap_file, include_paths=True)
        .materialize()
        .size_bytes()
    )
    assert 0.5 * actual <= with_paths <= 2 * actual


def test_estimate_inmemory_data_size_narrow_time_range(
    ray_start_regular_shared, binary_mcap_file
):
    """A `time_range` selecting a small slice must not estimate the whole file.

    Message counts come from the MCAP summary, which cannot express a time
    range. When sampling covers the whole selection the measured size is exact,
    so the estimate has to track the filter rather than the file.
    """
    # `create_binary_mcap_file` writes log times at 1e9 + i * 33e6.
    start = 1000000000
    time_range = (start, start + 10 * 33000000)

    unfiltered = MCAPDatasource(paths=[binary_mcap_file]).estimate_inmemory_data_size()
    filtered = MCAPDatasource(
        paths=[binary_mcap_file], time_range=TimeRange(*time_range)
    ).estimate_inmemory_data_size()

    ds = ray.data.read_mcap(binary_mcap_file, time_range=time_range).materialize()
    actual = ds.size_bytes()

    # 10 of 360 messages: the estimate must fall, not stay flat.
    assert filtered < unfiltered / 10
    assert 0.5 * actual <= filtered <= 2 * actual


def test_read_mcap_orders_messages_by_log_time(ray_start_regular_shared, tmp_path):
    """Messages must be returned in ascending log time order.

    The file is written in descending order, so passing this requires the
    reader's ordering rather than the file's layout.
    """
    path = os.path.join(tmp_path, "descending.mcap")
    create_binary_mcap_file(path, 200, descending=True)

    rows = ray.data.read_mcap(path).take_all()

    assert len(rows) == 200
    log_times = [row["log_time"] for row in rows]
    assert log_times == sorted(log_times)
    assert [row["sequence"] for row in rows] == list(range(200))


def test_read_mcap_preserves_binary_payloads(ray_start_regular_shared, tmp_path):
    """Non-JSON payloads must round-trip byte for byte.

    A `cdr`-encoded message is opaque bytes; the JSON decoding path must not
    alter it.
    """
    path = os.path.join(tmp_path, "payloads.mcap")
    create_binary_mcap_file(path, 128, payload_size=256)

    rows = ray.data.read_mcap(path).take_all()

    assert len(rows) == 128
    for i, row in enumerate(rows):
        assert bytes(row["data"]) == bytes([i % 256]) * 256


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
