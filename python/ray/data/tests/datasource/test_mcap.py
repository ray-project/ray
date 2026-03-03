import importlib.util
import json
import os

import pytest

import ray
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
