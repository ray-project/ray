"""
Tests for MCAP datasource functionality using ray.data.read_mcap().

These tests cover the MCAP reading functionality through the main Ray Data API,
following the pattern established by other file-based datasource tests.
"""

import importlib.util
import json
import os
import tempfile
from time import time_ns
from typing import TYPE_CHECKING

import pytest

import ray
from ray.tests.conftest import *  # noqa

# Try to import mcap, skip tests if not available
MCAP_AVAILABLE = importlib.util.find_spec("mcap") is not None

if TYPE_CHECKING or MCAP_AVAILABLE:
    from ray.data import Dataset

# Skip all tests if mcap is not available
pytestmark = pytest.mark.skipif(
    not MCAP_AVAILABLE,
    reason="mcap module not available. Install with: pip install mcap",
)


def create_mcap_file(file_path, messages_data):
    """Create a real MCAP file with the given messages.

    Args:
        file_path: Path where the MCAP file should be created.
        messages_data: List of dictionaries containing message data to write.
    """
    from mcap.writer import Writer

    with open(file_path, "wb") as stream:
        writer = Writer(stream)
        writer.start(profile="", library="ray-test-v1")

        schemas = {}
        channels = {}

        for msg_data in messages_data:
            schema_name = msg_data.get("schema_name", "default_schema")
            topic = msg_data.get("topic", "/default/topic")
            channel_name = msg_data.get("channel", "default_channel")
            message_encoding = msg_data.get("message_encoding", "json")
            data = msg_data.get("data", {"test": "data"})
            log_time = msg_data.get("log_time", time_ns())
            publish_time = msg_data.get("publish_time", time_ns())

            # Register schema if not already registered
            if schema_name not in schemas:
                schema_id = writer.register_schema(
                    name=schema_name,
                    encoding="jsonschema",
                    data=json.dumps(
                        {
                            "type": "object",
                            "properties": {
                                "test": {"type": "string"},
                                "value": {"type": "number"},
                            },
                        }
                    ).encode(),
                )
                schemas[schema_name] = schema_id

            # Register channel if not already registered
            channel_key = (topic, channel_name)
            if channel_key not in channels:
                channel_id = writer.register_channel(
                    schema_id=schemas[schema_name],
                    topic=topic,
                    message_encoding=message_encoding,
                    metadata={"channel_name": channel_name},
                )
                channels[channel_key] = channel_id

            # Add the message
            writer.add_message(
                channel_id=channels[channel_key],
                log_time=log_time,
                data=json.dumps(data).encode("utf-8"),
                publish_time=publish_time,
            )

        writer.finish()


@pytest.fixture
def basic_mcap_file():
    """Create a basic MCAP file with a few test messages.

    Returns:
        Path to the created MCAP file containing 3 test messages with different
        schemas and channels.
    """
    temp_dir = tempfile.mkdtemp()
    test_file_path = os.path.join(temp_dir, "basic_test.mcap")

    messages_data = [
        {
            "schema_name": "sensor_data",
            "topic": "/camera/image",
            "channel": "camera",
            "data": {"timestamp": 1500000000, "frame_id": "camera_frame"},
            "log_time": 1500000000000000000,
            "publish_time": 1500000001000000000,
        },
        {
            "schema_name": "sensor_data",
            "topic": "/lidar/points",
            "channel": "lidar",
            "data": {"timestamp": 1500000010, "point_count": 1024},
            "log_time": 1500000010000000000,
            "publish_time": 1500000011000000000,
        },
        {
            "schema_name": "control_data",
            "topic": "/cmd_vel",
            "channel": "control",
            "data": {"linear_x": 1.5, "angular_z": 0.5},
            "log_time": 1500000020000000000,
            "publish_time": 1500000021000000000,
        },
    ]

    create_mcap_file(test_file_path, messages_data)
    yield test_file_path

    # Cleanup
    import shutil

    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def multi_channel_mcap_file():
    """Create an MCAP file with multiple channels and topics.

    Returns:
        Path to the created MCAP file containing 20 test messages across
        2 different channels with timestamps for time-based filtering tests.
    """
    temp_dir = tempfile.mkdtemp()
    test_file_path = os.path.join(temp_dir, "multi_channel_test.mcap")

    base_time = 1600000000000000000  # Base time in nanoseconds
    messages_data = []

    # Create messages for different channels and topics
    for i in range(10):
        messages_data.extend(
            [
                {
                    "schema_name": "sensor_msgs/Image",
                    "topic": "/camera/image_raw",
                    "channel": "camera",
                    "data": {"width": 640, "height": 480, "seq": i},
                    "log_time": base_time + i * 100000000,
                    "publish_time": base_time + i * 100000000 + 1000000,
                },
                {
                    "schema_name": "sensor_msgs/PointCloud2",
                    "topic": "/lidar/points",
                    "channel": "lidar",
                    "data": {"point_count": 1024 + i, "seq": i},
                    "log_time": base_time + i * 100000000 + 50000000,
                    "publish_time": base_time + i * 100000000 + 51000000,
                },
            ]
        )

    create_mcap_file(test_file_path, messages_data)
    yield test_file_path

    # Cleanup
    import shutil

    shutil.rmtree(temp_dir, ignore_errors=True)


def test_read_mcap_basic(ray_start_regular_shared, basic_mcap_file):
    """Test basic MCAP file reading."""
    ds = ray.data.read_mcap(basic_mcap_file)

    # Verify dataset is created
    assert isinstance(ds, Dataset)

    # Verify we can get basic info without errors
    assert ds.count() == 3  # We created 3 messages

    # Verify we can read the data
    rows = ds.take_all()
    assert len(rows) == 3

    # Check that basic fields are present
    for row in rows:
        assert "data" in row
        assert "log_time" in row
        assert "publish_time" in row
        assert "topic" in row


def test_read_mcap_with_channels(ray_start_regular_shared, basic_mcap_file):
    """Test MCAP reading with channel filtering."""
    # Note: In basic_mcap_file, we have "camera" and "lidar" channels
    channels = {"camera", "lidar"}
    ds = ray.data.read_mcap(basic_mcap_file, channels=channels)

    assert isinstance(ds, Dataset)
    rows = ds.take_all()

    # Should get messages from camera and lidar channels
    topics = {row["topic"] for row in rows}
    assert "/camera/image" in topics
    assert "/lidar/points" in topics


def test_read_mcap_with_topics(ray_start_regular_shared, multi_channel_mcap_file):
    """Test MCAP reading with topic filtering."""
    topics = {"/camera/image_raw", "/lidar/points"}
    ds = ray.data.read_mcap(multi_channel_mcap_file, topics=topics)

    assert isinstance(ds, Dataset)
    rows = ds.take_all()

    # Should only get messages from specified topics
    actual_topics = {row["topic"] for row in rows}
    assert actual_topics.issubset(topics)


def test_read_mcap_with_time_range(ray_start_regular_shared, multi_channel_mcap_file):
    """Test MCAP reading with time range filtering."""
    # Use a time range that should capture some but not all messages
    base_time = 1600000000000000000
    time_range = (base_time, base_time + 500000000)  # First 5 message pairs
    ds = ray.data.read_mcap(multi_channel_mcap_file, time_range=time_range)

    assert isinstance(ds, Dataset)
    rows = ds.take_all()

    # Should have fewer messages due to time filtering
    assert len(rows) < 20  # Less than total messages
    assert len(rows) > 0  # But still some messages


def test_read_mcap_with_message_types(
    ray_start_regular_shared, multi_channel_mcap_file
):
    """Test MCAP reading with message type filtering."""
    message_types = {"sensor_msgs/Image"}
    ds = ray.data.read_mcap(multi_channel_mcap_file, message_types=message_types)

    assert isinstance(ds, Dataset)
    rows = ds.take_all()

    # Should only get Image messages, not PointCloud2
    assert len(rows) == 10  # Only camera messages
    for row in rows:
        assert row["topic"] == "/camera/image_raw"


def test_read_mcap_multiple_files(ray_start_regular_shared):
    """Test reading multiple MCAP files."""
    temp_dir = tempfile.mkdtemp()
    try:
        # Create multiple real MCAP files
        file_paths = []
        for i in range(3):
            file_path = os.path.join(temp_dir, f"test_{i}.mcap")
            messages_data = [
                {
                    "schema_name": f"test_schema_{i}",
                    "topic": f"/test/topic_{i}",
                    "channel": f"channel_{i}",
                    "data": {"file_index": i, "message": f"test_{i}"},
                    "log_time": 1500000000000000000 + i * 1000000000,
                    "publish_time": 1500000001000000000 + i * 1000000000,
                }
            ]
            create_mcap_file(file_path, messages_data)
            file_paths.append(file_path)

        ds = ray.data.read_mcap(file_paths)
        assert isinstance(ds, Dataset)

        # Should have 3 messages total (1 from each file)
        rows = ds.take_all()
        assert len(rows) == 3

        # Verify data from different files
        file_indices = {row["data"]["file_index"] for row in rows}
        assert file_indices == {0, 1, 2}

    finally:
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


def test_read_mcap_with_include_paths(ray_start_regular_shared, basic_mcap_file):
    """Test MCAP reading with path inclusion."""
    ds = ray.data.read_mcap(basic_mcap_file, include_paths=True)

    assert isinstance(ds, Dataset)
    rows = ds.take_all()

    # Verify that path information is included
    for row in rows:
        assert "_file_path" in row
        assert basic_mcap_file in row["_file_path"]


def test_read_mcap_with_include_metadata_false(
    ray_start_regular_shared, basic_mcap_file
):
    """Test MCAP reading without metadata."""
    ds_with_metadata = ray.data.read_mcap(basic_mcap_file, include_metadata=True)
    ds_without_metadata = ray.data.read_mcap(basic_mcap_file, include_metadata=False)

    rows_with = ds_with_metadata.take_all()
    rows_without = ds_without_metadata.take_all()

    # Both should have same number of rows
    assert len(rows_with) == len(rows_without)

    # Rows with metadata should have schema fields
    assert "schema_name" in rows_with[0]

    # Rows without metadata should not have schema fields
    assert "schema_name" not in rows_without[0]


def test_read_mcap_invalid_time_range(ray_start_regular_shared, basic_mcap_file):
    """Test MCAP reading with invalid time range."""
    # Start time >= end time should raise error
    with pytest.raises(ValueError, match="start_time must be less than end_time"):
        ray.data.read_mcap(basic_mcap_file, time_range=(2000000000, 1000000000))

    # Negative time values should raise error
    with pytest.raises(ValueError, match="time values must be non-negative"):
        ray.data.read_mcap(basic_mcap_file, time_range=(-1000000000, 2000000000))


def test_read_mcap_channels_and_topics_exclusive(
    ray_start_regular_shared, basic_mcap_file
):
    """Test that channels and topics parameters are mutually exclusive."""
    with pytest.raises(ValueError, match="Cannot specify both 'channels' and 'topics'"):
        ray.data.read_mcap(
            basic_mcap_file, channels={"camera"}, topics={"/camera/image_raw"}
        )


def test_read_mcap_nonexistent_file(ray_start_regular_shared):
    """Test MCAP reading with nonexistent file."""
    with pytest.raises(Exception):  # Should raise some kind of file not found error
        ds = ray.data.read_mcap("/nonexistent/file.mcap")
        ds.materialize()  # Force execution


def test_read_mcap_directory(ray_start_regular_shared):
    """Test reading MCAP files from a directory."""
    temp_dir = tempfile.mkdtemp()
    try:
        # Create multiple real MCAP files in directory
        for i in range(2):
            file_path = os.path.join(temp_dir, f"data_{i}.mcap")
            messages_data = [
                {
                    "schema_name": f"dir_test_schema_{i}",
                    "topic": f"/dir/topic_{i}",
                    "channel": f"dir_channel_{i}",
                    "data": {"dir_index": i, "message": f"dir_test_{i}"},
                    "log_time": 1600000000000000000 + i * 1000000000,
                    "publish_time": 1600000001000000000 + i * 1000000000,
                }
            ]
            create_mcap_file(file_path, messages_data)

        ds = ray.data.read_mcap(temp_dir)
        assert isinstance(ds, Dataset)

        # Should read both files
        rows = ds.take_all()
        assert len(rows) == 2

        # Verify data from both files
        dir_indices = {row["data"]["dir_index"] for row in rows}
        assert dir_indices == {0, 1}

    finally:
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


def test_read_mcap_with_override_num_blocks(ray_start_regular_shared, basic_mcap_file):
    """Test MCAP reading with override_num_blocks parameter."""
    ds = ray.data.read_mcap(basic_mcap_file, override_num_blocks=2)

    assert isinstance(ds, Dataset)

    # Should still read all the data
    rows = ds.take_all()
    assert len(rows) == 3


def test_read_mcap_file_extensions(ray_start_regular_shared):
    """Test MCAP reading with custom file extensions."""
    temp_dir = tempfile.mkdtemp()
    try:
        # Create a real MCAP file with .mcap extension
        mcap_file = os.path.join(temp_dir, "data.mcap")
        messages_data = [
            {
                "schema_name": "ext_test_schema",
                "topic": "/ext/topic",
                "channel": "ext_channel",
                "data": {"test": "mcap_extension"},
                "log_time": 1700000000000000000,
                "publish_time": 1700000001000000000,
            }
        ]
        create_mcap_file(mcap_file, messages_data)

        # Create a file with different extension (not MCAP format)
        other_file = os.path.join(temp_dir, "data.txt")
        with open(other_file, "wb") as f:
            f.write(b"NOT_MCAP_DATA" * 50)

        # Should only read .mcap files by default
        ds = ray.data.read_mcap(temp_dir)
        assert isinstance(ds, Dataset)
        rows = ds.take_all()
        assert len(rows) == 1
        assert rows[0]["data"]["test"] == "mcap_extension"

        # Should fail when trying to read non-MCAP files with wrong extension
        with pytest.raises(Exception):
            ds = ray.data.read_mcap(temp_dir, file_extensions=["txt"])
            ds.materialize()  # Force execution

    finally:
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


def test_read_mcap_without_mcap_library(ray_start_regular_shared, basic_mcap_file):
    """Test MCAP reading fails gracefully without mcap library."""
    from unittest.mock import patch

    with patch.dict("sys.modules", {"mcap": None}):
        with pytest.raises(ImportError, match="MCAPDatasource.*depends on 'mcap'"):
            ray.data.read_mcap(basic_mcap_file)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
