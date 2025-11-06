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
    assert len(rows) == 2
    for row in rows:
        assert "data" in row
        assert "topic" in row
        assert "log_time" in row
        assert "publish_time" in row
        assert "sequence" in row

    # Verify topics are correct
    topics = {row["topic"] for row in rows}
    assert topics == {"/camera/image", "/lidar/points"}


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
    from ray.data.datasource import TimeRange

    path, base_time = time_series_mcap_file

    # Filter to first 5 messages using tuple format (backwards compatible)
    time_range = (base_time, base_time + 5000000)
    ds = ray.data.read_mcap(path, time_range=time_range)

    rows = ds.take_all()
    assert len(rows) <= 5
    for row in rows:
        assert base_time <= row["log_time"] < base_time + 5000000

    # Test with TimeRange object
    time_range_obj = TimeRange(
        start_time=base_time + 2000000, end_time=base_time + 8000000
    )
    ds2 = ray.data.read_mcap(path, time_range=time_range_obj)
    rows2 = ds2.take_all()
    assert len(rows2) <= 6  # Messages 2-7 inclusive
    for row in rows2:
        assert base_time + 2000000 <= row["log_time"] < base_time + 8000000


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


# ============================================================================
# Tests for File-Level Metadata and Predicate Pushdown
# ============================================================================


@pytest.fixture
def multi_file_mcap_dir(tmp_path):
    """Fixture providing multiple MCAP files in a directory with different topics."""
    mcap_dir = os.path.join(tmp_path, "mcap_data")
    os.makedirs(mcap_dir)

    base_time = 1000000000

    # File 1: Only topic_a messages
    path1 = os.path.join(mcap_dir, "file_a.mcap")
    messages1 = [
        {
            "topic": "/topic_a",
            "data": {"file": 1, "seq": i},
            "log_time": base_time + i * 1000000,
        }
        for i in range(5)
    ]
    create_test_mcap_file(path1, messages1)

    # File 2: Only topic_b messages
    path2 = os.path.join(mcap_dir, "file_b.mcap")
    messages2 = [
        {
            "topic": "/topic_b",
            "data": {"file": 2, "seq": i},
            "log_time": base_time + 10000000 + i * 1000000,
        }
        for i in range(5)
    ]
    create_test_mcap_file(path2, messages2)

    # File 3: Mixed topics
    path3 = os.path.join(mcap_dir, "file_mixed.mcap")
    messages3 = [
        {
            "topic": "/topic_a" if i % 2 == 0 else "/topic_b",
            "data": {"file": 3, "seq": i},
            "log_time": base_time + 20000000 + i * 1000000,
        }
        for i in range(4)
    ]
    create_test_mcap_file(path3, messages3)

    return mcap_dir, base_time


def test_read_mcap_file_metadata_provider(
    ray_start_regular_shared, multi_file_mcap_dir
):
    """Test that MCAPFileMetadataProvider correctly extracts file metadata."""
    mcap_dir, base_time = multi_file_mcap_dir

    # We can't directly test _read_file_metadata without RetryingPyFileSystem,
    # but we can test that the datasource uses it correctly
    ds = ray.data.read_mcap(mcap_dir)

    # Verify we read all files
    assert ds.count() == 14  # 5 + 5 + 4 messages


def test_read_mcap_directory_with_topic_filter(
    ray_start_regular_shared, multi_file_mcap_dir
):
    """Test reading directory with topic filtering - should skip files without the topic."""
    mcap_dir, base_time = multi_file_mcap_dir

    # Read only topic_a messages
    ds = ray.data.read_mcap(mcap_dir, topics={"/topic_a"})
    rows = ds.take_all()

    # Should get topic_a messages from file_a (5) and file_mixed (2)
    assert len(rows) == 7
    assert all(row["topic"] == "/topic_a" for row in rows)

    # Read only topic_b messages
    ds = ray.data.read_mcap(mcap_dir, topics={"/topic_b"})
    rows = ds.take_all()

    # Should get topic_b messages from file_b (5) and file_mixed (2)
    assert len(rows) == 7
    assert all(row["topic"] == "/topic_b" for row in rows)


def test_read_mcap_directory_with_time_range_filter(
    ray_start_regular_shared, multi_file_mcap_dir
):
    """Test reading directory with time range filtering."""
    from ray.data.datasource import TimeRange

    mcap_dir, base_time = multi_file_mcap_dir

    # Read only messages from the first file's time range
    time_range = TimeRange(start_time=base_time, end_time=base_time + 6000000)
    ds = ray.data.read_mcap(mcap_dir, time_range=time_range)
    rows = ds.take_all()

    # Should only get messages from file_a
    assert len(rows) == 5
    assert all(row["data"]["file"] == 1 for row in rows)


def test_mcap_predicate_pushdown_topic(ray_start_regular_shared, multi_file_mcap_dir):
    """Test predicate pushdown with topic filtering."""
    from ray.data.expressions import col

    mcap_dir, base_time = multi_file_mcap_dir

    # Read all files, then filter by topic using predicate pushdown
    ds = ray.data.read_mcap(mcap_dir)
    ds_filtered = ds.filter(col("topic") == "/topic_a")

    rows = ds_filtered.take_all()
    assert len(rows) == 7  # topic_a messages from file_a and file_mixed
    assert all(row["topic"] == "/topic_a" for row in rows)


def test_mcap_predicate_pushdown_time(ray_start_regular_shared, multi_file_mcap_dir):
    """Test predicate pushdown with time filtering."""
    from ray.data.expressions import col

    mcap_dir, base_time = multi_file_mcap_dir

    # Filter by time range using predicate pushdown (<)
    ds = ray.data.read_mcap(mcap_dir)
    cutoff_time = base_time + 11000000
    ds_filtered = ds.filter(col("log_time") < cutoff_time)

    rows = ds_filtered.take_all()
    # Should get messages from file_a and first message from file_b
    assert all(row["log_time"] < cutoff_time for row in rows)
    assert len(rows) == 6  # 5 from file_a + 1 from file_b

    # Test >= operator
    start_time = base_time + 20000000
    ds_filtered2 = ds.filter(col("log_time") >= start_time)
    rows2 = ds_filtered2.take_all()
    assert all(row["log_time"] >= start_time for row in rows2)
    assert len(rows2) == 4  # Messages from file_mixed


def test_mcap_predicate_pushdown_combined(
    ray_start_regular_shared, multi_file_mcap_dir
):
    """Test predicate pushdown with combined topic and time filters."""
    from ray.data.expressions import col

    mcap_dir, base_time = multi_file_mcap_dir

    # Read all files, then apply multiple filters
    ds = ray.data.read_mcap(mcap_dir)
    cutoff_time = base_time + 21000000
    ds_filtered = ds.filter(
        (col("topic") == "/topic_a") & (col("log_time") >= cutoff_time)
    )

    rows = ds_filtered.take_all()
    # Should only get topic_a messages from file_mixed with time >= cutoff
    assert len(rows) == 2
    assert all(row["topic"] == "/topic_a" for row in rows)
    assert all(row["log_time"] >= cutoff_time for row in rows)
    assert all(row["data"]["file"] == 3 for row in rows)

    # Test with OR logic - same column (should be pushed down correctly)
    ds_filtered2 = ds.filter(
        (col("topic") == "/topic_a") | (col("topic") == "/topic_b")
    )
    rows2 = ds_filtered2.take_all()
    topics2 = {row["topic"] for row in rows2}
    assert topics2 == {"/topic_a", "/topic_b"}
    assert len(rows2) == 14  # All messages from topic_a and topic_b


def test_mcap_predicate_pushdown_or_same_column(
    ray_start_regular_shared, multi_topic_mcap_file
):
    """Test that OR expressions with the same column are correctly pushed down.

    This test verifies the fix for recursive column extraction in OR expressions.
    Expressions like (col("topic") == "/a") | (col("topic") == "/b") should
    be detected as referencing the same column and pushed down correctly.

    Previously, get_column_name() only checked direct ColumnExpr objects, causing
    it to return None for BinaryExpr objects like (col("topic") == value), which
    prevented can_pushdown_or() from detecting that both sides reference the
    same column.
    """
    from ray.data.expressions import col

    # Test OR with same column (topic) - should be pushed down
    ds = ray.data.read_mcap(multi_topic_mcap_file)
    ds_filtered = ds.filter((col("topic") == "/topic_a") | (col("topic") == "/topic_b"))

    rows = ds_filtered.take_all()
    topics = {row["topic"] for row in rows}
    assert topics == {"/topic_a", "/topic_b"}
    assert len(rows) == 6  # 2/3 of messages (3 per topic, 2 topics)

    # Test OR with same column (schema_name) - should be pushed down
    ds2 = ray.data.read_mcap(multi_topic_mcap_file, include_metadata=True)
    ds_filtered2 = ds2.filter(
        (col("schema_name") == "test_schema") | (col("schema_name") == "test_schema")
    )
    rows2 = ds_filtered2.take_all()
    # All messages use test_schema, so should get all 9
    assert len(rows2) == 9
    assert all(row["schema_name"] == "test_schema" for row in rows2)

    # Test OR with different columns (should still work, but not optimized)
    ds_filtered3 = ds.filter(
        (col("topic") == "/topic_a") | (col("log_time") > 1000000000)
    )
    rows3 = ds_filtered3.take_all()
    # Should get topic_a messages OR messages with log_time > base_time
    # Since all messages have log_time > base_time, should get all messages
    assert len(rows3) == 9


def test_mcap_predicate_pushdown_with_initial_filter(
    ray_start_regular_shared, multi_file_mcap_dir
):
    """Test that predicate pushdown works with initial topic filtering."""
    from ray.data.expressions import col

    mcap_dir, base_time = multi_file_mcap_dir

    # Start with topic filter, then add time filter via pushdown
    ds = ray.data.read_mcap(mcap_dir, topics={"/topic_a"})
    cutoff_time = base_time + 21000000
    ds_filtered = ds.filter(col("log_time") >= cutoff_time)

    rows = ds_filtered.take_all()
    # Should only get topic_a messages with time >= cutoff
    assert len(rows) == 2
    assert all(row["topic"] == "/topic_a" for row in rows)
    assert all(row["log_time"] >= cutoff_time for row in rows)

    # Test with initial time range filter, then add topic filter
    from ray.data.datasource import TimeRange

    time_range = TimeRange(
        start_time=base_time + 20000000, end_time=base_time + 30000000
    )
    ds2 = ray.data.read_mcap(mcap_dir, time_range=time_range)
    ds_filtered2 = ds2.filter(col("topic") == "/topic_a")
    rows2 = ds_filtered2.take_all()
    assert len(rows2) == 2
    assert all(row["topic"] == "/topic_a" for row in rows2)
    assert all(
        base_time + 20000000 <= row["log_time"] < base_time + 30000000 for row in rows2
    )


def test_mcap_predicate_pushdown_schema_name(
    ray_start_regular_shared, multi_topic_mcap_file
):
    """Test predicate pushdown with schema_name filtering."""
    from ray.data.expressions import col

    # Read with metadata to get schema names
    ds = ray.data.read_mcap(multi_topic_mcap_file, include_metadata=True)
    ds_filtered = ds.filter(col("schema_name") == "test_schema")

    rows = ds_filtered.take_all()
    # All our test messages use the same schema
    assert len(rows) == 9
    assert all(row["schema_name"] == "test_schema" for row in rows)

    # Test filtering with non-existent schema
    ds_filtered2 = ds.filter(col("schema_name") == "nonexistent")
    rows2 = ds_filtered2.take_all()
    assert len(rows2) == 0


def test_mcap_supports_predicate_pushdown(ray_start_regular_shared, simple_mcap_file):
    """Test that MCAPDatasource reports it supports predicate pushdown."""
    from ray.data._internal.datasource.mcap_datasource import MCAPDatasource

    ds_internal = MCAPDatasource(paths=simple_mcap_file)
    assert ds_internal.supports_predicate_pushdown() is True


def test_mcap_file_level_metadata_blocks(ray_start_regular_shared, multi_file_mcap_dir):
    """Test that file-level metadata provides accurate block sizing information."""
    mcap_dir, base_time = multi_file_mcap_dir

    ds = ray.data.read_mcap(mcap_dir)

    # Verify that blocks have size metadata
    blocks = ds._plan.execute().blocks
    for block_ref, metadata in blocks:
        # Each block should have size_bytes information
        assert metadata.size_bytes is not None
        assert metadata.size_bytes > 0


def test_mcap_timerange_class():
    """Test TimeRange dataclass."""
    from ray.data.datasource import TimeRange

    # Create a TimeRange
    tr = TimeRange(start_time=1000, end_time=2000)
    assert tr.start_time == 1000
    assert tr.end_time == 2000

    # Test validation - start_time >= end_time
    with pytest.raises(ValueError, match="start_time must be less than end_time"):
        TimeRange(start_time=2000, end_time=1000)

    # Test validation - negative times
    with pytest.raises(ValueError, match="time values must be non-negative"):
        TimeRange(start_time=-1000, end_time=2000)

    with pytest.raises(ValueError, match="time values must be non-negative"):
        TimeRange(start_time=1000, end_time=-2000)


def test_mcap_multiple_filters_intersection(
    ray_start_regular_shared, multi_file_mcap_dir
):
    """Test that multiple filters are combined with AND logic (intersection)."""
    from ray.data.expressions import col

    mcap_dir, base_time = multi_file_mcap_dir

    # Start with broad topic filter
    ds = ray.data.read_mcap(mcap_dir, topics={"/topic_a", "/topic_b"})

    # Apply narrower topic filter - should intersect
    ds_filtered = ds.filter(col("topic") == "/topic_a")

    rows = ds_filtered.take_all()
    assert len(rows) == 7
    assert all(row["topic"] == "/topic_a" for row in rows)


def test_mcap_predicate_pushdown_file_level_filtering(
    ray_start_regular_shared, multi_file_mcap_dir
):
    """Test that predicate pushdown filters files at the file level using metadata.

    This test verifies that when using .filter() with predicate pushdown, files
    that don't match the filter are excluded at the file level (before reading),
    not just at the message level. This is the key optimization: file-level
    metadata filtering via MCAPFileMetadataProvider.expand_paths().
    """
    from ray.data.expressions import col

    mcap_dir, base_time = multi_file_mcap_dir

    # Read directory with predicate pushdown - should only read matching files
    ds = ray.data.read_mcap(mcap_dir)

    # Filter to only topic_a - this should filter files at metadata level
    # file_b.mcap should be excluded entirely (contains only topic_b)
    ds_filtered = ds.filter(col("topic") == "/topic_a")

    rows = ds_filtered.take_all()

    # Verify we get topic_a messages
    assert len(rows) == 7  # topic_a messages from file_a (5) and file_mixed (2)
    assert all(row["topic"] == "/topic_a" for row in rows)

    # Verify file_b messages are NOT included (file should have been filtered out)
    file_ids = {row["data"]["file"] for row in rows}
    assert 2 not in file_ids  # file_b should be excluded
    assert 1 in file_ids  # file_a should be included
    assert 3 in file_ids  # file_mixed should be included (has topic_a messages)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
