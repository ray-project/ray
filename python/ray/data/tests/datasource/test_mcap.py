import importlib.util
import json
import logging
import os
import re
from typing import Any, Dict

import pyarrow as pa
import pytest

import ray
from ray.data._internal.datasource_v2.formats.mcap import mcap_datasource_v2
from ray.data.datasource.path_util import _unwrap_protocol
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

    ``True`` reads through ``MCAPDatasourceV2`` and ``False`` through the legacy
    ``MCAPDatasource``. Both must return the same rows.
    """
    ray.data.DataContext.get_current().use_datasource_v2 = request.param
    return request.param


def create_test_mcap_file(file_path: str, messages: list, index: bool = True) -> None:
    """Create a test MCAP file with given messages.

    With ``index=False`` the file has no chunks and no chunk index, so readers
    scan it from the start.
    """
    from mcap.writer import IndexType, Writer

    with open(file_path, "wb") as stream:
        if index:
            writer = Writer(stream)
        else:
            writer = Writer(
                stream,
                use_chunking=False,
                index_types=IndexType.NONE,
                use_statistics=False,
                use_summary_offsets=False,
            )
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
        # On V2, files are listed at execution time, so ``input_files()`` is empty.
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
    ray_start_regular_shared, tmp_path, override_num_blocks, datasource_v2, caplog
):
    """A read with ``override_num_blocks`` still returns every row.

    The V2 datasource sizes read tasks by bytes, so it ignores the setting and warns.
    """
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

    # Ray's loggers do not propagate to the root logger caplog listens on.
    read_api_logger = logging.getLogger("ray.data.read_api")
    read_api_logger.addHandler(caplog.handler)
    try:
        ds = ray.data.read_mcap(path, override_num_blocks=override_num_blocks)
    finally:
        read_api_logger.removeHandler(caplog.handler)

    assert ds.count() == 3
    assert len(ds.take_all()) == 3
    warned = any(
        "ignores `override_num_blocks`" in record.message for record in caplog.records
    )
    assert warned == datasource_v2


def test_read_mcap_passes_memory_to_the_v2_datasource(
    ray_start_regular_shared, simple_mcap_file, datasource_v2, monkeypatch
):
    """``memory``, or else ``ray_remote_args["memory"]``, reaches the V2 datasource,
    so its memory warning knows what each read task reserves."""
    if not datasource_v2:
        pytest.skip("Only the V2 datasource takes the reservation.")
    seen = []
    original_init = mcap_datasource_v2.MCAPDatasourceV2.__init__

    def record_reservation(self, *args, **kwargs):
        seen.append(kwargs.get("read_task_memory"))
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(
        mcap_datasource_v2.MCAPDatasourceV2, "__init__", record_reservation
    )

    ray.data.read_mcap(simple_mcap_file, memory=2 * 1024**3)
    ray.data.read_mcap(simple_mcap_file, ray_remote_args={"memory": 1024**3})
    ray.data.read_mcap(simple_mcap_file)

    assert seen == [2 * 1024**3, 1024**3, None]


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


def test_read_mcap_partition_filter_applies_to_schema_planning(
    ray_start_regular_shared, tmp_path
):
    """Planning opens only the files the partition filter keeps.

    The sixteen sampled files hold no ``/a``, so planning looks further. The
    filtered-out partition's ``/a`` is CDR; were it opened, ``data`` would be
    planned as bytes.
    """
    from mcap.writer import Writer

    from ray.data.datasource.partitioning import PartitionStyle, PathPartitionFilter

    def write(path, topic, message_encoding):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as stream:
            writer = Writer(stream)
            writer.start(profile="", library="ray-test")
            schema_id = writer.register_schema(
                name="test_schema", encoding="jsonschema", data=b"{}"
            )
            channel_id = writer.register_channel(
                schema_id=schema_id, topic=topic, message_encoding=message_encoding
            )
            writer.add_message(
                channel_id=channel_id,
                log_time=1000000000,
                publish_time=1000000000,
                data=json.dumps({"value": 1}).encode(),
            )
            writer.finish()

    write(os.path.join(tmp_path, "p=a", "dropped.mcap"), "/a", "cdr")
    for i in range(16):
        write(os.path.join(tmp_path, "p=b", f"other_{i:02d}.mcap"), "/b", "json")
    write(os.path.join(tmp_path, "p=b", "zz_selected.mcap"), "/a", "json")
    partition_filter = PathPartitionFilter.of(
        filter_fn=lambda partitions: partitions["p"] == "b",
        style=PartitionStyle.HIVE,
        base_dir=str(tmp_path),
    )

    ds = ray.data.read_mcap(
        str(tmp_path), topics=["/a"], partition_filter=partition_filter
    )
    assert [row["data"] for row in ds.take_all()] == [{"value": 1}]


@pytest.mark.parametrize("index", [True, False], ids=["indexed", "unindexed"])
def test_read_mcap_log_time_order(
    ray_start_regular_shared, tmp_path, datasource_v2, index
):
    """Messages written out of log-time order come back sorted by default, with
    or without a chunk index.

    With ``log_time_order=False`` they come back in the order they were written.
    The legacy datasource always sorts, so it refuses ``False``.
    """
    path = os.path.join(tmp_path, "unordered.mcap")
    messages = [
        {"topic": "/test", "data": {"seq": i}, "log_time": log_time}
        for i, log_time in enumerate([3_000_000_000, 1_000_000_000, 2_000_000_000])
    ]
    create_test_mcap_file(path, messages, index=index)

    ordered = ray.data.read_mcap(path).take_all()
    assert [row["data"]["seq"] for row in ordered] == [1, 2, 0]

    if not datasource_v2:
        with pytest.raises(NotImplementedError, match="log_time_order"):
            ray.data.read_mcap(path, log_time_order=False)
        return
    as_written = ray.data.read_mcap(path, log_time_order=False).take_all()
    assert [row["data"]["seq"] for row in as_written] == [0, 1, 2]


def _write_chunked_mcap(path, num_messages, topics=("/a", "/b", "/c")):
    """Write one message per chunk, so every message boundary is a split point."""
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
    """On V2, a one-byte packing budget reads each chunk of a file in its own task.

    The nine-chunk file then comes out as nine blocks.
    """
    path = os.path.join(tmp_path, "chunked.mcap")
    _write_chunked_mcap(path, 9)
    monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", 1)

    ds = ray.data.read_mcap(path).materialize()

    assert ds.count() == 9
    assert sorted(row["data"]["seq"] for row in ds.take_all()) == list(range(9))
    if datasource_v2:
        assert ds.num_blocks() == 9


def _heaviest_chunk(path, **options) -> int:
    """The weight the V2 listing gives the heaviest chunk of ``path``."""
    datasource = mcap_datasource_v2.MCAPDatasourceV2([path], **options)
    (manifest,) = datasource._get_file_indexer().list_files(
        pa.array([path]), filesystem=datasource.filesystem
    )
    return max(int(md["size_bytes"]) for md in manifest.file_chunk_metadatas)


def test_read_mcap_preserve_order_keeps_split_files_in_log_time_order(
    ray_start_regular_shared, tmp_path, monkeypatch, datasource_v2
):
    """With ``preserve_order``, each file comes out in ``log_time`` order from
    start to end, and the files follow each other in the order they are passed.

    On V2 each file is split across several read tasks, and each path is listed
    by a listing task of its own.
    """
    num_files, num_messages = 3, 12
    paths = [os.path.join(tmp_path, f"f{i}.mcap") for i in range(num_files)]
    for path in paths:
        _write_chunked_mcap(path, num_messages)
    ray.data.DataContext.get_current().execution_options.preserve_order = True
    if datasource_v2:
        # Room for two or three chunks per task, so every file needs several.
        budget = int(2.5 * _heaviest_chunk(paths[0], include_paths=True))
        monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", budget)

    ds = ray.data.read_mcap(paths, include_paths=True).materialize()
    rows = [(os.path.basename(r["path"]), r["log_time"]) for r in ds.take_all()]

    assert rows == [
        (f"f{i}.mcap", 1_000_000_000 + j * 1_000_000)
        for i in range(num_files)
        for j in range(num_messages)
    ]
    if datasource_v2:
        assert ds.num_blocks() >= 3 * num_files


def test_read_mcap_preserve_order_keeps_files_of_mixed_size_in_listing_order(
    ray_start_regular_shared, tmp_path, monkeypatch, datasource_v2
):
    """With ``preserve_order``, a large file listed between two small ones in a
    directory comes out between them, as the legacy reader returns them.

    On V2 one listing task lists the directory. The large file needs a task of
    its own, while the small ones would fit one task together.
    """
    # The directory lists its files in name order.
    sizes = {"a_small.mcap": 1, "b_large.mcap": 12, "c_small.mcap": 1}
    paths = [os.path.join(tmp_path, name) for name in sizes]
    for path, num_messages in zip(paths, sizes.values()):
        _write_chunked_mcap(path, num_messages)
    ray.data.DataContext.get_current().execution_options.preserve_order = True
    if datasource_v2:
        budget = int(2.5 * _heaviest_chunk(paths[1], include_paths=True))
        monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", budget)

    ds = ray.data.read_mcap(str(tmp_path), include_paths=True)
    files = [os.path.basename(row["path"]) for row in ds.take_all()]

    assert files == [name for name, count in sizes.items() for _ in range(count)]


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

    monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", 1)
    resplit = ray.data.read_mcap(paths, include_row_id=True).materialize()
    # The read really is split: a task per chunk, not one per file.
    assert resplit.num_blocks() > len(paths)
    assert {row["row_id"]: row["log_time"] for row in resplit.take_all()} == by_id

    # The topic filter drops rows but does not change the ids of the rows it keeps.
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


def test_read_mcap_v1_refuses_a_row_id_checkpoint(
    ray_start_regular_shared, simple_mcap_file, datasource_v2
):
    """A checkpoint keyed on ``row_id`` needs the column, which only the V2
    datasource builds. On the V1 legacy design ``read_mcap`` refuses it at once,
    instead of reading without the ids the checkpoint needs."""
    from ray.data.checkpoint import CheckpointConfig

    if datasource_v2:
        pytest.skip("V2 turns row_id on")
    ctx = ray.data.DataContext.get_current()
    ctx.checkpoint_config = CheckpointConfig(
        id_column="row_id", checkpoint_path="/tmp/ray_data_mcap_ckpt_unused"
    )

    with pytest.raises(NotImplementedError, match="checkpoint keyed on `row_id`"):
        ray.data.read_mcap(simple_mcap_file)


@pytest.mark.parametrize("name", ["row_id", "uid"])
def test_read_mcap_refuses_generated_id_column(
    ray_start_regular_shared, simple_mcap_file, name
):
    """A checkpoint config that asks for generated ids fails at ``read_mcap``.

    Only ``read_parquet`` builds generated ids. The name ``row_id`` must not turn
    on MCAP's own ``row_id`` column instead.
    """
    from ray.data.checkpoint import CheckpointConfig
    from ray.data.checkpoint.interfaces import InvalidCheckpointingConfig

    ctx = ray.data.DataContext.get_current()
    ctx.checkpoint_config = CheckpointConfig(
        generated_id_column=name, checkpoint_path="/tmp/ray_data_mcap_ckpt_unused"
    )

    with pytest.raises(
        InvalidCheckpointingConfig, match="works only with `read_parquet`"
    ):
        ray.data.read_mcap(simple_mcap_file)


def test_read_mcap_v2_schema_matches_rows(
    ray_start_regular_shared, multi_topic_mcap_file, datasource_v2
):
    """The planning-time schema is the schema of the blocks that come out."""
    if not datasource_v2:
        pytest.skip("the legacy path infers its schema from the first block")
    ds = ray.data.read_mcap(multi_topic_mcap_file, include_paths=True)
    planned = ds.schema()
    (bundle,) = ds.iter_internal_ref_bundles()
    (block,) = ray.get(bundle.block_refs)
    assert block.schema.names == planned.names
    assert block.schema.types == planned.types


def test_read_mcap_schema_matches_messages_whose_fields_vary(
    ray_start_regular_shared, tmp_path, datasource_v2
):
    """``ds.schema()`` gives ``data`` the type its blocks hold when the first
    message lacks a field and has an int where later messages have a float."""
    path = os.path.join(tmp_path, "varying.mcap")
    payloads = [{"x": 1}, {"x": 2.5, "y": "a"}, {"x": 3}]
    create_test_mcap_file(
        path,
        [{"topic": "/a", "data": p, "log_time": i} for i, p in enumerate(payloads)],
    )
    ds = ray.data.read_mcap(path)

    planned = ds.schema().base_schema.field("data").type
    materialized = ds.materialize().schema().base_schema.field("data").type

    assert planned == pa.struct([("x", pa.float64()), ("y", pa.string())])
    assert materialized == planned


@pytest.mark.parametrize("corruption", ["cut_short", "not_mcap"])
def test_read_mcap_max_errored_blocks_skips_a_corrupt_recording(
    ray_start_regular_shared, tmp_path, datasource_v2, corruption
):
    """A folder with one corrupt recording reads the others when
    ``max_errored_blocks`` allows one error, and fails naming the file when not.
    """
    folder = os.path.join(tmp_path, "drive")
    os.makedirs(folder)
    valid = os.path.join(folder, "1.mcap")
    create_test_mcap_file(
        valid,
        [{"topic": "/imu", "data": {"i": i}, "log_time": i} for i in range(3)],
    )
    with open(valid, "rb") as f:
        data = f.read()
    corrupt = os.path.join(folder, "2.mcap")
    with open(corrupt, "wb") as f:
        # A run that crashed mid-recording, or a file that is not MCAP at all.
        f.write(data[: len(data) // 2] if corruption == "cut_short" else b"x" * 100)
    ctx = ray.data.DataContext.get_current()

    ctx.max_errored_blocks = 1
    assert ray.data.read_mcap(folder).count() == 3

    ctx.max_errored_blocks = 0
    with pytest.raises(Exception) as error:
        ray.data.read_mcap(folder).count()
    if datasource_v2:
        assert "2.mcap" in str(error.value)


def write_folder_with_a_corrupt_chunk(tmp_path) -> str:
    """Write ``a.mcap``, ``b.mcap`` and ``c.mcap`` (three messages each) into one
    folder, with ``b.mcap``'s only chunk zeroed: its summary reads, its messages
    do not. Returns the folder."""
    from ray.data.tests.unit.datasource_v2.mcap_testing import write_corrupt_chunk

    folder = os.path.join(tmp_path, "drive")
    os.makedirs(folder)
    for name in ("a", "c"):
        create_test_mcap_file(
            os.path.join(folder, f"{name}.mcap"),
            [{"topic": "/imu", "data": {"i": i}, "log_time": i} for i in range(3)],
        )
    a = os.path.join(folder, "a.mcap")
    write_corrupt_chunk(os.path.join(folder, "b.mcap"), a, 0)
    return folder


@pytest.mark.parametrize("fused_map", [False, True])
def test_read_mcap_max_errored_blocks_keeps_files_sharing_a_task_with_a_corrupt_chunk(
    ray_start_regular_shared, tmp_path, datasource_v2, fused_map
):
    """With ``max_errored_blocks=1``, a recording whose only chunk is corrupt is
    skipped and every other file is read, including files in the same read task,
    also when a map is fused into the read."""
    folder = write_folder_with_a_corrupt_chunk(tmp_path)
    ray.data.DataContext.get_current().max_errored_blocks = 1
    ds = ray.data.read_mcap(folder, include_paths=True)
    if fused_map:
        ds = ds.map_batches(lambda batch: batch)

    rows = ds.take_all()

    # V2 packs the three small files into one read task; legacy splits them
    # over two tasks on this one-CPU cluster, a.mcap with b.mcap.
    files = sorted(os.path.basename(row["path"]) for row in rows)
    assert files == ["a.mcap"] * 3 + ["c.mcap"] * 3


@pytest.mark.parametrize("fused_map", [False, True])
def test_read_mcap_max_errored_blocks_loses_only_the_rows_of_corrupt_chunks(
    ray_start_regular_shared, tmp_path, datasource_v2, fused_map
):
    """With ``max_errored_blocks=1``, V2 loses only the rows of a file's two
    corrupt chunks, also when a map is fused into the read. Their read task
    raises once for both, so one allowed error is enough."""
    if not datasource_v2:
        pytest.skip("the legacy reader stops at a file's first corrupt chunk")
    from ray.data.tests.unit.datasource_v2.mcap_testing import (
        CHUNKED_FILE_MESSAGES,
        round_robin_messages,
        write_corrupt_chunk,
        write_mcap,
    )

    folder = os.path.join(tmp_path, "drive")
    os.makedirs(folder)
    good = os.path.join(folder, "a.mcap")
    # One message per chunk; b.mcap is a.mcap with chunks 2 and 6 zeroed.
    write_mcap(good, round_robin_messages(CHUNKED_FILE_MESSAGES))
    once = write_corrupt_chunk(os.path.join(tmp_path, "once.mcap"), good, 2)
    write_corrupt_chunk(os.path.join(folder, "b.mcap"), once, 6)
    ray.data.DataContext.get_current().max_errored_blocks = 1
    ds = ray.data.read_mcap(folder, include_paths=True)
    if fused_map:
        ds = ds.map_batches(lambda batch: batch)

    rows = ds.take_all()

    seqs = sorted((os.path.basename(row["path"]), row["data"]["seq"]) for row in rows)
    expected = [("a.mcap", i) for i in range(CHUNKED_FILE_MESSAGES)]
    expected += [("b.mcap", i) for i in range(CHUNKED_FILE_MESSAGES) if i not in (2, 6)]
    assert seqs == expected


def test_read_mcap_lists_files_in_parallel(
    ray_start_regular_shared, tmp_path, datasource_v2
):
    """The V2 listing packs per task, so several files are listed by several tasks."""
    if not datasource_v2:
        pytest.skip("ListFiles tasks exist only on the V2 read path")

    paths = []
    for i in range(3):
        path = os.path.join(tmp_path, f"parallel_{i}.mcap")
        create_test_mcap_file(
            path,
            [{"topic": "/t", "data": {"i": i}, "log_time": 1_000_000_000 + i}],
        )
        paths.append(path)

    ds = ray.data.read_mcap(paths).materialize()
    assert sorted(row["data"]["i"] for row in ds.take_all()) == [0, 1, 2]
    match = re.search(r"ListFiles: (\d+) tasks executed", ds.stats())
    assert match is not None, ds.stats()
    # One listing task per path (up to 200), not a single task for all paths,
    # because the MCAP packer packs per listing shard.
    assert int(match.group(1)) == 3


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
        cam_schema_id = writer.register_schema(
            name="foxglove_msgs/msg/CompressedVideo", encoding="ros2msg", data=b""
        )
        imu_schema = writer.register_schema(
            name="sensor_msgs/msg/Imu", encoding="ros2msg", data=b""
        )
        cam = writer.register_channel(
            schema_id=cam_schema_id, topic="/cam", message_encoding="cdr"
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


def _sample_rows(path, anchors):
    """One sample per anchor in ``path``, clipped to [0.5 s, 2.5 s)."""
    return [
        {
            "sample_id": f"s{i}",
            "files": [path],
            "clip_start": 500_000_000,
            "clip_end": 2_500_000_000,
            "anchor": anchor,
        }
        for i, anchor in enumerate(anchors)
    ]


def test_read_mcap_samples(ray_start_regular_shared, tmp_path, datasource_v2):
    """A sample read gives one row per sample: its own columns, then each topic's
    steps. Projection, ``count`` and ``limit`` apply to it. The legacy datasource
    refuses it."""
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import steps

    path = os.path.join(tmp_path, "video.mcap")
    _write_video_recording(path)
    samples = ray.data.from_items(
        _sample_rows(path, [1_000_000_000, 1_500_000_000, 2_400_000_000])
    )
    topics = {
        "/cam": TopicSpec(steps.before(0.3, fps=10)),
        "/imu": TopicSpec(steps.after(frames=3, fps=20), pad="zeros"),
    }

    if not datasource_v2:
        with pytest.raises(NotImplementedError, match="V2 datasource"):
            ray.data.read_mcap(samples=samples, topics=topics)
        return
    ds = ray.data.read_mcap(samples=samples, topics=topics)
    rows = sorted(ds.take_all(), key=lambda row: row["anchor"])

    assert ds.schema().names == [
        "sample_id",
        "files",
        "clip_start",
        "clip_end",
        "anchor",
        "data:/cam",
        "times:/cam",
        "valid:/cam",
        "data:/imu",
        "times:/imu",
        "valid:/imu",
    ]
    cam = [[int.from_bytes(d[-4:], "big") for d in row["data:/cam"]] for row in rows]
    assert cam == [[8, 9, 10], [13, 14, 15], [22, 23, 24]]
    # The last sample's steps at 2.5 and 2.55 s lie past its clip, so are empty.
    assert [row["valid:/imu"] for row in rows] == [[True] * 3] * 2 + [
        [True, False, False]
    ]
    assert rows[2]["data:/imu"][1:] == [b"", b""]
    assert ds.count() == 3
    assert ds.limit(2).count() == 2
    assert sorted(
        r["sample_id"] for r in ds.select_columns(["sample_id"]).take_all()
    ) == [
        "s0",
        "s1",
        "s2",
    ]


def test_read_mcap_samples_all_in_step(
    ray_start_regular_shared, tmp_path, datasource_v2
):
    """``match.all_in_step`` gives each step every message of its slot, through
    a full read."""
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import match, steps

    if not datasource_v2:
        pytest.skip("sample reads are V2 only")
    path = os.path.join(tmp_path, "video.mcap")
    _write_video_recording(path)
    samples = ray.data.from_items(_sample_rows(path, [1_000_000_000]))
    topics = {"/imu": TopicSpec(steps.before(0.2, fps=10), match.all_in_step())}

    (row,) = ray.data.read_mcap(samples=samples, topics=topics).take_all()

    # Steps at 0.9 and 1.0 s over a 50 Hz IMU: (0.8, 0.9] and (0.9, 1.0].
    assert [[t // 1_000_000 for t in step] for step in row["times:/imu"]] == [
        [820, 840, 860, 880, 900],
        [920, 940, 960, 980, 1000],
    ]
    assert row["valid:/imu"] == [True, True]


@pytest.mark.parametrize(
    "kwargs, error, match",
    [
        ({"paths": "x.mcap"}, ValueError, r"does not take \['paths'\]"),
        ({"time_range": (0, 1)}, ValueError, "time_range"),
        ({"topics": ["/cam"]}, ValueError, r"topics=\{topic: TopicSpec"),
        ({"topics": {"/cam": 0.5}}, ValueError, "map each topic to a TopicSpec"),
        ({"time_source": "header"}, NotImplementedError, "header"),
        ({"time_source": "wall"}, ValueError, "time_source must be"),
    ],
)
def test_read_mcap_samples_rejects_options(
    ray_start_regular_shared, tmp_path, datasource_v2, kwargs, error, match
):
    """A sample read refuses the options of a read of paths, and needs a
    ``TopicSpec`` per topic and log time."""
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import steps

    if not datasource_v2:
        pytest.skip("validated on the V2 path")
    samples = ray.data.from_items(_sample_rows("x.mcap", [1_000_000_000]))
    options: Dict[str, Any] = {"topics": {"/cam": TopicSpec(steps.last(1))}}
    options.update(kwargs)

    with pytest.raises(error, match=match):
        ray.data.read_mcap(samples=samples, **options)


def test_read_mcap_whole_clips_without_anchors(
    ray_start_regular_shared, tmp_path, datasource_v2
):
    """A captioning read: clips of any length with no anchor column, a fixed
    number of camera frames across each clip, and the IMU's last sample in
    it."""
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import steps

    if not datasource_v2:
        pytest.skip("validated on the V2 path")
    path = os.path.join(tmp_path, "video.mcap")
    _write_video_recording(path)
    # Clips of 1 s and 2 s; the camera logs a frame every 0.1 s.
    clips = ray.data.from_items(
        [
            {
                "clip_id": "a",
                "files": [path],
                "clip_start": 0,
                "clip_end": 1_000_000_000,
            },
            {
                "clip_id": "b",
                "files": [path],
                "clip_start": 1_000_000_000,
                "clip_end": 3_000_000_000,
            },
        ]
    )

    ds = ray.data.read_mcap(
        samples=clips,
        topics={
            "/cam": TopicSpec(steps.across(3)),
            "/imu": TopicSpec(steps.at("clip_end")),
        },
    )
    rows = {row["clip_id"]: row for row in ds.take_all()}

    cam = {
        k: [int.from_bytes(d[-4:], "big") for d in r["data:/cam"]]
        for k, r in rows.items()
    }
    assert cam == {"a": [0, 5, 9], "b": [10, 20, 29]}
    # The IMU logs every 20 ms, so its last sample in a clip is 20 ms before
    # the clip's end.
    assert {k: r["times:/imu"] for k, r in rows.items()} == {
        "a": [980_000_000],
        "b": [2_980_000_000],
    }


def test_read_mcap_samples_needs_its_columns(
    ray_start_regular_shared, simple_mcap_file, datasource_v2
):
    """The samples need an anchor column when a topic measures from the anchor,
    and ``topics`` takes specs only with samples."""
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import steps

    if not datasource_v2:
        pytest.skip("validated on the V2 path")
    topics = {"/cam": TopicSpec(steps.last(1))}
    no_anchor = ray.data.from_items(
        [{"files": ["x.mcap"], "clip_start": 0, "clip_end": 10}]
    )

    with pytest.raises(ValueError, match=r"topics \['/cam'\] measure from it"):
        ray.data.read_mcap(samples=no_anchor, topics=topics)
    with pytest.raises(ValueError, match="only in a sample read"):
        ray.data.read_mcap(simple_mcap_file, topics=topics)
    with pytest.raises(ValueError, match="needs the paths to read"):
        ray.data.read_mcap(topics=["/cam"])


def test_read_mcap_samples_with_no_samples_reads_no_rows(
    ray_start_regular_shared, datasource_v2
):
    """A samples dataset that a filter empties reads zero rows: planning has
    nothing to estimate task sizes from and falls back to the default."""
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import steps

    if not datasource_v2:
        pytest.skip("sample reads are V2 only")
    samples = ray.data.from_items(_sample_rows("x.mcap", [1_000_000_000]))
    no_samples = samples.filter(lambda row: False)

    ds = ray.data.read_mcap(
        samples=no_samples, topics={"/cam": TopicSpec(steps.last(1))}
    )

    assert ds.take_all() == []


def test_read_mcap_samples_plans_without_warnings(
    ray_start_regular_shared, restore_data_context, tmp_path, datasource_v2
):
    """Under a checkpoint config, planning reads the samples' schema and first
    rows without it, so planning a sample read warns about nothing."""
    import warnings

    from ray.data.checkpoint import CheckpointConfig
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import steps

    if not datasource_v2:
        pytest.skip("sample reads are V2 only")
    ray.data.DataContext.get_current().checkpoint_config = CheckpointConfig(
        id_column="sample_id", checkpoint_path=str(tmp_path / "checkpoint")
    )
    # A map, so the samples' schema is only known by running them.
    samples = ray.data.from_items(_sample_rows("x.mcap", [1_000_000_000])).map_batches(
        lambda batch: batch, batch_format="pyarrow"
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ray.data.read_mcap(samples=samples, topics={"/cam": TopicSpec(steps.last(1))})

    assert [str(w.message) for w in caught] == []


def test_read_mcap_samples_of_local_files_read_on_the_driver(
    ray_start_regular_shared, tmp_path, datasource_v2
):
    """Samples whose files use the ``local://`` scheme read on the driver's node,
    as a read of ``local://`` paths does, since only it can see them."""
    from ray.data._internal.logical.operators.read_operator import ReadFiles
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import steps

    if not datasource_v2:
        pytest.skip("sample reads are V2 only")
    path = os.path.join(tmp_path, "video.mcap")
    _write_video_recording(path)
    samples = ray.data.from_items(_sample_rows(f"local://{path}", [1_000_000_000]))

    ds = ray.data.read_mcap(samples=samples, topics={"/cam": TopicSpec(steps.last(1))})

    read_op = ds._logical_plan.dag
    assert isinstance(read_op, ReadFiles)
    # The selector names the node by its id, under Ray's node-id label.
    label_selector = read_op.ray_remote_args["label_selector"]
    assert list(label_selector.values()) == [ray.get_runtime_context().get_node_id()]
    assert [row["valid:/cam"] for row in ds.take_all()] == [[True]]


def test_read_mcap_samples_of_local_files_fail_under_ray_client(
    ray_start_regular_shared, tmp_path, datasource_v2, monkeypatch
):
    """Through Ray Client, samples whose files use the ``local://`` scheme fail
    the read, as a read of ``local://`` paths does: the cluster's read tasks
    can't see the client's disk."""
    import ray.util.client
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import steps

    if not datasource_v2:
        pytest.skip("sample reads are V2 only")
    path = os.path.join(tmp_path, "video.mcap")
    _write_video_recording(path)
    samples = ray.data.from_items(_sample_rows(f"local://{path}", [1_000_000_000]))
    # Plan the samples dataset first: the read is what Ray Client must refuse.
    samples = samples.materialize()
    monkeypatch.setattr(ray.util.client.ray, "is_connected", lambda: True)

    with pytest.raises(ValueError, match="Ray Client"):
        ray.data.read_mcap(samples=samples, topics={"/cam": TopicSpec(steps.last(1))})


def test_read_mcap_samples_backward_and_within(
    ray_start_regular_shared, tmp_path, datasource_v2
):
    """``match.backward`` takes the last IMU sample at or before each step, and
    ``within="recording"`` lets a camera's steps run past the clip's end."""
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import match, steps

    if not datasource_v2:
        pytest.skip("sample reads are V2 only")
    path = os.path.join(tmp_path, "video.mcap")
    _write_video_recording(path)
    # Steps at 2.415 and 2.425 s: nearest would take the 50 Hz IMU sample at
    # 2.42 s for both, backward takes 2.40 s for the first. The camera's steps
    # at 2.5 and 2.6 s pass the clip's end at 2.5 s.
    samples = ray.data.from_items(_sample_rows(path, [2_425_000_000]))
    topics = {
        "/imu": TopicSpec(steps.before(frames=2, fps=100), match.backward()),
        "/cam": TopicSpec(steps.after(0.2, fps=10, within="recording")),
    }

    (row,) = ray.data.read_mcap(samples=samples, topics=topics).take_all()

    assert row["times:/imu"] == [2_400_000_000, 2_420_000_000]
    assert row["times:/cam"] == [2_500_000_000, 2_600_000_000]
    assert row["valid:/cam"] == [True, True]


def test_read_mcap_samples_refuses_a_row_id_checkpoint(
    ray_start_regular_shared, restore_data_context, datasource_v2
):
    """Sample rows have no ``row_id``, so a checkpoint keyed on it is refused up
    front rather than when the first row is written."""
    from ray.data.checkpoint import CheckpointConfig
    from ray.data.checkpoint.interfaces import InvalidCheckpointingConfig
    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import steps

    ray.data.DataContext.get_current().checkpoint_config = CheckpointConfig(
        id_column="row_id", checkpoint_path="/tmp/unused"
    )
    samples = ray.data.from_items(_sample_rows("x.mcap", [1_000_000_000]))

    with pytest.raises(InvalidCheckpointingConfig, match='id_column="sample_id"'):
        ray.data.read_mcap(samples=samples, topics={"/cam": TopicSpec(steps.last(1))})


@pytest.mark.skipif(
    importlib.util.find_spec("av") is None,
    reason="av not available. Install with: pip install av",
)
def test_read_mcap_decode_samples(ray_start_regular_shared, tmp_path, datasource_v2):
    """With ``video``, a sample read decodes its camera topics: each row holds
    one frame per step, resized to one shape. A read of paths refuses
    ``video``."""
    from ray.data.datasource import TopicSpec, VideoOptions
    from ray.data.datasource.mcap import steps
    from ray.data.tests.unit.datasource_v2.mcap_testing import (
        FRAME_NS,
        encode_h264,
        write_payloads,
    )

    if not datasource_v2:
        pytest.skip("sample reads are V2 only")
    path = os.path.join(tmp_path, "h264.mcap")
    # 30 frames at about 30 fps, a keyframe every 10, in small chunks.
    write_payloads(
        path, encode_h264(30), schema_name="foxglove.CompressedVideo", chunk_size=400
    )
    # Anchored at frames 5, 15 and 25; the clip holds the whole stream.
    samples = ray.data.from_items(
        [
            {
                "files": [path],
                "clip_start": 0,
                "clip_end": 30 * FRAME_NS,
                "anchor": k * FRAME_NS,
            }
            for k in (5, 15, 25)
        ]
    )
    topics = {"/camera": TopicSpec(steps.before(frames=3, fps=10))}

    ds = ray.data.read_mcap(
        samples=samples, topics=topics, video=VideoOptions(resize=(24, 32))
    )
    rows = sorted(ds.take_all(), key=lambda row: row["anchor"])

    assert "frames:/camera" in ds.schema().names
    assert "data:/camera" not in ds.schema().names
    assert [row["frames:/camera"].shape for row in rows] == [(3, 24, 32, 3)] * 3
    # Frame 5's steps reach before the stream starts and repeat its first frame.
    assert rows[0]["valid:/camera"] == [False, True, True]
    assert all(row["valid:/camera"] == [True] * 3 for row in rows[1:])
    with pytest.raises(ValueError, match="only to a sample read"):
        ray.data.read_mcap(path, video=VideoOptions())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
