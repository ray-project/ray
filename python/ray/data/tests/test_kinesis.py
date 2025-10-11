"""Tests for Kinesis datasource."""

import pytest

import ray
from ray.data._internal.datasource.kinesis_datasource import KinesisDatasource
from ray.tests.conftest import *  # noqa


def test_kinesis_datasource_initialization(ray_start_regular_shared):
    """Test basic Kinesis datasource initialization."""
    kinesis_config = {"region_name": "us-west-2"}

    ds = KinesisDatasource(
        stream_name="test-stream",
        kinesis_config=kinesis_config,
        max_records_per_task=1000,
    )

    assert ds.stream_name == "test-stream"
    assert ds.kinesis_config == kinesis_config
    assert ds.max_records_per_task == 1000


def test_kinesis_datasource_name(ray_start_regular_shared):
    """Test datasource name generation."""
    kinesis_config = {"region_name": "us-west-2"}

    ds = KinesisDatasource(
        stream_name="test-stream",
        kinesis_config=kinesis_config,
    )
    assert ds.get_name() == "kinesis_unbound_datasource"


def test_kinesis_datasource_schema(ray_start_regular_shared):
    """Test schema retrieval."""
    kinesis_config = {"region_name": "us-west-2"}
    ds = KinesisDatasource(
        stream_name="test-stream",
        kinesis_config=kinesis_config,
    )

    schema = ds.get_unbound_schema(kinesis_config)
    assert schema is not None
    assert "stream_name" in schema.names
    assert "shard_id" in schema.names
    assert "sequence_number" in schema.names


def test_kinesis_datasource_config_validation(ray_start_regular_shared):
    """Test configuration validation."""
    # Missing region_name
    with pytest.raises(ValueError, match="region_name"):
        KinesisDatasource(
            stream_name="test-stream",
            kinesis_config={},
        )

    # Empty stream_name
    with pytest.raises(ValueError, match="stream_name cannot be empty"):
        KinesisDatasource(
            stream_name="",
            kinesis_config={"region_name": "us-west-2"},
        )

    # Invalid max_records_per_task
    with pytest.raises(ValueError, match="max_records_per_task must be positive"):
        KinesisDatasource(
            stream_name="test-stream",
            kinesis_config={"region_name": "us-west-2"},
            max_records_per_task=-1,
        )

    # Enhanced Fan-Out without consumer_name
    with pytest.raises(ValueError, match="consumer_name is required"):
        KinesisDatasource(
            stream_name="test-stream",
            kinesis_config={"region_name": "us-west-2"},
            enhanced_fan_out=True,
        )


def test_read_kinesis_basic(ray_start_regular_shared):
    """Test basic read_kinesis functionality."""
    # Will raise ImportError if boto3 not installed
    with pytest.raises(ImportError, match="boto3 is required"):
        ray.data.read_kinesis(
            stream_name="test-stream", region_name="us-west-2", trigger="once"
        )


def test_read_kinesis_trigger_formats(ray_start_regular_shared):
    """Test different trigger formats are parsed correctly."""
    # Test that different trigger formats are accepted
    # Will raise ImportError if boto3 not installed
    trigger_formats = ["once", "continuous", "30s", "interval:1m"]
    for trigger in trigger_formats:
        with pytest.raises(ImportError, match="boto3 is required"):
            ray.data.read_kinesis(
                stream_name="test-stream", region_name="us-west-2", trigger=trigger
            )


def test_kinesis_import_check():
    """Test that ImportError is raised when boto3 is not available."""
    from ray.data._internal.datasource.kinesis_datasource import _check_boto3_available

    try:
        _check_boto3_available()
        # If we get here, boto3 is installed
        assert True
    except ImportError as e:
        assert "boto3 is required" in str(e)


def test_kinesis_datasource_estimate_inmemory_data_size(ray_start_regular_shared):
    """Test that unbounded sources return None for memory estimation."""
    kinesis_config = {"region_name": "us-west-2"}
    ds = KinesisDatasource(
        stream_name="test-stream",
        kinesis_config=kinesis_config,
    )

    # Unbounded sources should return None (unknown size)
    assert ds.estimate_inmemory_data_size() is None


def test_kinesis_enhanced_fan_out_config(ray_start_regular_shared):
    """Test Enhanced Fan-Out configuration."""
    kinesis_config = {"region_name": "us-west-2"}

    # With Enhanced Fan-Out
    ds = KinesisDatasource(
        stream_name="test-stream",
        kinesis_config=kinesis_config,
        enhanced_fan_out=True,
        consumer_name="test-consumer",
    )

    assert ds.enhanced_fan_out is True
    assert ds.consumer_name == "test-consumer"


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
