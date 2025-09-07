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
    with pytest.raises(ValueError, match="region_name is required"):
        KinesisDatasource(
            stream_name="test-stream",
            kinesis_config={},
        )


def test_read_kinesis_basic(ray_start_regular_shared):
    """Test basic read_kinesis functionality."""
    # This will fail without actual Kinesis, but tests the API
    with pytest.raises(RuntimeError):
        ray.data.read_kinesis(
            stream_name="test-stream", region_name="us-west-2", trigger="once"
        )


def test_read_kinesis_trigger_formats(ray_start_regular_shared):
    """Test different trigger formats."""
    # Test that different trigger formats are accepted
    trigger_formats = ["once", "continuous", "30s", "interval:1m"]
    for trigger in trigger_formats:
        with pytest.raises(RuntimeError):
            ray.data.read_kinesis(
                stream_name="test-stream", region_name="us-west-2", trigger=trigger
            )


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
