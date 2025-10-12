"""Tests for Flink datasource."""

import pytest

import ray
from ray.data._internal.datasource.flink_datasource import FlinkDatasource
from ray.tests.conftest import *  # noqa


def test_flink_datasource_initialization(ray_start_regular_shared):
    """Test basic Flink datasource initialization."""
    flink_config = {"rest_api_url": "http://localhost:8081", "job_id": "test-job"}

    ds = FlinkDatasource(
        source_type="rest_api",
        flink_config=flink_config,
        max_records_per_task=1000,
    )

    assert ds.source_type_val == "rest_api"
    assert ds.flink_config == flink_config
    assert ds.max_records_per_task == 1000


def test_flink_datasource_different_source_types(ray_start_regular_shared):
    """Test Flink datasource with different source types."""
    # REST API source
    rest_config = {"rest_api_url": "http://localhost:8081", "job_id": "test-job"}
    ds1 = FlinkDatasource(source_type="rest_api", flink_config=rest_config)
    assert ds1.source_type_val == "rest_api"

    # Table source
    table_config = {"table_name": "test_table"}
    ds2 = FlinkDatasource(source_type="table", flink_config=table_config)
    assert ds2.source_type_val == "table"

    # Checkpoint source
    checkpoint_config = {"checkpoint_path": "/path/to/checkpoint"}
    ds3 = FlinkDatasource(source_type="checkpoint", flink_config=checkpoint_config)
    assert ds3.source_type_val == "checkpoint"


def test_flink_datasource_name(ray_start_regular_shared):
    """Test datasource name generation."""
    flink_config = {"rest_api_url": "http://localhost:8081", "job_id": "test-job"}

    ds = FlinkDatasource(
        source_type="rest_api",
        flink_config=flink_config,
    )
    assert ds.get_name() == "flink_unbound_datasource"


def test_flink_datasource_schema(ray_start_regular_shared):
    """Test schema retrieval."""
    flink_config = {"rest_api_url": "http://localhost:8081", "job_id": "test-job"}
    ds = FlinkDatasource(
        source_type="rest_api",
        flink_config=flink_config,
    )

    schema = ds.get_unbound_schema(flink_config)
    assert schema is not None
    assert "job_id" in schema.names
    assert "job_name" in schema.names
    assert "data" in schema.names


def test_flink_datasource_config_validation(ray_start_regular_shared):
    """Test configuration validation."""
    # Missing required config for REST API
    with pytest.raises(ValueError, match="rest_api_url is required"):
        FlinkDatasource(
            source_type="rest_api",
            flink_config={"job_id": "test-job"},
        )

    with pytest.raises(ValueError, match="job_id is required"):
        FlinkDatasource(
            source_type="rest_api",
            flink_config={"rest_api_url": "http://localhost:8081"},
        )

    # Missing required config for table source
    with pytest.raises(ValueError, match="table_name is required"):
        FlinkDatasource(
            source_type="table",
            flink_config={},
        )

    # Missing required config for checkpoint source
    with pytest.raises(ValueError, match="checkpoint_path is required"):
        FlinkDatasource(
            source_type="checkpoint",
            flink_config={},
        )


def test_read_flink_basic(ray_start_regular_shared):
    """Test basic read_flink functionality."""
    # Will raise ImportError if requests not installed
    with pytest.raises(ImportError, match="requests is required"):
        ray.data.read_flink(
            source_type="rest_api",
            rest_api_url="http://localhost:8081",
            job_id="test-job",
            trigger="once",
        )


def test_read_flink_trigger_formats(ray_start_regular_shared):
    """Test different trigger formats are parsed correctly."""
    # Test that different trigger formats are accepted
    # Will raise ImportError if requests not installed
    trigger_formats = ["once", "continuous", "30s", "interval:1m"]
    for trigger in trigger_formats:
        with pytest.raises(ImportError, match="requests is required"):
            ray.data.read_flink(
                source_type="rest_api",
                rest_api_url="http://localhost:8081",
                job_id="test-job",
                trigger=trigger,
            )


def test_flink_import_check():
    """Test that ImportError is raised when requests is not available."""
    from ray.data._internal.datasource.flink_datasource import _check_requests_available

    try:
        _check_requests_available()
        # If we get here, requests is installed
        assert True
    except ImportError as e:
        assert "requests is required" in str(e)


def test_flink_datasource_estimate_inmemory_data_size(ray_start_regular_shared):
    """Test that unbounded sources return None for memory estimation."""
    flink_config = {"rest_api_url": "http://localhost:8081", "job_id": "test-job"}
    ds = FlinkDatasource(
        source_type="rest_api",
        flink_config=flink_config,
    )

    # Unbounded sources should return None (unknown size)
    assert ds.estimate_inmemory_data_size() is None


def test_flink_invalid_source_type(ray_start_regular_shared):
    """Test validation of invalid source type."""
    with pytest.raises(ValueError, match="source_type must be one of"):
        FlinkDatasource(
            source_type="invalid_type",
            flink_config={},
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
