"""Tests for Kafka datasource."""

import pytest

import ray
from ray.data._internal.datasource.unbound.kafka_datasource import KafkaDatasource
from ray.tests.conftest import *  # noqa


def test_kafka_datasource_initialization(ray_start_regular_shared):
    """Test basic Kafka datasource initialization."""
    kafka_config = {"bootstrap_servers": "localhost:9092"}

    ds = KafkaDatasource(
        topics=["test-topic"],
        kafka_config=kafka_config,
        max_records_per_task=1000,
    )

    assert ds.topics == ["test-topic"]
    assert ds.kafka_config == kafka_config
    assert ds.max_records_per_task == 1000


def test_kafka_datasource_multiple_topics(ray_start_regular_shared):
    """Test Kafka datasource with multiple topics."""
    kafka_config = {"bootstrap_servers": "localhost:9092"}

    ds = KafkaDatasource(
        topics=["topic1", "topic2"],
        kafka_config=kafka_config,
    )

    assert ds.topics == ["topic1", "topic2"]


def test_kafka_datasource_string_topic(ray_start_regular_shared):
    """Test Kafka datasource with single string topic."""
    kafka_config = {"bootstrap_servers": "localhost:9092"}

    ds = KafkaDatasource(
        topics="single-topic",
        kafka_config=kafka_config,
    )

    assert ds.topics == ["single-topic"]


def test_kafka_datasource_name(ray_start_regular_shared):
    """Test datasource name generation."""
    kafka_config = {"bootstrap_servers": "localhost:9092"}

    ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)
    assert ds.get_name() == "kafka_unbound_datasource"


def test_kafka_datasource_schema(ray_start_regular_shared):
    """Test schema retrieval."""
    kafka_config = {"bootstrap_servers": "localhost:9092"}
    ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)

    schema = ds.get_unbound_schema(kafka_config)
    assert schema is not None
    assert "topic" in schema.names
    assert "partition" in schema.names
    assert "offset" in schema.names


def test_kafka_datasource_config_validation(ray_start_regular_shared):
    """Test configuration validation."""
    # Missing bootstrap_servers
    with pytest.raises(ValueError, match="bootstrap_servers is required"):
        KafkaDatasource(
            topics=["test"],
            kafka_config={},
        )


def test_read_kafka_basic(ray_start_regular_shared):
    """Test basic read_kafka functionality."""
    # Test that the function exists and can be called with basic params
    # This will fail without actual Kafka setup, but tests the API
    with pytest.raises(RuntimeError):
        ray.data.read_kafka(
            topics=["test-topic"], bootstrap_servers="localhost:9092", trigger="once"
        )


def test_read_kafka_trigger_formats(ray_start_regular_shared):
    """Test different trigger formats."""
    # Test that different trigger formats are accepted
    trigger_formats = ["once", "continuous", "30s", "interval:1m"]
    for trigger in trigger_formats:
        with pytest.raises(RuntimeError):
            ray.data.read_kafka(
                topics=["test-topic"],
                bootstrap_servers="localhost:9092",
                trigger=trigger,
            )


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
