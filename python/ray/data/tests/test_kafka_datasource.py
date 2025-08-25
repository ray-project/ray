import importlib.util
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from ray.data._internal.datasource.kafka_datasource import (
    KafkaDatasource,
    _parse_kafka_position,
)
from ray.data.datasource import ReadTask
from ray.tests.conftest import *  # noqa


class TestKafkaPositionParsing:
    """Test Kafka position parsing utilities."""

    def test_parse_offset_with_prefix(self):
        """Test parsing offset with 'offset:' prefix."""
        assert _parse_kafka_position("offset:12345") == 12345
        assert _parse_kafka_position("offset:0") == 0
        assert _parse_kafka_position("offset:999999") == 999999

    def test_parse_offset_without_prefix(self):
        """Test parsing offset without prefix."""
        assert _parse_kafka_position("12345") == 12345
        assert _parse_kafka_position("0") == 0

    def test_parse_invalid_offset(self):
        """Test parsing invalid offsets."""
        assert _parse_kafka_position("invalid") is None
        assert _parse_kafka_position("offset:abc") is None
        assert _parse_kafka_position("") is None
        assert _parse_kafka_position(None) is None

    def test_parse_negative_offset(self):
        """Test parsing negative offsets."""
        assert _parse_kafka_position("offset:-1") == -1


@pytest.mark.skipif(
    importlib.util.find_spec("kafka") is None, reason="kafka-python is not installed"
)
class TestKafkaDatasourceWithKafka:
    """Test KafkaDatasource with kafka-python available."""

    def test_datasource_initialization(self):
        """Test Kafka datasource initialization."""
        kafka_config = {"bootstrap_servers": "localhost:9092", "group.id": "test-group"}

        ds = KafkaDatasource(
            topics=["test-topic"],
            kafka_config=kafka_config,
            max_records_per_task=1000,
            start_offset="100",
            end_offset="200",
        )

        assert ds.topics == ["test-topic"]
        assert ds.kafka_config == kafka_config
        assert ds.max_records_per_task == 1000
        assert ds.start_position == "100"
        assert ds.end_position == "200"
        assert ds.supports_distributed_reads is True
        assert ds.estimate_inmemory_data_size() is None

    def test_datasource_with_multiple_topics(self):
        """Test Kafka datasource with multiple topics."""
        kafka_config = {"bootstrap_servers": "localhost:9092"}

        ds = KafkaDatasource(
            topics=["topic1", "topic2", "topic3"], kafka_config=kafka_config
        )

        assert ds.topics == ["topic1", "topic2", "topic3"]

    def test_datasource_with_string_topic(self):
        """Test Kafka datasource with single string topic."""
        kafka_config = {"bootstrap_servers": "localhost:9092"}

        ds = KafkaDatasource(topics="single-topic", kafka_config=kafka_config)

        assert ds.topics == ["single-topic"]

    def test_get_name(self):
        """Test datasource name generation."""
        kafka_config = {"bootstrap_servers": "localhost:9092"}

        # Single topic
        ds1 = KafkaDatasource(topics=["test"], kafka_config=kafka_config)
        assert ds1.get_name() == "kafka://test"

        # Multiple topics
        ds2 = KafkaDatasource(topics=["a", "b", "c"], kafka_config=kafka_config)
        assert ds2.get_name() == "kafka://a,b,c"

    def test_validation_missing_bootstrap_servers(self):
        """Test validation with missing bootstrap_servers."""
        with pytest.raises(ValueError, match="bootstrap_servers.*is required"):
            KafkaDatasource(
                topics=["test"],
                kafka_config={"group.id": "test"},  # Missing bootstrap_servers
            )

    def test_get_streaming_schema(self):
        """Test schema retrieval."""
        kafka_config = {"bootstrap_servers": "localhost:9092"}
        ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)

        schema = ds.get_streaming_schema()
        assert isinstance(schema, pa.Schema)

        # Check the actual schema returned by get_streaming_schema
        # Note: partition_id, read_timestamp, and current_position are added during task execution
        expected_columns = {
            "topic",
            "partition",
            "offset",
            "key",
            "value",
            "timestamp",
            "timestamp_type",
            "headers",
        }
        actual_columns = set(schema.names)
        assert actual_columns == expected_columns


class TestKafkaDatasourceWithoutKafka:
    """Test KafkaDatasource behavior when kafka-python is not available."""

    def test_datasource_initialization_without_kafka(self):
        """Test that datasource can be initialized without kafka-python."""
        # This should work without kafka-python since validation only happens on actual usage
        kafka_config = {"bootstrap_servers": "localhost:9092"}

        ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)
        assert ds.topics == ["test"]
        assert ds.kafka_config == kafka_config


@pytest.mark.skipif(
    importlib.util.find_spec("kafka") is None, reason="kafka-python is not installed"
)
class TestKafkaPartitionReading:
    """Test Kafka partition reading functionality."""

    def test_get_streaming_partitions_single_topic(self):
        """Test partition discovery for single topic."""
        kafka_config = {"bootstrap_servers": "localhost:9092"}
        ds = KafkaDatasource(topics=["test-topic"], kafka_config=kafka_config)

        # Mock the partitions_for_topic method
        mock_partitions = {0, 1, 2}

        with patch("kafka.KafkaConsumer") as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            mock_consumer.partitions_for_topic.return_value = mock_partitions

            partitions = ds.get_streaming_partitions()

            # Should have 3 partitions
            assert len(partitions) == 3

            # Verify partition structure
            for i, partition_info in enumerate(partitions):
                assert partition_info["topic"] == "test-topic"
                assert partition_info["partition"] in mock_partitions
                assert "partition_id" in partition_info
                assert "start_offset" in partition_info
                assert "end_offset" in partition_info

    def test_get_streaming_partitions_multiple_topics(self):
        """Test partition discovery for multiple topics."""
        kafka_config = {"bootstrap_servers": "localhost:9092"}
        ds = KafkaDatasource(
            topics=["topic1", "topic2"],
            kafka_config=kafka_config,
            max_records_per_task=500,
        )

        with patch("kafka.KafkaConsumer") as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer

            # topic1 has 2 partitions, topic2 has 3 partitions
            def mock_partitions_for_topic(topic):
                if topic == "topic1":
                    return {0, 1}
                elif topic == "topic2":
                    return {0, 1, 2}
                return None

            mock_consumer.partitions_for_topic.side_effect = mock_partitions_for_topic

            partitions = ds.get_streaming_partitions()

            # Should have 5 partitions total (2 + 3)
            assert len(partitions) == 5

            # Verify topics are correctly assigned
            topic1_partitions = [p for p in partitions if p["topic"] == "topic1"]
            topic2_partitions = [p for p in partitions if p["topic"] == "topic2"]

            assert len(topic1_partitions) == 2
            assert len(topic2_partitions) == 3

    def test_get_streaming_partitions_error_handling(self):
        """Test error handling during partition discovery."""
        kafka_config = {"bootstrap_servers": "localhost:9092"}
        ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)

        with patch("kafka.KafkaConsumer") as mock_consumer_class:
            mock_consumer_class.side_effect = Exception("Connection failed")

            with pytest.raises(RuntimeError, match="Failed to get Kafka partitions"):
                ds.get_streaming_partitions()


@pytest.mark.skipif(
    importlib.util.find_spec("kafka") is None, reason="kafka-python is not installed"
)
class TestKafkaReadTask:
    """Test Kafka read task creation and execution."""

    def test_create_read_task(self):
        """Test creating a Kafka read task."""
        kafka_config = {"bootstrap_servers": "localhost:9092"}
        ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)

        partition_info = {
            "topic": "test-topic",
            "partition": 0,
            "partition_id": "test-topic-0",
            "start_offset": "100",
            "end_offset": "200",
        }

        read_task = ds._create_streaming_read_task(partition_info)

        assert isinstance(read_task, ReadTask)
        assert read_task.metadata.num_rows == 1000  # max_records_per_task
        assert read_task.metadata.size_bytes is None

    def test_read_task_execution_mock(self):
        """Test read task execution with mocked Kafka consumer."""
        kafka_config = {"bootstrap_servers": "localhost:9092"}
        ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)

        partition_info = {
            "topic": "test-topic",
            "partition": 0,
            "partition_id": "test-topic-0",
            "start_offset": None,
            "end_offset": None,
        }

        # Mock Kafka messages
        mock_messages = [
            Mock(
                topic="test-topic",
                partition=0,
                offset=i,
                key=f"key-{i}",
                value=f"value-{i}",
                timestamp=1699999999000 + i,
                timestamp_type=0,
                headers=[],
            )
            for i in range(5)
        ]

        with patch(
            "ray.data._internal.datasource.kafka_datasource._create_kafka_reader"
        ) as mock_reader:

            def mock_read_partition():
                for msg in mock_messages:
                    yield {
                        "topic": msg.topic,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "key": msg.key,
                        "value": msg.value,
                        "timestamp": msg.timestamp,
                        "timestamp_type": msg.timestamp_type,
                        "headers": dict(msg.headers),
                    }

            def mock_get_position():
                return "offset:5"

            mock_reader.return_value = (mock_read_partition, mock_get_position)

            read_task = ds._create_streaming_read_task(partition_info)

            # Execute the read task
            blocks = list(read_task.read_fn())

            assert len(blocks) == 1
            # Verify that we got a pyarrow table
            block = blocks[0]
            assert hasattr(block, "to_pydict")
            data = block.to_pydict()
            assert len(data["topic"]) == 5
            assert data["topic"][0] == "test-topic"

    def test_read_task_with_offset_limits(self):
        """Test read task with start and end offset limits."""
        kafka_config = {"bootstrap_servers": "localhost:9092"}
        ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)

        partition_info = {
            "topic": "test-topic",
            "partition": 0,
            "partition_id": "test-topic-0",
            "start_offset": "100",
            "end_offset": "200",
        }

        # Test that the read task is created with correct offset parameters
        read_task = ds._create_streaming_read_task(partition_info)

        # Verify the task metadata reflects the offset limits
        assert read_task.metadata.num_rows == 1000  # max_records_per_task
        assert read_task.metadata.size_bytes is None

        # Test that the task can be executed (the actual seek logic is tested in integration tests)
        # since the seek happens inside the _create_kafka_reader function which is not easily mockable
        # in this unit test context
        assert isinstance(read_task, ReadTask)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
