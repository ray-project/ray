import pytest
import pyarrow as pa
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List

from ray.data.block import BlockMetadata  
from ray.data.datasource import ReadTask
from ray.data._internal.datasource.kafka_datasource import (
    KafkaDatasource,
    _parse_kafka_position,
)
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
        assert _parse_kafka_position("-1") == -1
        assert _parse_kafka_position("offset:-1") == -1


@pytest.mark.skipif(
    not pytest.importorskip("kafka", minversion=None),
    reason="kafka-python is not installed"
)
class TestKafkaDatasourceWithKafka:
    """Test KafkaDatasource with kafka-python available."""
    
    def test_datasource_initialization(self):
        """Test Kafka datasource initialization."""
        kafka_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group"
        }
        
        ds = KafkaDatasource(
            topics=["test-topic"],
            kafka_config=kafka_config,
            max_records_per_partition=1000,
            start_offset=100,
            end_offset=200
        )
        
        assert ds.topics == ["test-topic"]
        assert ds.kafka_config == kafka_config
        assert ds.max_records_per_task == 1000
        assert ds.start_position == "offset:100"
        assert ds.end_position == "offset:200"
        assert ds.supports_distributed_reads is True
        assert ds.estimate_inmemory_data_size() is None
    
    def test_datasource_with_multiple_topics(self):
        """Test Kafka datasource with multiple topics."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        
        ds = KafkaDatasource(
            topics=["topic1", "topic2", "topic3"],
            kafka_config=kafka_config
        )
        
        assert ds.topics == ["topic1", "topic2", "topic3"]
    
    def test_datasource_with_string_topic(self):
        """Test Kafka datasource with single string topic."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        
        ds = KafkaDatasource(
            topics="single-topic",
            kafka_config=kafka_config
        )
        
        assert ds.topics == ["single-topic"]
    
    def test_get_name(self):
        """Test datasource name generation."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        
        # Single topic
        ds1 = KafkaDatasource(topics=["test"], kafka_config=kafka_config)
        assert ds1.get_name() == "Kafka(test)"
        
        # Multiple topics
        ds2 = KafkaDatasource(topics=["a", "b", "c"], kafka_config=kafka_config)
        assert ds2.get_name() == "Kafka(a,b,c)"
    
    def test_validation_missing_bootstrap_servers(self):
        """Test validation with missing bootstrap.servers."""
        with pytest.raises(ValueError, match="bootstrap.servers is required"):
            KafkaDatasource(
                topics=["test"],
                kafka_config={"group.id": "test"}  # Missing bootstrap.servers
            )
    
    def test_get_streaming_schema(self):
        """Test schema retrieval."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)
        
        schema = ds.get_streaming_schema()
        assert isinstance(schema, pa.Schema)
        
        expected_columns = {
            'topic', 'partition', 'offset', 'key', 'value', 
            'timestamp', 'headers', 'value_json', 'partition_id',
            'read_timestamp', 'current_position'
        }
        actual_columns = set(schema.names)
        assert actual_columns == expected_columns


class TestKafkaDatasourceWithoutKafka:
    """Test KafkaDatasource behavior when kafka-python is not available."""
    
    def test_datasource_import_error(self):
        """Test that proper error is raised when kafka-python is not available."""
        with patch('ray.data._internal.datasource.kafka_datasource.KAFKA_AVAILABLE', False):
            with pytest.raises(ImportError, match="kafka-python is required"):
                KafkaDatasource(
                    topics=["test"],
                    kafka_config={"bootstrap.servers": "localhost:9092"}
                )


@pytest.mark.skipif(
    not pytest.importorskip("kafka", minversion=None),
    reason="kafka-python is not installed"
)
class TestKafkaPartitionReading:
    """Test Kafka partition reading functionality."""
    
    def test_get_streaming_partitions_mock(self):
        """Test getting streaming partitions with mocked Kafka."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        
        # Mock partition data
        mock_partitions = {0, 1, 2}
        
        with patch('ray.data._internal.datasource.kafka_datasource.KafkaConsumer') as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            mock_consumer.partitions_for_topic.return_value = mock_partitions
            
            ds = KafkaDatasource(
                topics=["test-topic"],
                kafka_config=kafka_config,
                max_records_per_partition=500
            )
            
            partitions = ds.get_streaming_partitions()
            
            # Verify correct number of partitions
            assert len(partitions) == 3
            
            # Verify partition structure
            for i, partition_info in enumerate(partitions):
                assert partition_info["topic"] == "test-topic"
                assert partition_info["partition"] in mock_partitions
                assert partition_info["kafka_config"] == kafka_config
                assert partition_info["max_records"] == 500
            
            # Verify consumer was closed
            mock_consumer.close.assert_called_once()
    
    def test_get_streaming_partitions_multiple_topics(self):
        """Test getting partitions for multiple topics."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        
        with patch('ray.data._internal.datasource.kafka_datasource.KafkaConsumer') as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            
            # Mock different partitions for different topics
            def mock_partitions_for_topic(topic):
                if topic == "topic-a":
                    return {0, 1}
                elif topic == "topic-b":
                    return {0, 1, 2}
                return set()
            
            mock_consumer.partitions_for_topic.side_effect = mock_partitions_for_topic
            
            ds = KafkaDatasource(
                topics=["topic-a", "topic-b"],
                kafka_config=kafka_config
            )
            
            partitions = ds.get_streaming_partitions()
            
            # Should have 2 + 3 = 5 total partitions
            assert len(partitions) == 5
            
            # Check topics are correct
            topic_a_partitions = [p for p in partitions if p["topic"] == "topic-a"]
            topic_b_partitions = [p for p in partitions if p["topic"] == "topic-b"]
            
            assert len(topic_a_partitions) == 2
            assert len(topic_b_partitions) == 3
    
    def test_get_streaming_partitions_no_partitions(self):
        """Test handling when no partitions are found."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        
        with patch('ray.data._internal.datasource.kafka_datasource.KafkaConsumer') as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            mock_consumer.partitions_for_topic.return_value = None  # No partitions
            
            ds = KafkaDatasource(topics=["non-existent-topic"], kafka_config=kafka_config)
            
            partitions = ds.get_streaming_partitions()
            assert len(partitions) == 0
    
    def test_get_streaming_partitions_error_handling(self):
        """Test error handling in partition discovery."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        
        with patch('ray.data._internal.datasource.kafka_datasource.KafkaConsumer') as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            mock_consumer.partitions_for_topic.side_effect = Exception("Connection failed")
            
            ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)
            
            with pytest.raises(Exception, match="Connection failed"):
                ds.get_streaming_partitions()


@pytest.mark.skipif(
    not pytest.importorskip("kafka", minversion=None),
    reason="kafka-python is not installed"
)
class TestKafkaReadTask:
    """Test Kafka read task creation and execution."""
    
    def test_create_read_task(self):
        """Test creating a Kafka read task."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)
        
        partition_info = {
            "topic": "test-topic",
            "partition": 0,
            "kafka_config": kafka_config,
            "start_position": "offset:100",
            "end_position": "offset:200",
            "max_records": 1000,
        }
        
        read_task = ds._create_streaming_read_task(partition_info)
        
        assert isinstance(read_task, ReadTask)
        assert read_task.metadata.num_rows is None
        assert read_task.metadata.size_bytes is None
    
    def test_read_task_execution_mock(self):
        """Test read task execution with mocked Kafka consumer."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)
        
        # Mock Kafka messages
        mock_messages = [
            Mock(
                topic="test-topic",
                partition=0,
                offset=100,
                key="key1",
                value='{"data": "message1"}',
                timestamp=1234567890000,
                headers=[]
            ),
            Mock(
                topic="test-topic", 
                partition=0,
                offset=101,
                key="key2",
                value='{"data": "message2"}',
                timestamp=1234567891000,
                headers=[("header1", b"value1")]
            ),
        ]
        
        with patch('ray.data._internal.datasource.kafka_datasource.KafkaConsumer') as mock_consumer_class, \
             patch('ray.data._internal.datasource.kafka_datasource.TopicPartition') as mock_topic_partition:
            
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            mock_consumer.__iter__.return_value = iter(mock_messages)
            
            partition_info = {
                "topic": "test-topic",
                "partition": 0,
                "kafka_config": kafka_config,
                "start_position": "offset:100",
                "end_position": None,
                "max_records": 10,
            }
            
            read_task = ds._create_streaming_read_task(partition_info)
            
            # Execute the read task
            blocks = list(read_task())
            assert len(blocks) == 1
            
            # Check the resulting block
            block = blocks[0]
            assert isinstance(block, pa.Table)
            assert len(block) == 2
            
            # Verify data content
            records = block.to_pylist()
            
            # Check first record
            record1 = records[0]
            assert record1["topic"] == "test-topic"
            assert record1["partition"] == 0
            assert record1["offset"] == 100
            assert record1["key"] == "key1"
            assert record1["value"] == '{"data": "message1"}'
            assert record1["value_json"] == {"data": "message1"}  # Parsed JSON
            assert "partition_id" in record1
            assert "read_timestamp" in record1
            assert "current_position" in record1
            
            # Check second record
            record2 = records[1]
            assert record2["offset"] == 101
            assert record2["headers"] == {"header1": "value1"}
            
            # Verify consumer was properly configured
            mock_consumer.assign.assert_called_once()
            mock_consumer.close.assert_called_once()
    
    def test_read_task_json_parsing_error(self):
        """Test handling of JSON parsing errors."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)
        
        # Mock message with invalid JSON
        mock_message = Mock(
            topic="test-topic",
            partition=0,
            offset=100,
            key="key1",
            value="invalid json {",  # Invalid JSON
            timestamp=1234567890000,
            headers=[]
        )
        
        with patch('ray.data._internal.datasource.kafka_datasource.KafkaConsumer') as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            mock_consumer.__iter__.return_value = iter([mock_message])
            
            partition_info = {
                "topic": "test-topic",
                "partition": 0,
                "kafka_config": kafka_config,
                "start_position": None,
                "end_position": None,
                "max_records": 10,
            }
            
            read_task = ds._create_streaming_read_task(partition_info)
            
            # Execute the read task
            blocks = list(read_task())
            block = blocks[0]
            records = block.to_pylist()
            
            # Should handle JSON parsing error gracefully
            record = records[0]
            assert record["value"] == "invalid json {"
            assert record["value_json"] is None  # Failed to parse
    
    def test_read_task_with_offset_limits(self):
        """Test read task with start and end offset limits."""
        kafka_config = {"bootstrap.servers": "localhost:9092"}
        ds = KafkaDatasource(topics=["test"], kafka_config=kafka_config)
        
        # Mock messages with different offsets
        mock_messages = [
            Mock(topic="test", partition=0, offset=150, key="key1", value="msg1", timestamp=1000, headers=[]),
            Mock(topic="test", partition=0, offset=160, key="key2", value="msg2", timestamp=2000, headers=[]),
            Mock(topic="test", partition=0, offset=170, key="key3", value="msg3", timestamp=3000, headers=[]),
            Mock(topic="test", partition=0, offset=180, key="key4", value="msg4", timestamp=4000, headers=[]),
        ]
        
        with patch('ray.data._internal.datasource.kafka_datasource.KafkaConsumer') as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            
            # Mock iterator that stops at end_offset
            def mock_iter():
                for msg in mock_messages:
                    if msg.offset >= 170:  # Simulate end_offset=170
                        break
                    yield msg
            
            mock_consumer.__iter__ = mock_iter
            
            partition_info = {
                "topic": "test",
                "partition": 0,
                "kafka_config": kafka_config,
                "start_position": "offset:100",
                "end_position": "offset:170",  # Should stop before offset 170
                "max_records": 10,
            }
            
            read_task = ds._create_streaming_read_task(partition_info)
            
            # Execute the read task
            blocks = list(read_task())
            block = blocks[0]
            records = block.to_pylist()
            
            # Should only get messages before offset 170
            assert len(records) == 2
            assert records[0]["offset"] == 150
            assert records[1]["offset"] == 160
            
            # Verify seek was called with start offset
            mock_consumer.seek.assert_called()


if __name__ == "__main__":
    import sys
    
    sys.exit(pytest.main(["-v", __file__])) 