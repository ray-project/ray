import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Union

import pyarrow as pa

from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource import ReadTask
from ray.data.datasource.streaming_datasource import (
    StreamingDatasource,
    StreamingMetrics,
    create_streaming_read_task,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

# Global state for tracking current positions across partitions
_kafka_current_positions = {}


def _parse_kafka_position(position: str) -> Optional[int]:
    """Parse Kafka offset from position string.
    
    Args:
        position: Position string in format "offset:123" or just "123".
    
    Returns:
        Parsed offset as integer, or None if invalid.
    """
    if position:
        try:
            if position.startswith("offset:"):
                return int(position.split(":", 1)[1])
            else:
                return int(position)
        except ValueError:
            logger.warning(f"Invalid Kafka offset: {position}")
    return None


def _read_kafka_partition(
    topic: str,
    partition: int,
    kafka_config: Dict[str, Any],
    max_records: int = 1000,
) -> Iterator[Dict[str, Any]]:
    """Read records from a Kafka partition.
    
    Args:
        topic: Kafka topic name.
        partition: Partition number.
        kafka_config: Kafka consumer configuration.
        max_records: Maximum records to read per call.
    
    Yields:
        Dictionary records from Kafka messages.
    """
    _check_import("kafka_datasource", module="kafka", package="kafka-python")
    
    from kafka import KafkaConsumer, TopicPartition
    from kafka.errors import KafkaError
    
    consumer = None
    try:
        consumer = KafkaConsumer(**kafka_config)
        
        # Create topic partition and assign to consumer
        topic_partition = TopicPartition(topic, partition)
        consumer.assign([topic_partition])
        
        # Track position
        position_key = f"{topic}-{partition}"
        
        # Set up metrics
        metrics = StreamingMetrics()
        records_read = 0
        
        for message in consumer:
            if records_read >= max_records:
                break
                
            try:
                # Convert message to dictionary
                record = {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "key": message.key.decode("utf-8") if message.key else None,
                    "value": message.value.decode("utf-8") if message.value else None,
                    "timestamp": datetime.fromtimestamp(message.timestamp / 1000.0) if message.timestamp else None,
                    "headers": dict(message.headers or []),
                }
                
                # Try to parse JSON value
                try:
                    if record["value"]:
                        record["value_json"] = json.loads(record["value"])
                    else:
                        record["value_json"] = None
                except (json.JSONDecodeError, TypeError):
                    record["value_json"] = None
                
                # Update position tracking
                _kafka_current_positions[position_key] = message.offset
                
                # Record metrics
                message_size = len(message.value) if message.value else 0
                metrics.record_read(1, message_size)
                
                records_read += 1
                yield record
                
            except Exception as e:
                logger.warning(f"Error processing Kafka message: {e}")
                metrics.record_error()
                continue
                
    except KafkaError as e:
        logger.error(f"Error reading from Kafka topic {topic}, partition {partition}: {e}")
        raise RuntimeError(f"Kafka read error: {e}") from e
    finally:
        if consumer:
            consumer.close()


def _get_kafka_position(topic: str, partition: int) -> str:
    """Get current position for a Kafka partition.
    
    Args:
        topic: Kafka topic name.
        partition: Partition number.
    
    Returns:
        Current position string in format "offset:123".
    """
    position_key = f"{topic}-{partition}"
    offset = _kafka_current_positions.get(position_key, 0)
    return f"offset:{offset}"


@PublicAPI(stability="alpha")
class KafkaDatasource(StreamingDatasource):
    """Kafka datasource for reading from Apache Kafka topics.
    
    This datasource supports reading from Kafka topics in both batch and streaming
    modes. It handles multiple topics and partitions automatically.
    
    Examples:
        Basic usage:
        
        .. testcode::
            :skipif: True
            
            from ray.data._internal.datasource.kafka_datasource import KafkaDatasource
            
            # Create Kafka datasource
            datasource = KafkaDatasource(
                topics=["my-topic"],
                kafka_config={
                    "bootstrap.servers": "localhost:9092",
                    "group.id": "ray-consumer"
                }
            )
            
        With authentication:
        
        .. testcode::
            :skipif: True
            
            from ray.data._internal.datasource.kafka_datasource import KafkaDatasource
            
            # Create datasource with SASL authentication
            datasource = KafkaDatasource(
                topics=["secure-topic"],
                kafka_config={
                    "bootstrap.servers": "kafka-cluster:9093",
                    "group.id": "ray-consumer",
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "PLAIN",
                    "sasl.username": "username",
                    "sasl.password": "password"
                }
            )
            
        With specific offsets:
        
        .. testcode::
            :skipif: True
            
            from ray.data._internal.datasource.kafka_datasource import KafkaDatasource
            
            # Read from specific offset range
            datasource = KafkaDatasource(
                topics=["events"],
                kafka_config={"bootstrap.servers": "localhost:9092"},
                start_offset="offset:1000",
                end_offset="offset:2000"
            )
    """
    
    def __init__(
        self,
        topics: Union[str, List[str]],
        kafka_config: Dict[str, Any],
        max_records_per_task: int = 1000,
        start_offset: Optional[str] = None,
        end_offset: Optional[str] = None,
        **kwargs
    ):
        """Initialize Kafka datasource.
        
        Args:
            topics: Kafka topic(s) to read from.
            kafka_config: Kafka consumer configuration dictionary.
            max_records_per_task: Maximum records per task per batch.
            start_offset: Starting offset position.
            end_offset: Ending offset position.
            **kwargs: Additional arguments passed to StreamingDatasource.
        """
        # Ensure topics is a list
        if isinstance(topics, str):
            topics = [topics]
        self.topics = topics
        self.kafka_config = kafka_config
        
        # Create streaming config
        streaming_config = {
            "topics": self.topics,
            "kafka_config": self.kafka_config,
            "source_identifier": f"kafka://{kafka_config.get('bootstrap.servers', 'unknown')}/{','.join(topics if isinstance(topics, list) else [topics])}",
        }
        
        super().__init__(
            max_records_per_task=max_records_per_task,
            start_position=start_offset,
            end_position=end_offset,
            streaming_config=streaming_config,
            **kwargs
        )
    
    def _validate_config(self) -> None:
        """Validate Kafka configuration.
        
        Raises:
            ValueError: If configuration is invalid.
            ImportError: If kafka-python is not available.
        """
        _check_import(self, module="kafka", package="kafka-python")
        
        if not self.topics:
            raise ValueError("topics must be provided and non-empty")
            
        if not self.kafka_config:
            raise ValueError("kafka_config must be provided")
            
        if "bootstrap.servers" not in self.kafka_config:
            raise ValueError("bootstrap.servers is required in kafka_config")
    
    def get_name(self) -> str:
        """Get datasource name.
        
        Returns:
            String identifier for this datasource.
        """
        return f"kafka({','.join(self.topics)})"
    
    def get_streaming_partitions(self) -> List[Dict[str, Any]]:
        """Get Kafka partitions to read from.
        
        Returns:
            List of partition metadata dictionaries.
        """
        _check_import(self, module="kafka", package="kafka-python")
        
        from kafka import KafkaConsumer
        
        partitions = []
        consumer = None
        
        try:
            consumer = KafkaConsumer(**self.kafka_config)
            
            # Discover partitions for each topic
            for topic in self.topics:
                topic_partitions = consumer.partitions_for_topic(topic)
                if topic_partitions:
                    for partition_id in topic_partitions:
                        partition_info = {
                            "partition_id": f"{topic}-{partition_id}",
                            "topic": topic,
                            "partition": partition_id,
                            "start_offset": _parse_kafka_position(self.start_position) if self.start_position else None,
                            "end_offset": _parse_kafka_position(self.end_position) if self.end_position else None,
                        }
                        partitions.append(partition_info)
                        
        finally:
            if consumer:
                consumer.close()
        
        return partitions
    
    def _create_streaming_read_task(self, partition_info: Dict[str, Any]) -> ReadTask:
        """Create read task for a Kafka partition.
        
        Args:
            partition_info: Partition metadata from get_streaming_partitions.
            
        Returns:
            ReadTask for this partition.
        """
        topic = partition_info["topic"]
        partition = partition_info["partition"]
        partition_id = partition_info["partition_id"]
        
        def read_kafka_fn():
            """Read from Kafka partition."""
            return _read_kafka_partition(
                topic=topic,
                partition=partition,
                kafka_config=self.kafka_config,
                max_records=self.max_records_per_task,
            )
        
        def get_position_fn():
            """Get current Kafka position."""
            return _get_kafka_position(topic, partition)
        
        def get_schema_fn():
            """Get Kafka message schema."""
            return pa.schema([
                ("topic", pa.string()),
                ("partition", pa.int32()),
                ("offset", pa.int64()),
                ("key", pa.string()),
                ("value", pa.string()),
                ("timestamp", pa.timestamp("ms")),
                ("headers", pa.map_(pa.string(), pa.string())),
                ("value_json", pa.string()),  # JSON will be stored as string
            ])
        
        return create_streaming_read_task(
            partition_id=partition_id,
            streaming_config=self.streaming_config,
            read_source_fn=read_kafka_fn,
            get_position_fn=get_position_fn,
            get_schema_fn=get_schema_fn,
            start_position=self.start_position,
            end_position=self.end_position,
            max_records=self.max_records_per_task,
        )
    
    def get_streaming_schema(self) -> Optional[pa.Schema]:
        """Get schema for Kafka messages.
        
        Returns:
            PyArrow schema for Kafka message structure.
        """
        return pa.schema([
            ("topic", pa.string()),
            ("partition", pa.int32()),
            ("offset", pa.int64()),
            ("key", pa.string()),
            ("value", pa.string()),
            ("timestamp", pa.timestamp("ms")),
            ("headers", pa.map_(pa.string(), pa.string())),
            ("value_json", pa.string()),
        ]) 