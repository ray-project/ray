import logging
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Union

import pyarrow as pa

from ray.data._internal.util import _check_import
from ray.data.datasource import ReadTask
from ray.data.datasource.streaming_datasource import (
    StreamingDatasource,
    StreamingMetrics,
    create_streaming_read_task,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


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


def _create_kafka_reader(
    topic: str,
    partition: int,
    kafka_config: Dict[str, Any],
    start_offset: Optional[int] = None,
    end_offset: Optional[int] = None,
    max_records: Optional[int] = None,
    batch_timeout_ms: int = 1000,
    fetch_max_wait_ms: int = 500,
) -> tuple[callable, callable]:
    """Create a Kafka reader function with encapsulated state.

    This function creates a closure that encapsulates all state needed for
    reading from a Kafka partition, avoiding the use of global variables.

    Args:
        topic: Kafka topic name.
        partition: Partition number.
        kafka_config: Kafka consumer configuration.
        start_offset: Starting offset position.
        end_offset: Ending offset position.
        max_records: Maximum records to read per call.
        batch_timeout_ms: Timeout for batch polling in milliseconds.
        fetch_max_wait_ms: Maximum wait time for batching in milliseconds.

    Returns:
        Tuple of (read_function, get_position_function).

    Examples:
        Creating a Kafka reader:

        .. testcode::

            read_fn, get_pos_fn = _create_kafka_reader(
                topic="my-topic",
                partition=0,
                kafka_config={"bootstrap.servers": "localhost:9092"},
                start_offset=1000,
                max_records=100
            )
    """
    _check_import(module="kafka", package="kafka-python")
    from kafka import KafkaConsumer, TopicPartition
    from kafka.errors import KafkaError

    # State variables encapsulated in closure
    current_offset = start_offset
    consumer = None
    topic_partition = TopicPartition(topic, partition)
    metrics = StreamingMetrics()

    # Initialize max_records with default if not provided
    if max_records is None:
        max_records = 1000

    def read_partition() -> Iterator[Dict[str, Any]]:
        """Read records from Kafka partition, maintaining position state."""
        nonlocal current_offset, consumer

        try:
            # Initialize consumer if needed
            if consumer is None:
                # Optimize Kafka consumer configuration for streaming performance
                optimized_config = kafka_config.copy()

                # Performance optimizations
                optimized_config.update({
                    "enable_auto_commit": False,  # Manual offset management
                    "fetch.min.bytes": 1,  # Fetch even small batches
                    "fetch.max.wait.ms": fetch_max_wait_ms,  # Wait for batching
                    "max.partition.fetch.bytes": 1048576,  # 1MB per partition
                    "receive.buffer.bytes": 32768,  # 32KB receive buffer
                    "max.poll.records": 1000,  # Default batch size
                    "max.poll.interval.ms": 300000,  # 5 min poll interval
                })

                consumer = KafkaConsumer(
                    **optimized_config,
                    value_deserializer=lambda x: x.decode("utf-8") if x else None,
                    key_deserializer=lambda x: x.decode("utf-8") if x else None,
                )

                # Assign specific partition
                consumer.assign([topic_partition])

                # Seek to starting position
                if current_offset is not None:
                    consumer.seek(topic_partition, current_offset)
                else:
                    # Default to latest
                    consumer.seek_to_end(topic_partition)

            # Poll for records with optimized batching
            # Use fetch.max.wait.ms for better batching efficiency
            message_batch = consumer.poll(
                timeout_ms=batch_timeout_ms,
                max_records=max_records
            )

            if topic_partition in message_batch:
                for message in message_batch[topic_partition]:
                    # Check if we've reached the end offset
                    if end_offset is not None and message.offset >= end_offset:
                        logger.info(
                            f"Reached end offset {end_offset} for {topic}:{partition}"
                        )
                        return  # Stop reading

                    # Update current position
                    current_offset = message.offset + 1  # Next offset to read

                    # Convert Kafka message to dict
                    record_dict = {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key,
                        "value": message.value,
                        "timestamp": datetime.fromtimestamp(message.timestamp / 1000.0)
                        if message.timestamp
                        else datetime.now(),
                        "timestamp_type": message.timestamp_type,
                        "headers": dict(message.headers) if message.headers else {},
                    }

                    # Track metrics
                    value_size = (
                        len(message.value.encode("utf-8")) if message.value else 0
                    )
                    key_size = len(message.key.encode("utf-8")) if message.key else 0
                    metrics.record_read(1, value_size + key_size)

                    yield record_dict

        except KafkaError as e:
            logger.error(f"Kafka error reading from {topic}:{partition}: {e}")
            metrics.record_error()
            # Don't yield anything on error, let the retry logic handle it

        except Exception as e:
            logger.error(f"Unexpected error in Kafka reader: {e}")
            metrics.record_error()
            raise
        finally:
            # Note: Don't close consumer here as it will be reused across calls
            pass

    def get_current_position() -> str:
        """Get current offset position."""
        return f"offset:{current_offset if current_offset is not None else 'latest'}"

    return read_partition, get_current_position


@PublicAPI(stability="alpha")
class KafkaDatasource(StreamingDatasource):
    """Kafka datasource for reading from Apache Kafka topics.

    This datasource provides structured streaming capabilities for Apache Kafka,
    supporting real-time data ingestion with configurable triggers and automatic
    offset tracking.

    Examples:
        Basic Kafka topic reading:

        .. testcode::
            :skipif: True

            import ray
            from ray.data._internal.datasource.kafka_datasource import KafkaDatasource

            # Read from Kafka topic with default configuration
            ds = ray.data.read_kafka(
                topics=["my-topic"],
                kafka_config={
                    "bootstrap.servers": "localhost:9092"
                }
            )

        Advanced configuration with custom triggers:

        .. testcode::
            :skipif: True

            import ray
            from ray.data._internal.logical.operators.streaming_data_operator import (
                StreamingTrigger,
            )

            # Read with fixed interval trigger
            trigger = StreamingTrigger.fixed_interval("10s")
            ds = ray.data.read_kafka(
                topics=["events", "metrics"],
                kafka_config={
                    "bootstrap.servers": "kafka1:9092,kafka2:9092",
                    "group.id": "ray-consumer-group",
                    "security.protocol": "SSL"
                },
                trigger=trigger,
                max_records_per_task=500
            )
    """

    def __init__(
        self,
        topics: Union[str, List[str]],
        kafka_config: Dict[str, Any],
        max_records_per_task: int = 1000,
        start_offset: Optional[str] = None,
        end_offset: Optional[str] = None,
        streaming_config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize Kafka datasource.

        Args:
            topics: Kafka topic(s) to read from. Can be string or list of strings.
            kafka_config: Kafka consumer configuration (bootstrap.servers, etc.).
            max_records_per_task: Maximum records per partition per task per batch.
            start_offset: Starting offset for reading.
            end_offset: Ending offset for reading.
            streaming_config: Additional streaming configuration.
        """
        self.topics = topics if isinstance(topics, list) else [topics]
        self.kafka_config = kafka_config

        # Create streaming config
        streaming_config = {
            "topics": self.topics,
            "kafka_config": self.kafka_config,
            "source_identifier": (
                f"kafka://{kafka_config.get('bootstrap.servers', 'unknown')}/"
                f"{','.join(self.topics)}"
            ),
        }

        super().__init__(
            max_records_per_task=max_records_per_task,
            start_position=start_offset,
            end_position=end_offset,
            streaming_config=streaming_config,
        )

    def _validate_config(self) -> None:
        """Validate Kafka configuration.

        Raises:
            ValueError: If required configuration is missing or invalid.
        """
        if not self.topics:
            raise ValueError("topics is required for Kafka datasource")

        if not isinstance(self.kafka_config, dict):
            raise ValueError("kafka_config must be a dictionary")

        # Check for both forms - kafka-python accepts both
        if not (
            self.kafka_config.get("bootstrap_servers")
            or self.kafka_config.get("bootstrap.servers")
        ):
            raise ValueError(
                "bootstrap_servers (or bootstrap.servers) is required in kafka_config"
            )

    def get_name(self) -> str:
        """Return datasource name.

        Returns:
            String representation of the datasource.
        """
        return f"kafka://{','.join(self.topics)}"

    def get_streaming_partitions(self) -> List[Dict[str, Any]]:
        """Get partitions for the Kafka topics.

        Returns:
            List of partition info dictionaries, one per topic partition.
        """
        _check_import("kafka")
        from kafka import KafkaConsumer

        try:
            # Create temporary consumer to discover partitions
            consumer = KafkaConsumer(**self.kafka_config)

            partitions = []
            for topic in self.topics:
                # Get partition metadata for topic
                topic_partitions = consumer.partitions_for_topic(topic)

                if topic_partitions is None:
                    logger.warning(f"Topic {topic} not found or has no partitions")
                    continue

                for partition_id in topic_partitions:
                    partitions.append(
                        {
                            "topic": topic,
                            "partition": partition_id,
                            "partition_id": f"{topic}-{partition_id}",
                            "start_offset": self.start_position,
                            "end_offset": self.end_position,
                        }
                    )

            consumer.close()
            logger.info(
                f"Found {len(partitions)} partitions for Kafka topics {self.topics}"
            )
            return partitions

        except Exception as e:
            logger.error(f"Error listing Kafka partitions: {e}")
            raise RuntimeError(f"Failed to get Kafka partitions: {e}") from e

    def _create_streaming_read_task(self, partition_info: Dict[str, Any]) -> ReadTask:
        """Create a read task for a Kafka partition.

        Args:
            partition_info: Partition information containing topic and partition
                details.

        Returns:
            ReadTask for reading from the partition.
        """
        topic = partition_info["topic"]
        partition = partition_info["partition"]
        partition_id = partition_info["partition_id"]
        start_offset = partition_info.get("start_offset")
        end_offset = partition_info.get("end_offset")

        # Create stateful reader functions
        read_partition_fn, get_position_fn = _create_kafka_reader(
            topic=topic,
            partition=partition,
            kafka_config=self.kafka_config,
            start_offset=_parse_kafka_position(start_offset) if start_offset else None,
            end_offset=_parse_kafka_position(end_offset) if end_offset else None,
            max_records=self.max_records_per_task,
        )

        def get_schema() -> pa.Schema:
            """Return schema for Kafka records."""
            return pa.schema(
                [
                    ("topic", pa.string()),
                    ("partition", pa.int32()),
                    ("offset", pa.int64()),
                    ("key", pa.string()),
                    ("value", pa.string()),
                    ("timestamp", pa.timestamp("us")),
                    ("timestamp_type", pa.int32()),
                    ("headers", pa.string()),  # JSON string representation
                ]
            )

        return create_streaming_read_task(
            partition_id=partition_id,
            streaming_config=self.streaming_config,
            read_source_fn=read_partition_fn,
            get_position_fn=get_position_fn,
            get_schema_fn=get_schema,
            start_position=self.start_position,
            end_position=self.end_position,
            max_records=self.max_records_per_task,
        )

    def get_streaming_schema(self) -> Optional[pa.Schema]:
        """Return the schema for Kafka streaming data.

        Returns:
            PyArrow schema for Kafka records.
        """
        return pa.schema(
            [
                ("topic", pa.string()),
                ("partition", pa.int32()),
                ("offset", pa.int64()),
                ("key", pa.string()),
                ("value", pa.string()),
                ("timestamp", pa.timestamp("us")),
                ("timestamp_type", pa.int32()),
                ("headers", pa.string()),
                ("partition_id", pa.string()),
                ("read_timestamp", pa.timestamp("us")),
                ("current_position", pa.string()),
            ]
        )
