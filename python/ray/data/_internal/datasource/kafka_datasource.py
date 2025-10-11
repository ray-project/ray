"""Kafka datasource for unbound data streams.

This module provides a Kafka datasource implementation for Ray Data that works
with the UnboundedDataOperator.

Requires: kafka-python or confluent-kafka
"""

import logging
from typing import Any, Dict, Iterator, List, Optional, Union

import pyarrow as pa

from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.unbound_datasource import (
    UnboundDatasource,
    create_unbound_read_task,
)

logger = logging.getLogger(__name__)


def _check_kafka_available():
    """Check if kafka-python is available."""
    try:
        import kafka  # noqa: F401

        return True
    except ImportError:
        raise ImportError(
            "kafka-python is required for Kafka datasource. "
            "Install with: pip install kafka-python"
        )


class KafkaDatasource(UnboundDatasource):
    """Kafka datasource for reading from Kafka topics."""

    def __init__(
        self,
        topics: Union[str, List[str]],
        kafka_config: Dict[str, Any],
        max_records_per_task: int = 1000,
        start_offset: Optional[str] = None,
        end_offset: Optional[str] = None,
    ):
        """Initialize Kafka datasource.

        Args:
            topics: Kafka topic name(s) to read from
            kafka_config: Kafka configuration dictionary with keys:
                - bootstrap_servers (required): Kafka broker addresses
                - group_id: Consumer group ID
                - auto_offset_reset: 'earliest' or 'latest'
                - enable_auto_commit: Whether to auto-commit offsets
                - session_timeout_ms: Session timeout
                - max_poll_records: Max records per poll
                - security_protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
                - sasl_mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI
                - ssl_* parameters for SSL configuration
            max_records_per_task: Maximum records per task per batch
            start_offset: Starting offset ('earliest', 'latest', or numeric)
            end_offset: Ending offset (numeric, or None for unbounded)

        Raises:
            ValueError: If required configuration is missing
            ImportError: If kafka-python is not installed
        """
        super().__init__("kafka")
        _check_kafka_available()

        # Validate required configuration
        if not kafka_config.get("bootstrap_servers"):
            raise ValueError("bootstrap_servers is required in kafka_config")

        if not topics:
            raise ValueError("topics cannot be empty")

        if max_records_per_task <= 0:
            raise ValueError("max_records_per_task must be positive")

        self.topics = topics if isinstance(topics, list) else [topics]
        self.kafka_config = kafka_config
        self.max_records_per_task = max_records_per_task
        self.start_offset = start_offset or "latest"
        self.end_offset = end_offset

    def _get_read_tasks_for_partition(
        self,
        partition_info: Dict[str, Any],
        parallelism: int,
    ) -> List[ReadTask]:
        """Create read tasks for Kafka topics.

        Args:
            partition_info: Partition information (not used for Kafka, we use topics)
            parallelism: Number of parallel read tasks to create

        Returns:
            List of ReadTask objects for Kafka topics
        """
        tasks = []

        # Create one task per topic, up to parallelism limit
        topics_to_process = (
            self.topics[:parallelism] if parallelism > 0 else self.topics
        )

        # Store config for use in read functions (avoid serialization issues)
        kafka_config = self.kafka_config
        max_records_per_task = self.max_records_per_task
        start_offset = self.start_offset
        end_offset = self.end_offset

        for topic in topics_to_process:

            def create_kafka_read_fn(
                topic_name: str = topic,
                kafka_config: Dict[str, Any] = kafka_config,
                max_records_per_task: int = max_records_per_task,
                start_offset: str = start_offset,
                end_offset: Optional[str] = end_offset,
            ):
                """Create a Kafka read function with captured variables.

                This factory function captures configuration variables as default arguments
                to avoid serialization issues when the read function is executed remotely
                by Ray. Using default arguments ensures all needed config is available
                in the remote task without requiring 'self' to be serialized.
                """

                def kafka_read_fn() -> Iterator[pa.Table]:
                    """Read function for Kafka topic using kafka-python.

                    This function runs remotely in a Ray task. It creates a KafkaConsumer,
                    reads messages from the assigned topic, and yields PyArrow tables
                    incrementally for efficient streaming processing.
                    """
                    from kafka import KafkaConsumer, TopicPartition
                    from kafka.structs import OffsetAndMetadata
                    import json

                    # Build consumer configuration from provided settings
                    # We disable auto-commit by default to give Ray Data control over offset management
                    consumer_config = {
                        "bootstrap_servers": kafka_config["bootstrap_servers"],
                        "auto_offset_reset": kafka_config.get(
                            "auto_offset_reset", start_offset
                        ),
                        "enable_auto_commit": kafka_config.get(
                            "enable_auto_commit", False
                        ),
                        "group_id": kafka_config.get("group_id"),
                        "session_timeout_ms": kafka_config.get(
                            "session_timeout_ms", 30000
                        ),
                        # Limit poll records to support incremental yielding
                        "max_poll_records": min(
                            max_records_per_task,
                            kafka_config.get("max_poll_records", 500),
                        ),
                    }

                    # Add security configuration if present
                    # Supports PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
                    if "security_protocol" in kafka_config:
                        consumer_config["security_protocol"] = kafka_config[
                            "security_protocol"
                        ]

                    # Add SASL authentication if configured
                    # Supports PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI mechanisms
                    if "sasl_mechanism" in kafka_config:
                        consumer_config["sasl_mechanism"] = kafka_config[
                            "sasl_mechanism"
                        ]
                    if "sasl_plain_username" in kafka_config:
                        consumer_config["sasl_plain_username"] = kafka_config[
                            "sasl_plain_username"
                        ]
                    if "sasl_plain_password" in kafka_config:
                        consumer_config["sasl_plain_password"] = kafka_config[
                            "sasl_plain_password"
                        ]

                    # Add SSL/TLS configuration if present
                    # These allow secure connections to Kafka brokers
                    ssl_keys = [
                        "ssl_cafile",
                        "ssl_certfile",
                        "ssl_keyfile",
                        "ssl_check_hostname",
                        "ssl_ciphers",
                    ]
                    for key in ssl_keys:
                        if key in kafka_config:
                            consumer_config[key] = kafka_config[key]

                    # Value deserializer - automatically handle different data formats
                    # Try JSON first (most common), then fall back to plain string
                    def deserialize_value(value):
                        if value is None:
                            return None
                        try:
                            # Attempt to parse as JSON (handles objects, arrays, etc.)
                            return json.loads(value.decode("utf-8"))
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            # Not JSON, return as plain string with error replacement
                            return value.decode("utf-8", errors="replace")

                    consumer_config["value_deserializer"] = deserialize_value
                    # Key deserializer - simple UTF-8 decode or None
                    consumer_config["key_deserializer"] = (
                        lambda k: k.decode("utf-8") if k else None
                    )

                    # Create the Kafka consumer with all configuration
                    consumer = KafkaConsumer(**consumer_config)

                    try:
                        # Query Kafka to get all partitions for this topic
                        # This returns a set of partition IDs (integers)
                        partitions = consumer.partitions_for_topic(topic_name)
                        if not partitions:
                            # Topic doesn't exist or no partitions available - exit early
                            return

                        # Use manual assignment instead of subscribe() for better control
                        # This allows us to explicitly manage offsets and seeking
                        # which is important for bounded reads (start_offset -> end_offset)
                        topic_partitions = [
                            TopicPartition(topic_name, p) for p in partitions
                        ]
                        consumer.assign(topic_partitions)

                        # Seek to the requested starting position
                        # Supports: numeric offsets, 'earliest', 'latest'
                        if start_offset and start_offset.isdigit():
                            # Numeric offset - seek all partitions to this absolute offset
                            for tp in topic_partitions:
                                consumer.seek(tp, int(start_offset))
                        elif start_offset == "earliest":
                            # Start from the beginning of all partitions
                            consumer.seek_to_beginning(*topic_partitions)
                        elif start_offset == "latest":
                            # Start from the end (only read new messages)
                            consumer.seek_to_end(*topic_partitions)

                        records = []
                        records_read = 0
                        # Parse end offset if specified (for bounded reads)
                        end_offset_int = (
                            int(end_offset)
                            if end_offset and end_offset.isdigit()
                            else None
                        )

                        # For streaming execution: yield blocks incrementally (in batches of 1000)
                        # rather than accumulating all data in memory. This allows Ray Data's
                        # streaming execution engine to start processing while we're still reading,
                        # improving memory efficiency and reducing latency.
                        batch_size = min(max_records_per_task, 1000)  # Yield in chunks

                        # Main polling loop - read messages until we hit max_records_per_task
                        while records_read < max_records_per_task:
                            # Poll for a batch of messages from Kafka
                            # timeout_ms: how long to wait if no messages available
                            # max_records: limit batch size to support incremental yielding
                            msg_batch = consumer.poll(
                                timeout_ms=kafka_config.get("poll_timeout_ms", 1000),
                                max_records=min(
                                    batch_size, max_records_per_task - records_read
                                ),
                            )

                            if not msg_batch:
                                # No more messages available right now
                                # Yield any accumulated records and exit the loop
                                if records:
                                    table = pa.Table.from_pylist(records)
                                    yield table
                                break

                            # poll() returns dict of {TopicPartition: [messages]}
                            # Process all messages from all partitions in this batch
                            for topic_partition, messages in msg_batch.items():
                                for msg in messages:
                                    # Check if we've reached the end offset (for bounded reads)
                                    if (
                                        end_offset_int is not None
                                        and msg.offset >= end_offset_int
                                    ):
                                        # Yield final batch and stop reading
                                        if records:
                                            table = pa.Table.from_pylist(records)
                                            yield table
                                        return

                                    # Extract all message metadata into a flat record
                                    # This includes offset, key, value, headers, timestamps
                                    records.append(
                                        {
                                            "offset": msg.offset,  # Unique offset within partition
                                            "key": msg.key,  # Message key (already deserialized)
                                            "value": msg.value,  # Message value (already deserialized)
                                            "topic": msg.topic,  # Topic name
                                            "partition": msg.partition,  # Partition ID
                                            "timestamp": msg.timestamp,  # Message timestamp (ms since epoch)
                                            "timestamp_type": msg.timestamp_type,  # 0=CreateTime, 1=LogAppendTime
                                            "headers": dict(msg.headers)
                                            if msg.headers
                                            else {},  # Message headers
                                        }
                                    )
                                    records_read += 1

                                    # Yield incrementally when we hit batch size
                                    # This enables Ray Data to start processing before we finish reading
                                    if len(records) >= batch_size:
                                        table = pa.Table.from_pylist(records)
                                        yield table
                                        records = []  # Clear for next batch

                                    # Check if we've hit our total limit
                                    if records_read >= max_records_per_task:
                                        # Yield final batch and exit
                                        if records:
                                            table = pa.Table.from_pylist(records)
                                            yield table
                                        return

                    finally:
                        # Always close the consumer to release connections
                        # This is critical for proper resource cleanup
                        consumer.close()

                return kafka_read_fn

            # Create metadata for this task
            metadata = BlockMetadata(
                num_rows=self.max_records_per_task,
                size_bytes=None,
                input_files=[f"kafka://{topic}"],
                exec_stats=None,
            )

            # Create schema - flexible to handle JSON values or strings
            schema = pa.schema(
                [
                    ("offset", pa.int64()),
                    ("key", pa.string()),
                    ("value", pa.string()),  # Can be JSON string or plain string
                    ("topic", pa.string()),
                    ("partition", pa.int32()),
                    ("timestamp", pa.int64()),  # Kafka timestamp in milliseconds
                    ("timestamp_type", pa.int32()),  # 0=CreateTime, 1=LogAppendTime
                    ("headers", pa.map_(pa.string(), pa.string())),  # Message headers
                ]
            )

            # Create read task
            task = create_unbound_read_task(
                read_fn=create_kafka_read_fn(topic),
                metadata=metadata,
                schema=schema,
            )
            tasks.append(task)

        return tasks

    def get_name(self) -> str:
        """Get name of this datasource."""
        return "kafka_unbound_datasource"

    def get_unbound_schema(self, kafka_config: Dict[str, Any]) -> Optional["pa.Schema"]:
        """Get schema for Kafka messages.

        Args:
            kafka_config: Kafka configuration

        Returns:
            PyArrow schema for Kafka messages
        """
        # Standard Kafka message schema
        return pa.schema(
            [
                ("offset", pa.int64()),
                ("key", pa.string()),
                ("value", pa.string()),  # Can be JSON string or plain string
                ("topic", pa.string()),
                ("partition", pa.int32()),
                ("timestamp", pa.int64()),  # Kafka timestamp in milliseconds
                ("timestamp_type", pa.int32()),  # 0=CreateTime, 1=LogAppendTime
                ("headers", pa.map_(pa.string(), pa.string())),  # Message headers
            ]
        )

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory data size for Kafka streams.

        Returns:
            None for unbounded streams
        """
        return None
