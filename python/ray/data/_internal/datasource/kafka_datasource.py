"""Kafka datasource for bounded data reads.

This module provides a Kafka datasource implementation for Ray Data that supports
bounded reads with timestamp-based or offset-based range queries.

Requires:
    - kafka-python: https://kafka-python.readthedocs.io/
"""

import logging
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

import pyarrow as pa

if TYPE_CHECKING:
    from kafka import KafkaConsumer, TopicPartition

from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data._internal.util import _check_import, call_with_retry
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)


def _normalize_timestamp_to_ms(timestamp: Union[int, datetime]) -> int:
    """Convert timestamp to milliseconds since epoch.

    Args:
        timestamp: Either an int (milliseconds or seconds since epoch) or datetime object.

    Returns:
        Milliseconds since epoch as int.

    Raises:
        ValueError: If timestamp is invalid or unsupported type.
    """
    if isinstance(timestamp, datetime):
        # Convert datetime to milliseconds since epoch
        return int(timestamp.timestamp() * 1000)
    elif isinstance(timestamp, int):
        # If timestamp is less than year 2000 in milliseconds, assume it's in seconds
        # Year 2000 in milliseconds: 946684800000
        if timestamp < 946684800000:
            return timestamp * 1000
        return timestamp
    else:
        raise ValueError(f"Unsupported timestamp type: {type(timestamp)}")


def _convert_timestamp_to_offset(
    consumer: "KafkaConsumer",
    topic_partition: "TopicPartition",
    timestamp_ms: int,
) -> int:
    """Convert a timestamp to a Kafka offset.

    Args:
        consumer: KafkaConsumer instance with partitions already assigned.
        topic_partition: TopicPartition instance.
        timestamp_ms: Timestamp in milliseconds since epoch.

    Returns:
        Offset for the given timestamp.

    Raises:
        ValueError: If no offset found for the timestamp (timestamp out of range).
    """
    # Query offsets for the given timestamp
    offsets = consumer.offsets_for_times({topic_partition: timestamp_ms})
    if offsets[topic_partition] is None:
        raise ValueError(
            f"No offset found for timestamp {timestamp_ms} in topic {topic_partition.topic}, "
            f"partition {topic_partition.partition}. Timestamp may be before first message "
            f"or after last message."
        )
    return offsets[topic_partition].offset


def _build_consumer_config_for_discovery(
    bootstrap_servers: List[str], authentication: Dict[str, Any]
) -> Dict[str, Any]:
    """Build minimal consumer config for partition discovery.

    Args:
        bootstrap_servers: List of Kafka broker addresses.
        authentication: Authentication configuration dict.

    Returns:
        Consumer configuration dict for discovery.
    """
    config = {
        "bootstrap_servers": bootstrap_servers,
        "enable_auto_commit": False,
        "auto_offset_reset": "latest",
        "consumer_timeout_ms": 1000,  # Short timeout for discovery
    }

    if not authentication:
        return config

    # Add essential auth config for discovery
    for key in ["security_protocol", "sasl_mechanism"]:
        if key in authentication:
            config[key] = authentication[key]

    if "sasl_username" in authentication:
        config["sasl_plain_username"] = authentication["sasl_username"]
    if "sasl_password" in authentication:
        config["sasl_plain_password"] = authentication["sasl_password"]

    # Add SSL config if present
    ssl_key_mapping = {
        "ssl_ca_location": "ssl_cafile",
        "ssl_certificate_location": "ssl_certfile",
        "ssl_key_location": "ssl_keyfile",
    }
    for auth_key, config_key in ssl_key_mapping.items():
        if auth_key in authentication:
            config[config_key] = authentication[auth_key]
        elif config_key in authentication:
            config[config_key] = authentication[config_key]

    return config


def _build_consumer_config_for_read(
    bootstrap_servers: List[str],
    authentication: Dict[str, Any],
    topic_name: str,
    partition_id: int,
) -> Dict[str, Any]:
    """Build full consumer config for reading messages.

    Args:
        bootstrap_servers: List of Kafka broker addresses.
        authentication: Authentication configuration dict.
        topic_name: Topic name for unique group_id.
        partition_id: Partition ID for unique group_id.

    Returns:
        Consumer configuration dict for reading.
    """
    config = {
        "bootstrap_servers": bootstrap_servers,
        "enable_auto_commit": False,
        "auto_offset_reset": "latest",  # Default, will be overridden by seek
        "group_id": f"ray-data-kafka-{topic_name}-{partition_id}-{uuid.uuid4().hex[:8]}",
    }

    if not authentication:
        return config

    # Direct mappings (auth key -> config key)
    direct_mappings = {
        "security_protocol": "security_protocol",
        "sasl_mechanism": "sasl_mechanism",
        "sasl_kerberos_service_name": "sasl_kerberos_service_name",
        "sasl_kerberos_domain_name": "sasl_kerberos_domain_name",
        "sasl_oauth_token_provider": "sasl_oauth_token_provider",
    }
    for auth_key, config_key in direct_mappings.items():
        if auth_key in authentication:
            config[config_key] = authentication[auth_key]

    # Special mappings (auth key -> different config key)
    if "sasl_username" in authentication:
        config["sasl_plain_username"] = authentication["sasl_username"]
    if "sasl_password" in authentication:
        config["sasl_plain_password"] = authentication["sasl_password"]

    # Add SSL/TLS configuration if present
    ssl_key_mapping = {
        "ssl_ca_location": "ssl_cafile",
        "ssl_certificate_location": "ssl_certfile",
        "ssl_key_location": "ssl_keyfile",
    }
    for auth_key, config_key in ssl_key_mapping.items():
        if auth_key in authentication:
            config[config_key] = authentication[auth_key]
        elif config_key in authentication:
            config[config_key] = authentication[config_key]

    # Add other SSL parameters directly
    for key in ["ssl_check_hostname", "ssl_ciphers", "ssl_password", "ssl_crlfile"]:
        if key in authentication:
            config[key] = authentication[key]

    # Add deserializers
    def deserialize_value(value):
        if value is None:
            return None
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as e:
            raise ValueError(f"Failed to decode message value as UTF-8: {e}") from e

    def deserialize_key(key):
        if key is None:
            return None
        try:
            return key.decode("utf-8")
        except UnicodeDecodeError as e:
            raise ValueError(f"Failed to decode message key as UTF-8: {e}") from e

    config["value_deserializer"] = deserialize_value
    config["key_deserializer"] = deserialize_key

    return config


def _resolve_offset(
    consumer: "KafkaConsumer",
    topic_partition: "TopicPartition",
    offset_value: Union[int, str, datetime],
    is_end: bool = False,
) -> int:
    """Convert offset value to offset for a partition.

    Args:
        consumer: KafkaConsumer instance with partition assigned.
        topic_partition: TopicPartition instance.
        offset_value: Offset value (int, str, or datetime).
        is_end: Whether this is an end_offset (for validation).

    Returns:
        Kafka offset as int.

    Raises:
        ValueError: If offset value is invalid or cannot be resolved.
    """
    if isinstance(offset_value, datetime):
        timestamp_ms = _normalize_timestamp_to_ms(offset_value)
        return _convert_timestamp_to_offset(consumer, topic_partition, timestamp_ms)

    if isinstance(offset_value, int):
        # If it's a reasonable offset (< OFFSET_TIMESTAMP_THRESHOLD), treat as offset
        # Otherwise treat as timestamp in milliseconds
        if offset_value < KafkaDatasource.OFFSET_TIMESTAMP_THRESHOLD:
            return offset_value
        # Likely a timestamp in milliseconds - convert
        return _convert_timestamp_to_offset(consumer, topic_partition, offset_value)

    if isinstance(offset_value, str):
        if offset_value == "earliest":
            consumer.seek_to_beginning(topic_partition)
            return consumer.position(topic_partition)
        if offset_value == "latest":
            if is_end:
                raise ValueError(
                    "end_offset cannot be 'latest'. Use a specific offset or timestamp."
                )
            consumer.seek_to_end(topic_partition)
            return consumer.position(topic_partition)
        if offset_value.isdigit():
            return int(offset_value)
        raise ValueError(
            f"Invalid {'end' if is_end else 'start'}_offset string: {offset_value}"
        )

    raise ValueError(f"Unsupported offset type: {type(offset_value)}")


def _convert_headers_to_dict(headers) -> Dict[str, str]:
    """Convert Kafka message headers to dictionary.

    Args:
        headers: Kafka message headers (list of tuples).

    Returns:
        Dictionary with string keys and values.

    Raises:
        ValueError: If header cannot be decoded as UTF-8.
    """
    if not headers:
        return {}

    headers_dict = {}
    for key, value in headers:
        try:
            key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            value_str = (
                value.decode("utf-8") if isinstance(value, bytes) else str(value)
            )
        except UnicodeDecodeError as e:
            raise ValueError(f"Failed to decode header as UTF-8: {e}") from e
        headers_dict[key_str] = value_str

    return headers_dict


class KafkaDatasource(Datasource):
    """Kafka datasource for reading from Kafka topics with bounded reads."""

    MIN_ROWS_PER_READ_TASK = 50
    # Threshold to distinguish between offset and timestamp in milliseconds
    # Offsets are typically much smaller than timestamps (year 2000 in ms = 946684800000)
    # Using 1e15 (1 quadrillion) as threshold - offsets will never reach this value
    OFFSET_TIMESTAMP_THRESHOLD = 1e15
    # Batch size for incremental block yielding
    BATCH_SIZE_FOR_YIELD = 1000

    def __init__(
        self,
        topics: Union[str, List[str]],
        bootstrap_servers: Union[str, List[str]],
        start_offset: Optional[Union[int, str, datetime]] = None,
        end_offset: Optional[Union[int, str, datetime]] = None,
        authentication: Optional[Dict[str, Any]] = None,
        max_records_per_task: int = 1000,
        poll_timeout_ms: int = 30000,
    ):
        """Initialize Kafka datasource.

        Args:
            topics: Kafka topic name(s) to read from.
            bootstrap_servers: Kafka broker addresses (string or list of strings).
            start_offset: Starting position. Can be:
                - int: Offset number or timestamp in milliseconds
                - str: "earliest", "latest", or offset number as string
                - datetime: Timestamp to convert to offset
            end_offset: Ending position. Can be:
                - int: Offset number or timestamp in milliseconds
                - str: Offset number as string
                - datetime: Timestamp to convert to offset
            authentication: Authentication configuration dict with keys:
                - security_protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
                - sasl_mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI
                - sasl_username: Username for SASL authentication
                - sasl_password: Password for SASL authentication
                - ssl_* parameters for SSL configuration
            max_records_per_task: Maximum records per task per batch.
            poll_timeout_ms: Timeout in milliseconds to wait for messages (default 30000ms/30s).

        Raises:
            ValueError: If required configuration is missing.
            ImportError: If kafka-python is not installed.
        """
        _check_import(self, module="kafka", package="kafka-python")

        if not topics:
            raise ValueError("topics cannot be empty")

        if not bootstrap_servers:
            raise ValueError("bootstrap_servers cannot be empty")

        if max_records_per_task <= 0:
            raise ValueError("max_records_per_task must be positive")

        if poll_timeout_ms <= 0:
            raise ValueError("poll_timeout_ms must be positive")

        # Validate bootstrap_servers format
        if isinstance(bootstrap_servers, str):
            if not bootstrap_servers or ":" not in bootstrap_servers:
                raise ValueError(
                    f"Invalid bootstrap_servers format: {bootstrap_servers}. "
                    "Expected 'host:port' or list of 'host:port' strings."
                )
        elif isinstance(bootstrap_servers, list):
            if not bootstrap_servers:
                raise ValueError("bootstrap_servers cannot be empty list")
            for server in bootstrap_servers:
                if not isinstance(server, str) or ":" not in server:
                    raise ValueError(
                        f"Invalid bootstrap_servers format: {server}. "
                        "Expected 'host:port' string."
                    )

        self.topics = topics if isinstance(topics, list) else [topics]
        self.bootstrap_servers = (
            bootstrap_servers
            if isinstance(bootstrap_servers, list)
            else [bootstrap_servers]
        )
        self.start_offset = start_offset
        self.end_offset = end_offset
        self.authentication = authentication or {}
        self.max_records_per_task = max_records_per_task
        self.poll_timeout_ms = poll_timeout_ms

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory data size, or None if unknown."""
        return None

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        """Create read tasks for Kafka partitions.

        Creates one read task per partition for better scalability and parallelism.
        Each task reads from a single partition of a single topic.

        Args:
            parallelism: Number of parallel read tasks to create.
            per_task_row_limit: Maximum number of rows per read task.

        Returns:
            List of ReadTask objects, one per partition.
        """
        if parallelism <= 0:
            raise ValueError(f"parallelism must be > 0, got: {parallelism}")

        # Discover all partitions for all topics
        # We need to create a consumer on the driver to discover partitions
        from kafka import KafkaConsumer

        # Build minimal consumer config for partition discovery
        consumer_config = _build_consumer_config_for_discovery(
            self.bootstrap_servers, self.authentication
        )

        # Discover partitions for all topics
        topic_partitions = []  # List of (topic, partition) tuples
        discovery_consumer = None
        try:
            discovery_consumer = KafkaConsumer(**consumer_config)
            for topic in self.topics:
                partitions = discovery_consumer.partitions_for_topic(topic)
                if not partitions:
                    raise ValueError(
                        f"Topic {topic} has no partitions or doesn't exist"
                    )
                for partition in partitions:
                    topic_partitions.append((topic, partition))
        finally:
            if discovery_consumer:
                discovery_consumer.close()

        if not topic_partitions:
            return []

        # Limit to parallelism
        topic_partitions = topic_partitions[:parallelism]

        # Store config for use in read functions (avoid serialization issues)
        bootstrap_servers = self.bootstrap_servers
        start_offset = self.start_offset
        end_offset = self.end_offset
        authentication = self.authentication
        max_records_per_task = self.max_records_per_task
        poll_timeout_ms = self.poll_timeout_ms

        tasks = []
        for topic_name, partition_id in topic_partitions:

            def create_kafka_read_fn(
                topic_name: str = topic_name,
                partition_id: int = partition_id,
                bootstrap_servers: List[str] = bootstrap_servers,
                start_offset: Optional[Union[int, str, datetime]] = start_offset,
                end_offset: Optional[Union[int, str, datetime]] = end_offset,
                authentication: Dict[str, Any] = authentication,
                max_records_per_task: int = max_records_per_task,
                poll_timeout_ms: int = poll_timeout_ms,
                per_task_row_limit: Optional[int] = per_task_row_limit,
            ):
                """Create a Kafka read function with captured variables.

                This factory function captures configuration variables as default arguments
                to avoid serialization issues when the read function is executed remotely
                by Ray. Using default arguments ensures all needed config is available
                in the remote task without requiring 'self' to be serialized.
                """

                def kafka_read_fn() -> Iterable[Block]:
                    """Read function for a single Kafka partition using kafka-python.

                    This function runs remotely in a Ray task. It creates a KafkaConsumer,
                    reads messages from a single assigned partition, and yields PyArrow tables
                    incrementally for efficient streaming processing.
                    """
                    from kafka import KafkaConsumer, TopicPartition

                    # Build consumer configuration
                    consumer_config = _build_consumer_config_for_read(
                        bootstrap_servers, authentication, topic_name, partition_id
                    )

                    # Create the Kafka consumer
                    consumer = KafkaConsumer(**consumer_config)

                    try:
                        # Assign only the specific partition for this task
                        topic_partition = TopicPartition(topic_name, partition_id)
                        consumer.assign([topic_partition])

                        # Determine start offset for this partition
                        if start_offset is not None:
                            start_off = _resolve_offset(
                                consumer, topic_partition, start_offset, is_end=False
                            )
                        else:
                            # Default to earliest
                            consumer.seek_to_beginning(topic_partition)
                            start_off = consumer.position(topic_partition)

                        # Determine end offset for this partition
                        end_off = None
                        if end_offset is not None:
                            end_off = _resolve_offset(
                                consumer, topic_partition, end_offset, is_end=True
                            )

                        # Validate start_offset <= end_offset
                        if end_off is not None and start_off > end_off:
                            raise ValueError(
                                f"start_offset ({start_off}) > end_offset ({end_off}) "
                                f"for partition {partition_id} in topic {topic_name}"
                            )

                        # Seek to the requested starting position
                        consumer.seek(topic_partition, start_off)

                        records = []
                        records_read = 0
                        # Use per_task_row_limit if provided, otherwise use max_records_per_task
                        effective_limit = (
                            per_task_row_limit
                            if per_task_row_limit is not None
                            else max_records_per_task
                        )
                        ctx = DataContext.get_current()
                        output_buffer = BlockOutputBuffer(
                            OutputBlockSizeOption.of(
                                target_max_block_size=ctx.target_max_block_size
                            )
                        )

                        # Main polling loop - read messages until we hit effective_limit or end_offset
                        partition_done = False
                        while records_read < effective_limit and not partition_done:
                            # Poll for a batch of messages from Kafka
                            msg_batch = consumer.poll(
                                timeout_ms=poll_timeout_ms,
                                max_records=min(effective_limit - records_read, 500),
                            )

                            if not msg_batch:
                                # No more messages available right now
                                # Yield any accumulated records and exit the loop
                                break

                            # poll() returns dict of {TopicPartition: [messages]}
                            # Since we only assigned one partition, there should be at most one entry
                            messages = msg_batch.get(topic_partition, [])

                            for msg in messages:
                                # Check if we've reached the end offset (for bounded reads)
                                # Use >= for exclusive end_offset (don't include end_offset message)
                                if end_off is not None and msg.offset >= end_off:
                                    partition_done = True
                                    break

                                # Extract all message metadata into a flat record
                                headers_dict = _convert_headers_to_dict(msg.headers)

                                records.append(
                                    {
                                        "offset": msg.offset,
                                        "key": msg.key,
                                        "value": msg.value,
                                        "topic": msg.topic,
                                        "partition": msg.partition,
                                        "timestamp": msg.timestamp,
                                        "timestamp_type": msg.timestamp_type,
                                        "headers": headers_dict,
                                    }
                                )
                                records_read += 1

                                # Yield incrementally when we hit batch size
                                if len(records) >= KafkaDatasource.BATCH_SIZE_FOR_YIELD:
                                    table = pa.Table.from_pylist(records)
                                    output_buffer.add_block(table)
                                    if output_buffer.has_next():
                                        yield output_buffer.next()
                                    records = []  # Clear for next batch

                                # Check if we've hit our total limit
                                if records_read >= effective_limit:
                                    # Yield final batch and finalize buffer
                                    if records:
                                        table = pa.Table.from_pylist(records)
                                        output_buffer.add_block(table)
                                    output_buffer.finalize()
                                    if output_buffer.has_next():
                                        yield output_buffer.next()
                                    return

                        # Yield any remaining records
                        if records:
                            table = pa.Table.from_pylist(records)
                            output_buffer.add_block(table)
                            if output_buffer.has_next():
                                yield output_buffer.next()

                        output_buffer.finalize()
                        if output_buffer.has_next():
                            yield output_buffer.next()

                    finally:
                        # Always close the consumer to release connections
                        consumer.close()

                return kafka_read_fn

            # Create metadata for this task
            metadata = BlockMetadata(
                num_rows=max_records_per_task,
                size_bytes=None,
                input_files=[f"kafka://{topic_name}/{partition_id}"],
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
            task = ReadTask(
                read_fn=call_with_retry(
                    create_kafka_read_fn(topic_name, partition_id),
                    description=f"read Kafka topic {topic_name} partition {partition_id}",
                ),
                metadata=metadata,
                schema=schema,
                per_task_row_limit=per_task_row_limit,
            )
            tasks.append(task)

        return tasks
