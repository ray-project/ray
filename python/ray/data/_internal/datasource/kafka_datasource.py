"""Kafka datasource for bounded and unbounded data reads.

This module provides two Kafka datasource implementations for Ray Data:

1. KafkaBoundedDatasource: For bounded batch reads with offset ranges (trigger="once")
   - Uses Datasource base class
   - Reads between start_offset and end_offset
   - Returns binary key/value for flexibility

2. KafkaStreamingDatasource: For unbounded streaming reads (trigger="continuous", etc.)
   - Uses UnboundDatasource base class
   - Supports continuous, interval, and cron triggers
   - Returns string key/value for convenience

Requires:
    - kafka-python: https://kafka-python.readthedocs.io/
"""

import logging
import time
from dataclasses import dataclass, fields
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa

if TYPE_CHECKING:
    from kafka import KafkaConsumer, TopicPartition

from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask
from ray.data.datasource.unbound_datasource import (
    UnboundDatasource,
    create_unbound_read_task,
)

logger = logging.getLogger(__name__)

# Batch size for incremental yielding
_KAFKA_BATCH_SIZE = 1000


@dataclass
class KafkaAuthConfig:
    """Authentication configuration for Kafka connections.

    Uses standard kafka-python parameter names. See kafka-python documentation
    for full details: https://kafka-python.readthedocs.io/

    Attributes:
        security_protocol: Protocol used to communicate with brokers
            (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL).
        sasl_mechanism: SASL authentication mechanism
            (PLAIN, GSSAPI, OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512).
        sasl_plain_username: Username for SASL PLAIN/SCRAM authentication.
        sasl_plain_password: Password for SASL PLAIN/SCRAM authentication.
        sasl_kerberos_name: Constructed gssapi.Name for GSSAPI.
        sasl_kerberos_service_name: Service name for GSSAPI (default: 'kafka').
        sasl_kerberos_domain_name: Kerberos domain name for GSSAPI.
        sasl_oauth_token_provider: OAuthBearer token provider instance.
        ssl_context: Pre-configured SSLContext (overrides other ssl_* options).
        ssl_check_hostname: Verify that certificate matches broker hostname.
        ssl_cafile: CA certificate file path.
        ssl_certfile: Client certificate file path.
        ssl_keyfile: Client private key file path.
        ssl_password: Password for loading certificate chain.
        ssl_crlfile: CRL file path for certificate expiration checking.
        ssl_ciphers: Available ciphers for SSL connections (OpenSSL format).
    """

    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None
    sasl_kerberos_name: Optional[str] = None
    sasl_kerberos_service_name: Optional[str] = None
    sasl_kerberos_domain_name: Optional[str] = None
    sasl_oauth_token_provider: Optional[Any] = None
    ssl_context: Optional[Any] = None
    ssl_check_hostname: Optional[bool] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_password: Optional[str] = None
    ssl_ciphers: Optional[str] = None
    ssl_crlfile: Optional[str] = None


def _add_authentication_to_config(
    config: Dict[str, Any], kafka_auth_config: Optional[KafkaAuthConfig]
) -> None:
    """Add authentication configuration to consumer config in-place.

    Args:
        config: Consumer config dict to modify.
        kafka_auth_config: Authentication configuration.
    """
    if kafka_auth_config:
        for field in fields(kafka_auth_config):
            value = getattr(kafka_auth_config, field.name)
            if value is not None:
                config[field.name] = value


def _validate_bootstrap_servers(bootstrap_servers: Union[str, List[str]]) -> List[str]:
    """Validate and normalize bootstrap_servers format.

    Args:
        bootstrap_servers: Server string or list of server strings.

    Returns:
        Normalized list of server strings.

    Raises:
        ValueError: If format is invalid.
    """
    if isinstance(bootstrap_servers, str):
        if not bootstrap_servers or ":" not in bootstrap_servers:
            raise ValueError(
                f"Invalid bootstrap_servers format: {bootstrap_servers}. "
                "Expected 'host:port' format."
            )
        return [bootstrap_servers]
    elif isinstance(bootstrap_servers, list):
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers cannot be empty list")
        for server in bootstrap_servers:
            if not isinstance(server, str) or ":" not in server:
                raise ValueError(
                    f"Invalid bootstrap_servers format: {server}. "
                    "Expected 'host:port' format."
                )
        return bootstrap_servers
    else:
        raise ValueError("bootstrap_servers must be string or list of strings")


def _resolve_offsets(
    consumer: "KafkaConsumer",
    topic_partition: "TopicPartition",
    start_offset: Union[int, Literal["earliest"]],
    end_offset: Union[int, Literal["latest"]],
) -> Tuple[int, int]:
    """Resolve start and end offsets to actual integer offsets.

    Args:
        consumer: Kafka consumer instance.
        topic_partition: TopicPartition to resolve offsets for.
        start_offset: Start offset (int or "earliest").
        end_offset: End offset (int or "latest").

    Returns:
        Tuple of (resolved_start_offset, resolved_end_offset).

    Raises:
        ValueError: If start_offset > end_offset after resolution.
    """
    earliest_offset = consumer.beginning_offsets([topic_partition])[topic_partition]
    latest_offset = consumer.end_offsets([topic_partition])[topic_partition]

    original_start, original_end = start_offset, end_offset

    if start_offset == "earliest" or start_offset is None:
        start_offset = earliest_offset
    if end_offset == "latest" or end_offset is None:
        end_offset = latest_offset

    if start_offset > end_offset:
        raise ValueError(
            f"start_offset ({original_start} -> {start_offset}) > "
            f"end_offset ({original_end} -> {end_offset}) "
            f"for partition {topic_partition.partition} in topic {topic_partition.topic}"
        )
    return start_offset, end_offset


# ============================================================================
# BOUNDED DATASOURCE (from master - for trigger="once" batch reads)
# ============================================================================


class KafkaBoundedDatasource(Datasource):
    """Kafka datasource for bounded batch reads with offset-based range queries.

    This datasource creates one read task per Kafka partition and reads messages
    between specified start and end offsets. Messages are returned with binary
    key/value fields to support any serialization format (JSON, Avro, Protobuf).

    Used when trigger="once" in read_kafka().
    """

    def __init__(
        self,
        topics: Union[str, List[str]],
        bootstrap_servers: Union[str, List[str]],
        start_offset: Union[int, Literal["earliest"]] = "earliest",
        end_offset: Union[int, Literal["latest"]] = "latest",
        kafka_auth_config: Optional[KafkaAuthConfig] = None,
        timeout_ms: int = 10000,
    ):
        """Initialize Kafka bounded datasource.

        Args:
            topics: Kafka topic name(s) to read from.
            bootstrap_servers: Kafka broker addresses.
            start_offset: Starting position (int or "earliest").
            end_offset: Ending position (int or "latest").
            kafka_auth_config: Authentication configuration.
            timeout_ms: Timeout for polling until reaching end_offset.

        Raises:
            ValueError: If configuration is invalid.
            ImportError: If kafka-python is not installed.
        """
        _check_import(self, module="kafka", package="kafka-python")

        if not topics:
            raise ValueError("topics cannot be empty")

        if isinstance(start_offset, int) and isinstance(end_offset, int):
            if start_offset > end_offset:
                raise ValueError("start_offset must be less than end_offset")
        if isinstance(start_offset, str) and start_offset == "latest":
            raise ValueError("start_offset cannot be 'latest'")
        if isinstance(end_offset, str) and end_offset == "earliest":
            raise ValueError("end_offset cannot be 'earliest'")

        if timeout_ms <= 0:
            raise ValueError("timeout_ms must be positive")

        self._topics = topics if isinstance(topics, list) else [topics]
        self._bootstrap_servers = _validate_bootstrap_servers(bootstrap_servers)
        self._start_offset = start_offset
        self._end_offset = end_offset
        self._kafka_auth_config = kafka_auth_config
        self._timeout_ms = timeout_ms
        self._target_max_block_size = DataContext.get_current().target_max_block_size

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return estimated in-memory data size (unknown for Kafka)."""
        return None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        """Create read tasks for Kafka partitions.

        Args:
            parallelism: Deprecated (one task per partition is created).
            per_task_row_limit: Maximum number of rows per read task.
            data_context: Unused.

        Returns:
            List of ReadTask objects, one per Kafka partition.
        """
        from kafka import KafkaConsumer

        # Discover partitions for all topics
        config = {
            "bootstrap_servers": self._bootstrap_servers,
            "enable_auto_commit": False,
            "consumer_timeout_ms": 1000,
        }
        _add_authentication_to_config(config, self._kafka_auth_config)

        topic_partitions = []
        with KafkaConsumer(**config) as consumer:
            for topic in self._topics:
                partitions = consumer.partitions_for_topic(topic)
                if not partitions:
                    raise ValueError(
                        f"Topic {topic} has no partitions or doesn't exist"
                    )
                topic_partitions.extend((topic, p) for p in partitions)

        # Create read task for each partition
        schema = self._create_schema(binary_format=True)
        return [
            self._create_partition_read_task(
                topic, partition, schema, per_task_row_limit
            )
            for topic, partition in topic_partitions
        ]

    def _create_partition_read_task(
        self,
        topic: str,
        partition: int,
        schema: pa.Schema,
        per_task_row_limit: Optional[int],
    ) -> ReadTask:
        """Create a read task for a single Kafka partition.

        Args:
            topic: Topic name.
            partition: Partition ID.
            schema: PyArrow schema.
            per_task_row_limit: Row limit per task.

        Returns:
            ReadTask for this partition.
        """
        # Capture config to avoid serialization issues
        bootstrap_servers = self._bootstrap_servers
        start_offset = self._start_offset
        end_offset = self._end_offset
        auth_config = self._kafka_auth_config
        timeout_ms = self._timeout_ms
        target_max_block_size = self._target_max_block_size

        def read_fn() -> Iterable[Block]:
            """Read function for a single Kafka partition."""
            from kafka import KafkaConsumer, TopicPartition

            config = {
                "bootstrap_servers": bootstrap_servers,
                "enable_auto_commit": False,
                "value_deserializer": lambda v: v,  # Keep as bytes
                "key_deserializer": lambda k: k,  # Keep as bytes
            }
            _add_authentication_to_config(config, auth_config)

            with KafkaConsumer(**config) as consumer:
                topic_partition = TopicPartition(topic, partition)
                consumer.assign([topic_partition])

                start_off, end_off = _resolve_offsets(
                    consumer, topic_partition, start_offset, end_offset
                )
                consumer.seek(topic_partition, start_off)

                records = []
                output_buffer = BlockOutputBuffer(
                    OutputBlockSizeOption.of(target_max_block_size=target_max_block_size)
                )

                timeout_seconds = timeout_ms / 1000.0
                start_time = time.time()

                while True:
                    # Check timeout
                    if time.time() - start_time >= timeout_seconds:
                        logger.warning(
                            f"Kafka read timed out after {timeout_ms}ms for "
                            f"{topic}/{partition}, end_offset {end_off} not reached"
                        )
                        break

                    # Check if done
                    if consumer.position(topic_partition) >= end_off:
                        break

                    # Poll for messages
                    remaining_ms = int((timeout_seconds - (time.time() - start_time)) * 1000)
                    msg_batch = consumer.poll(timeout_ms=min(remaining_ms, 10000))

                    if not msg_batch:
                        continue

                    for msg in msg_batch.get(topic_partition, []):
                        if msg.offset >= end_off:
                            break

                        records.append(
                            {
                                "offset": msg.offset,
                                "key": msg.key,
                                "value": msg.value,
                                "topic": msg.topic,
                                "partition": msg.partition,
                                "timestamp": msg.timestamp,
                                "timestamp_type": msg.timestamp_type,
                                "headers": dict(msg.headers) if msg.headers else {},
                            }
                        )

                        if len(records) >= _KAFKA_BATCH_SIZE:
                            output_buffer.add_block(pa.Table.from_pylist(records))
                            while output_buffer.has_next():
                                yield output_buffer.next()
                            records = []

                # Yield remaining records
                if records:
                    output_buffer.add_block(pa.Table.from_pylist(records))
                output_buffer.finalize()
                while output_buffer.has_next():
                    yield output_buffer.next()

        metadata = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            input_files=[f"kafka://{topic}/{partition}"],
            exec_stats=None,
        )

        return ReadTask(
            read_fn=read_fn,
            metadata=metadata,
            schema=schema,
            per_task_row_limit=per_task_row_limit,
        )

    @staticmethod
    def _create_schema(binary_format: bool = True) -> pa.Schema:
        """Create PyArrow schema for Kafka messages.

        Args:
            binary_format: If True, use binary types; if False, use string types.

        Returns:
            PyArrow schema for Kafka messages.
        """
        value_type = pa.binary() if binary_format else pa.string()
        key_type = pa.binary() if binary_format else pa.string()
        header_value_type = pa.binary() if binary_format else pa.string()

        return pa.schema(
            [
                ("offset", pa.int64()),
                ("key", key_type),
                ("value", value_type),
                ("topic", pa.string()),
                ("partition", pa.int32()),
                ("timestamp", pa.int64()),
                ("timestamp_type", pa.int32()),
                ("headers", pa.map_(pa.string(), header_value_type)),
            ]
        )


# ============================================================================
# STREAMING DATASOURCE (from this branch - for continuous/interval/cron)
# ============================================================================


class KafkaStreamingDatasource(UnboundDatasource):
    """Kafka datasource for unbounded streaming reads.

    This datasource supports continuous, interval, and cron-based streaming reads
    from Kafka topics. Messages are deserialized as strings for convenience.

    Used when trigger is "continuous", "interval:*", or "cron:*" in read_kafka().
    """

    def __init__(
        self,
        topics: Union[str, List[str]],
        bootstrap_servers: Union[str, List[str]],
        kafka_auth_config: Optional[KafkaAuthConfig] = None,
        max_records_per_task: int = 1000,
        start_offset: Optional[str] = None,
        end_offset: Optional[str] = None,
        group_id: Optional[str] = None,
        poll_timeout_ms: int = 30000,
    ):
        """Initialize Kafka streaming datasource.

        Args:
            topics: Kafka topic name(s) to read from.
            bootstrap_servers: Kafka broker addresses.
            kafka_auth_config: Authentication configuration.
            max_records_per_task: Maximum records per task per batch.
            start_offset: Starting offset ('earliest', 'latest', or numeric string).
            end_offset: Ending offset (numeric string, or None for unbounded).
            group_id: Consumer group ID.
            poll_timeout_ms: Timeout for polling messages.

        Raises:
            ValueError: If configuration is invalid.
            ImportError: If kafka-python is not installed.
        """
        super().__init__("kafka")
        _check_import(self, module="kafka", package="kafka-python")

        if not topics:
            raise ValueError("topics cannot be empty")
        if max_records_per_task <= 0:
            raise ValueError("max_records_per_task must be positive")

        self.topics = topics if isinstance(topics, list) else [topics]
        self.bootstrap_servers = _validate_bootstrap_servers(bootstrap_servers)
        self.kafka_auth_config = kafka_auth_config
        self.max_records_per_task = max_records_per_task
        self.start_offset = start_offset or "latest"
        self.end_offset = end_offset
        self.group_id = group_id
        self.poll_timeout_ms = poll_timeout_ms

    def _get_read_tasks_for_partition(
        self,
        partition_info: Dict[str, Any],
        parallelism: int,
    ) -> List[ReadTask]:
        """Create read tasks for Kafka topics.

        Args:
            partition_info: Unused (Kafka uses topics, not partitions).
            parallelism: Number of parallel read tasks to create.

        Returns:
            List of ReadTask objects for Kafka topics.
        """
        topics_to_process = (
            self.topics[:parallelism] if parallelism > 0 else self.topics
        )

        schema = KafkaBoundedDatasource._create_schema(binary_format=False)
        return [
            self._create_topic_read_task(topic, schema) for topic in topics_to_process
        ]

    def _create_topic_read_task(self, topic: str, schema: pa.Schema) -> ReadTask:
        """Create a read task for a single topic.

        Args:
            topic: Topic name.
            schema: PyArrow schema.

        Returns:
            ReadTask for this topic.
        """
        # Capture config to avoid serialization issues
        bootstrap_servers = self.bootstrap_servers
        auth_config = self.kafka_auth_config
        max_records = self.max_records_per_task
        start_offset = self.start_offset
        end_offset = self.end_offset
        group_id = self.group_id
        poll_timeout_ms = self.poll_timeout_ms

        def read_fn() -> Iterator[pa.Table]:
            """Read function for Kafka topic (unbounded streaming)."""
            from kafka import KafkaConsumer

            # Determine auto_offset_reset
            auto_offset_reset = (
                "latest"
                if start_offset and start_offset.isdigit()
                else (start_offset or "latest")
            )

            config = {
                "bootstrap_servers": bootstrap_servers,
                "enable_auto_commit": False,
                "auto_offset_reset": auto_offset_reset,
                "group_id": group_id,
                "session_timeout_ms": 30000,
                "max_poll_records": min(max_records, 500),
                "value_deserializer": lambda v: v.decode("utf-8") if v else None,
                "key_deserializer": lambda k: k.decode("utf-8") if k else None,
            }
            _add_authentication_to_config(config, auth_config)

            with KafkaConsumer(topic, **config) as consumer:
                # Seek to numeric offset if specified
                if start_offset and start_offset.isdigit():

                    partitions = consumer.assignment()
                    if not partitions:
                        consumer.poll(timeout_ms=1000)
                        partitions = consumer.assignment()

                    offset_value = int(start_offset)
                    for partition in partitions:
                        consumer.seek(partition, offset_value)

                records = []
                records_read = 0

                while True:
                    msg_batch = consumer.poll(timeout_ms=poll_timeout_ms)

                    if not msg_batch:
                        if records:
                            yield pa.Table.from_pylist(records)
                            records = []
                        continue

                    for partition_msgs in msg_batch.values():
                        for msg in partition_msgs:
                            # Check end offset
                            if (
                                end_offset
                                and end_offset.isdigit()
                                and msg.offset >= int(end_offset)
                            ):
                                if records:
                                    yield pa.Table.from_pylist(records)
                                return

                            records.append(
                                {
                                    "offset": msg.offset,
                                    "key": msg.key,
                                    "value": msg.value,
                                    "topic": msg.topic,
                                    "partition": msg.partition,
                                    "timestamp": msg.timestamp,
                                    "timestamp_type": msg.timestamp_type,
                                    "headers": dict(msg.headers) if msg.headers else {},
                                }
                            )

                            records_read += 1

                            # Yield batch
                            if len(records) >= min(max_records, _KAFKA_BATCH_SIZE):
                                yield pa.Table.from_pylist(records)
                                records = []

                            # Check max records limit
                            if records_read >= max_records:
                                if records:
                                    yield pa.Table.from_pylist(records)
                                return

        metadata = BlockMetadata(
            num_rows=self.max_records_per_task,
            size_bytes=None,
            input_files=[f"kafka://{topic}"],
            exec_stats=None,
        )

        return create_unbound_read_task(read_fn=read_fn, metadata=metadata, schema=schema)


# Backward compatibility alias - keep for existing code
KafkaDatasource = KafkaBoundedDatasource
