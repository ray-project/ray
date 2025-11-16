"""Kafka datasource for bounded data reads.

This module provides a Kafka datasource implementation for Ray Data that supports
bounded reads with offset-based range queries.

Message keys are decoded as UTF-8 strings (common case for routing keys).
Message values are returned as raw bytes to support any serialization format
(JSON, Avro, Protobuf, etc.). Users can decode values using map operations.

Requires:
    - kafka-python: https://kafka-python.readthedocs.io/
"""

import logging
import time
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
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

logger = logging.getLogger(__name__)


@dataclass
class KafkaAuthConfig:
    """Authentication configuration for Kafka connections.

    Uses standard kafka-python parameter names. See kafka-python documentation
    for full details: https://kafka-python.readthedocs.io/

    security_protocol: Protocol used to communicate with brokers.
        Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
        Default: PLAINTEXT.
    sasl_mechanism: Authentication mechanism when security_protocol
        is configured for SASL_PLAINTEXT or SASL_SSL. Valid values are:
        PLAIN, GSSAPI, OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512.
    sasl_plain_username: username for sasl PLAIN and SCRAM authentication.
        Required if sasl_mechanism is PLAIN or one of the SCRAM mechanisms.
    sasl_plain_password: password for sasl PLAIN and SCRAM authentication.
        Required if sasl_mechanism is PLAIN or one of the SCRAM mechanisms.
    sasl_kerberos_name: Constructed gssapi.Name for use with
        sasl mechanism handshake. If provided, sasl_kerberos_service_name and
        sasl_kerberos_domain name are ignored. Default: None.
    sasl_kerberos_service_name: Service name to include in GSSAPI
        sasl mechanism handshake. Default: 'kafka'
    sasl_kerberos_domain_name: kerberos domain name to use in GSSAPI
        sasl mechanism handshake. Default: one of bootstrap servers
    sasl_oauth_token_provider: OAuthBearer
        token provider instance. Default: None
    ssl_context: Pre-configured SSLContext for wrapping
        socket connections. If provided, all other ssl_* configurations
        will be ignored. Default: None.
    ssl_check_hostname: Flag to configure whether ssl handshake
        should verify that the certificate matches the brokers hostname.
        Default: True.
    ssl_cafile: Optional filename of ca file to use in certificate
        verification. Default: None.
    ssl_certfile: Optional filename of file in pem format containing
        the client certificate, as well as any ca certificates needed to
        establish the certificate's authenticity. Default: None.
    ssl_keyfile: Optional filename containing the client private key.
        Default: None.
    ssl_password: Optional password to be used when loading the
        certificate chain. Default: None.
    ssl_crlfile: Optional filename containing the CRL to check for
        certificate expiration. By default, no CRL check is done. When
        providing a file, only the leaf certificate will be checked against
        this CRL. The CRL can only be checked with Python 3.4+ or 2.7.9+.
        Default: None.
    ssl_ciphers: optionally set the available ciphers for ssl
        connections. It should be a string in the OpenSSL cipher list
        format. If no cipher can be selected (because compile-time options
        or other configuration forbids use of all the specified ciphers),
        an ssl.SSLError will be raised. See ssl.SSLContext.set_ciphers
    """

    # Security protocol
    security_protocol: str

    # SASL configuration
    sasl_mechanism: str
    sasl_plain_username: str
    sasl_plain_password: str
    sasl_kerberos_name: str
    sasl_kerberos_service_name: str
    sasl_kerberos_domain_name: str
    sasl_oauth_token_provider: Any

    # SSL configuration
    ssl_context: Any
    ssl_check_hostname: bool
    ssl_cafile: str
    ssl_certfile: str
    ssl_keyfile: str
    ssl_password: str
    ssl_ciphers: str
    ssl_crlfile: str


def _add_authentication_to_config(
    config: Dict[str, Any], kafka_auth_config: Optional[KafkaAuthConfig]
) -> None:
    """Add authentication configuration to consumer config in-place.

    Args:
        config: Consumer config dict to modify.
        kafka_auth_config: Authentication configuration.
    """
    if kafka_auth_config:
        config.update(kafka_auth_config)


def _build_consumer_config_for_discovery(
    bootstrap_servers: List[str], kafka_auth_config: Optional[KafkaAuthConfig]
) -> Dict[str, Any]:
    """Build minimal consumer config for partition discovery.

    Args:
        bootstrap_servers: List of Kafka broker addresses.
        kafka_auth_config: Authentication configuration.

    Returns:
        Consumer configuration dict for discovery.
    """
    config = {
        "bootstrap_servers": bootstrap_servers,
        "enable_auto_commit": False,
        "consumer_timeout_ms": 1000,  # Short timeout for discovery
    }
    _add_authentication_to_config(config, kafka_auth_config)
    return config


def _build_consumer_config_for_read(
    bootstrap_servers: List[str],
    kafka_auth_config: Optional[KafkaAuthConfig],
) -> Dict[str, Any]:
    """Build full consumer config for reading messages.

    Args:
        bootstrap_servers: List of Kafka broker addresses.
        kafka_auth_config: Authentication configuration.

    Returns:
        Consumer configuration dict for reading.
    """
    config = {
        "bootstrap_servers": bootstrap_servers,
        "enable_auto_commit": False,
        "value_deserializer": lambda v: v,
        "key_deserializer": lambda k: k,
    }
    _add_authentication_to_config(config, kafka_auth_config)
    return config


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
    """
    earliest_offset = consumer.beginning_offsets([topic_partition])[topic_partition]
    latest_offset = consumer.end_offsets([topic_partition])[topic_partition]

    # Keep original values for error messages
    original_start = start_offset
    original_end = end_offset

    if start_offset == "earliest" or start_offset is None:
        start_offset = earliest_offset
    if end_offset == "latest" or end_offset is None:
        end_offset = latest_offset

    if start_offset > end_offset:
        start_str = (
            f"{original_start}"
            if original_start == start_offset
            else f"{original_start} (resolved to {start_offset})"
        )
        end_str = (
            f"{original_end}"
            if original_end == end_offset
            else f"{original_end} (resolved to {end_offset})"
        )
        raise ValueError(
            f"start_offset ({start_str}) > end_offset ({end_str}) "
            f"for partition {topic_partition.partition} in topic {topic_partition.topic}"
        )
    return start_offset, end_offset


def _convert_headers_to_dict(headers: List[Tuple[bytes, bytes]]) -> Dict[str, bytes]:
    """Convert Kafka message headers to dictionary.

    Args:
        headers: Kafka message headers (list of tuples).

    Returns:
        Dictionary with string keys and binary values.

    Raises:
        ValueError: If header key cannot be decoded as UTF-8.
    """
    if not headers:
        return {}

    headers_dict = {}
    for key, value in headers:
        try:
            # Decode key as UTF-8 (keys should be strings)
            key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            # Keep value as bytes
            value_bytes = value
        except UnicodeDecodeError as e:
            raise ValueError(f"Failed to decode header key as UTF-8: {e}") from e
        headers_dict[key_str] = value_bytes

    return headers_dict


class KafkaDatasource(Datasource):
    """Kafka datasource for reading from Kafka topics with bounded reads."""

    # Batch size for incremental block yielding
    BATCH_SIZE_FOR_YIELD = 1000

    def __init__(
        self,
        topics: Union[str, List[str]],
        bootstrap_servers: Union[str, List[str]],
        start_offset: Union[int, Literal["earliest"]] = "earliest",
        end_offset: Union[int, Literal["latest"]] = "latest",
        kafka_auth_config: Optional[KafkaAuthConfig] = None,
        max_records_per_task: int = 1000,
        timeout_ms: int = 10000,
    ):
        """Initialize Kafka datasource.

        Args:
            topics: Kafka topic name(s) to read from.
            bootstrap_servers: Kafka broker addresses (string or list of strings).
            start_offset: Starting position. Can be:
                - int: Offset number or timestamp in milliseconds
                - str: "earliest", "latest", or offset number as string
            end_offset: Ending position. Can be:
                - int: Offset number or timestamp in milliseconds
                - str: Offset number as string
            kafka_auth_config: Authentication configuration. See KafkaAuthConfig for details.
            max_records_per_task: Maximum records per task per batch.
            timeout_ms: Timeout in milliseconds to poll to until reaching end_offset (default 10000ms/10s).

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

        if timeout_ms <= 0:
            raise ValueError("poll_timeout_ms must be positive")

        if isinstance(start_offset, int) and isinstance(end_offset, int):
            if start_offset > end_offset:
                raise ValueError("start_offset must be less than end_offset")

        if isinstance(start_offset, str) and start_offset == "latest":
            raise ValueError("start_offset cannot be 'latest'")
        if isinstance(end_offset, str) and end_offset == "earliest":
            raise ValueError("end_offset cannot be 'earliest'")

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
        self.kafka_auth_config = kafka_auth_config or {}
        self.max_records_per_task = max_records_per_task
        self.timeout_ms = timeout_ms

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory data size, or None if unknown."""
        return None

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        """Create read tasks for Kafka partitions.

        Creates one read task per partition.
        Each task reads from a single partition of a single topic.

        Args:
            parallelism: This argument is deprecated.
            per_task_row_limit: Maximum number of rows per read task.

        Returns:
            List of ReadTask objects, one per partition.
        """

        # Discover all partitions for all topics
        # We need to create a consumer on the driver to discover partitions
        from kafka import KafkaConsumer

        # Build minimal consumer config for partition discovery
        consumer_config = _build_consumer_config_for_discovery(
            self.bootstrap_servers, self.kafka_auth_config
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

        # Store config for use in read functions (avoid serialization issues)
        bootstrap_servers = self.bootstrap_servers
        start_offset = self.start_offset
        end_offset = self.end_offset
        max_records_per_task = self.max_records_per_task
        timeout_ms = self.timeout_ms
        kafka_auth_config = self.kafka_auth_config

        tasks = []
        for topic_name, partition_id in topic_partitions:

            def create_kafka_read_fn(
                topic_name: str = topic_name,
                partition_id: int = partition_id,
                bootstrap_servers: List[str] = bootstrap_servers,
                start_offset: Optional[Union[int, Literal["earliest"]]] = start_offset,
                end_offset: Optional[Union[int, Literal["latest"]]] = end_offset,
                kafka_auth_config: Optional[KafkaAuthConfig] = kafka_auth_config,
                timeout_ms: int = timeout_ms,
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
                        bootstrap_servers, kafka_auth_config
                    )

                    # Create the Kafka consumer
                    consumer = KafkaConsumer(**consumer_config)
                    try:
                        # Assign only the specific partition for this task
                        topic_partition = TopicPartition(topic_name, partition_id)
                        consumer.assign([topic_partition])

                        start_off, end_off = _resolve_offsets(
                            consumer, topic_partition, start_offset, end_offset
                        )
                        # Seek to the requested starting position
                        consumer.seek(topic_partition, start_off)

                        records = []
                        records_read = 0
                        # Use per_task_row_limit if provided, otherwise use max_records_per_task
                        ctx = DataContext.get_current()
                        output_buffer = BlockOutputBuffer(
                            OutputBlockSizeOption.of(
                                target_max_block_size=ctx.target_max_block_size
                            )
                        )

                        # Main polling loop - read maximum 500 messages per loop
                        partition_done = False
                        start_time = time.time()
                        timeout_seconds = timeout_ms / 1000.0

                        while not partition_done:
                            # Check if overall timeout has been reached
                            elapsed_time = time.time() - start_time
                            if elapsed_time >= timeout_seconds:
                                break

                            # Check if we've reached the end_offset before polling
                            # This avoids waiting for timeout when no more messages are available
                            current_position = consumer.position(topic_partition)
                            if current_position >= end_off:
                                break

                            # Calculate remaining timeout for this poll
                            remaining_timeout_ms = max(
                                1000, int((timeout_seconds - elapsed_time) * 1000)
                            )

                            # Poll for a batch of messages from Kafka
                            msg_batch = consumer.poll(
                                timeout_ms=min(remaining_timeout_ms, 10000),
                            )

                            if not msg_batch:
                                # No more messages available right now
                                # Yield any accumulated records and exit the loop
                                continue

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

            # Create schema - binary data for maximum flexibility
            schema = pa.schema(
                [
                    ("offset", pa.int64()),
                    ("key", pa.binary()),
                    ("value", pa.binary()),
                    ("topic", pa.string()),
                    ("partition", pa.int32()),
                    ("timestamp", pa.int64()),  # Kafka timestamp in milliseconds
                    ("timestamp_type", pa.int32()),  # 0=CreateTime, 1=LogAppendTime
                    ("headers", pa.map_(pa.string(), pa.binary())),  # Message headers
                ]
            )
            kafka_read_fn = create_kafka_read_fn(topic_name, partition_id)
            # Create read task
            task = ReadTask(
                read_fn=kafka_read_fn,
                metadata=metadata,
                schema=schema,
                per_task_row_limit=per_task_row_limit,
            )
            tasks.append(task)

        return tasks
