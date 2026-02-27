"""Kafka datasource for bounded data reads.

This module provides a Kafka datasource implementation for Ray Data that supports
bounded reads with offset-based range queries.

Message keys and values are returned as raw bytes to support any serialization format
(JSON, Avro, Protobuf, etc.). Users can decode them using map operations.

Requires:
    - confluent-kafka: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/
"""

import logging
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
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
    from confluent_kafka import Consumer, TopicPartition

from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)

# Mapping from kafka-python style KafkaAuthConfig fields to Confluent/librdkafka config.
_KAFKA_AUTH_TO_CONFLUENT: Dict[str, str] = {
    "security_protocol": "security.protocol",
    "sasl_mechanism": "sasl.mechanism",
    "sasl_plain_username": "sasl.username",
    "sasl_plain_password": "sasl.password",
    "sasl_kerberos_service_name": "sasl.kerberos.service.name",
    "sasl_kerberos_name": "sasl.kerberos.principal",
    "ssl_cafile": "ssl.ca.location",
    "ssl_certfile": "ssl.certificate.location",
    "ssl_keyfile": "ssl.key.location",
    "ssl_password": "ssl.key.password",
    "ssl_ciphers": "ssl.cipher.suites",
    "ssl_crlfile": "ssl.crl.location",
    # Note: ssl_check_hostname is intentionally NOT mapped due to semantics mismatch.
}


KAFKA_TOPIC_METADATA_TIMEOUT_S = 10
KAFKA_QUERY_OFFSET_TIMEOUT_S = 10

KAFKA_MSG_SCHEMA = pa.schema(
    [
        ("offset", pa.int64()),
        ("key", pa.binary()),
        ("value", pa.binary()),
        ("topic", pa.string()),
        ("partition", pa.int32()),
        ("timestamp", pa.int64()),  # Kafka timestamp in milliseconds
        (
            "timestamp_type",
            pa.int32(),
        ),  # 0=TIMESTAMP_NOT_AVAILABLE, 1=TIMESTAMP_CREATE_TIME, 2=TIMESTAMP_LOG_APPEND_TIME
        ("headers", pa.map_(pa.string(), pa.binary())),  # Message headers
    ]
)


@dataclass
class KafkaAuthConfig:
    """Authentication configuration for Kafka connections (kafka-python style).

    Deprecated: Prefer passing Confluent/librdkafka options via consumer_config.
    This class remains for backward compatibility and is mapped to Confluent
    configuration keys internally.
    """

    # Security protocol
    security_protocol: Optional[str] = None

    # SASL configuration
    sasl_mechanism: Optional[str] = None
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None
    sasl_kerberos_name: Optional[str] = None
    sasl_kerberos_service_name: Optional[str] = None
    sasl_kerberos_domain_name: Optional[str] = None
    sasl_oauth_token_provider: Optional[Any] = None

    # SSL configuration
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
    """Map KafkaAuthConfig (kafka-python style) into Confluent/librdkafka config.

    Special cases:
      - ssl_context: unsupported; warn and ignore
      - sasl_oauth_token_provider: unsupported; warn and ignore
      - sasl_kerberos_domain_name: unsupported; warn and ignore
      - ssl_check_hostname: not mapped due to semantics; if False, warn and ignore
    """
    if not kafka_auth_config:
        return
    warnings.warn(
        "kafka_auth_config (kafka-python style) is deprecated and will be removed in a future release. "
        "Please provide Confluent/librdkafka options via consumer_config instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    # Handle special fields with warnings
    if kafka_auth_config.ssl_context is not None:
        logger.warning(
            "ssl_context is not supported by Confluent. Skipping. "
            "Use KafkaAuthConfig fields ssl_cafile, ssl_certfile, ssl_keyfile instead."
        )
    if kafka_auth_config.sasl_oauth_token_provider is not None:
        logger.warning(
            "sasl_oauth_token_provider is not supported by Confluent. Skipping. "
            "Use consumer_config with sasl.oauthbearer.* options instead."
        )
    if kafka_auth_config.sasl_kerberos_domain_name is not None:
        logger.warning(
            "sasl_kerberos_domain_name is not supported by Confluent and will be ignored. "
            "Set sasl_kerberos_name (principal) or rely on defaults."
        )
    if kafka_auth_config.ssl_check_hostname is False:
        logger.warning(
            "ssl_check_hostname=False cannot be mapped safely to Confluent; "
            "setting enable.ssl.certificate.verification=False would disable all certificate verification. "
            "Ignoring ssl_check_hostname. If you need to disable only hostname verification, "
            "configure the client directly via consumer_config (e.g., ssl.endpoint.identification.algorithm=none)."
        )

    # Map directly compatible fields
    for key, confluent_key in _KAFKA_AUTH_TO_CONFLUENT.items():
        val = getattr(kafka_auth_config, key, None)
        if val is not None:
            config[confluent_key] = val


def _build_confluent_config(
    bootstrap_servers: List[str],
    kafka_auth_config: Optional[KafkaAuthConfig] = None,
    extra: Optional[Dict[str, Any]] = None,
    user_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build Confluent config with bootstrap servers and auth.

    Args:
        bootstrap_servers: List of Kafka broker addresses.
        kafka_auth_config: Authentication configuration (kafka-python style). Deprecated; prefer consumer_config with Confluent keys. Mutually exclusive with consumer_config.
        extra: Additional config options.
        user_config: User-provided config options.

    Returns:
        Confluent configuration dict.
    """
    config: Dict[str, Any] = {
        "bootstrap.servers": ",".join(bootstrap_servers),
    }
    # Map kafka-python-style auth if provided
    _add_authentication_to_config(config, kafka_auth_config)
    if extra:
        config.update(extra)
    if user_config:
        if (
            "bootstrap.servers" in user_config
            and user_config["bootstrap.servers"] != config["bootstrap.servers"]
        ):
            logger.warning(
                "Ignoring 'bootstrap.servers' from consumer_config; use bootstrap_servers parameter instead."
            )
        for k, v in user_config.items():
            if k == "bootstrap.servers":
                continue
            config[k] = v
    return config


def _build_consumer_config_for_read(
    bootstrap_servers: List[str],
    kafka_auth_config: Optional[KafkaAuthConfig] = None,
    consumer_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build Consumer config for reading messages (Confluent)."""
    return _build_confluent_config(
        bootstrap_servers,
        extra={
            "enable.auto.commit": False,
            # Confluent requires a group.id even when using manual assign.
            "group.id": "ray-data-kafka-reader",
        },
        user_config=consumer_config,
        kafka_auth_config=kafka_auth_config,
    )


def _datetime_to_ms(dt: datetime) -> int:
    """Convert a datetime to milliseconds since epoch (UTC).

    If the datetime has no timezone info (i.e., ``tzinfo is None``),
    it is assumed to be UTC. Timezone-aware datetimes are converted to
    UTC automatically via ``datetime.timestamp()``.

    Args:
        dt: A datetime object, with or without timezone info.

    Returns:
        Milliseconds since Unix epoch.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _resolve_offsets(
    consumer: "Consumer",
    topic_partition: "TopicPartition",
    start_offset: Union[int, datetime, Literal["earliest"]],
    end_offset: Union[int, datetime, Literal["latest"]],
) -> Tuple[int, int]:
    """Resolve start and end offsets to actual integer offsets.

    Handles int offsets, "earliest"/"latest" strings, and datetime objects.
    For datetime objects, uses ``consumer.offsets_for_times()`` to find the
    earliest offset whose timestamp is >= the given datetime.

    Args:
        consumer: Confluent Kafka consumer instance.
        topic_partition: TopicPartition to resolve offsets for.
        start_offset: Start offset (int, datetime, or "earliest").
        end_offset: End offset (int, datetime, or "latest").

    Returns:
        Tuple of (resolved_start_offset, resolved_end_offset).
    """
    from confluent_kafka import TopicPartition

    low, high = consumer.get_watermark_offsets(
        topic_partition, timeout=KAFKA_QUERY_OFFSET_TIMEOUT_S
    )
    earliest_offset = low
    latest_offset = high

    # Keep original values for error messages
    original_start = start_offset
    original_end = end_offset

    if start_offset == "earliest" or start_offset is None:
        start_offset = earliest_offset
    elif isinstance(start_offset, datetime):
        timestamp_ms = _datetime_to_ms(start_offset)
        tp_with_ts = TopicPartition(
            topic_partition.topic, topic_partition.partition, timestamp_ms
        )
        result = consumer.offsets_for_times(
            [tp_with_ts], timeout=KAFKA_QUERY_OFFSET_TIMEOUT_S
        )
        if result and result[0].offset >= 0:
            start_offset = result[0].offset
        else:
            start_offset = latest_offset

    if end_offset == "latest" or end_offset is None:
        end_offset = latest_offset
    elif isinstance(end_offset, datetime):
        timestamp_ms = _datetime_to_ms(end_offset)
        tp_with_ts = TopicPartition(
            topic_partition.topic, topic_partition.partition, timestamp_ms
        )
        result = consumer.offsets_for_times(
            [tp_with_ts], timeout=KAFKA_QUERY_OFFSET_TIMEOUT_S
        )
        if result and result[0].offset >= 0:
            end_offset = result[0].offset
        else:
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


class KafkaDatasource(Datasource):
    """Kafka datasource for reading from Kafka topics with bounded reads."""

    # Batch size for incremental block yielding
    BATCH_SIZE_FOR_YIELD = 1000

    def __init__(
        self,
        topics: Union[str, List[str]],
        bootstrap_servers: Union[str, List[str]],
        start_offset: Union[int, datetime, Literal["earliest"]] = "earliest",
        end_offset: Union[int, datetime, Literal["latest"]] = "latest",
        kafka_auth_config: Optional[KafkaAuthConfig] = None,
        consumer_config: Optional[Dict[str, Any]] = None,
        timeout_ms: int = 10000,
    ):
        """Initialize Kafka datasource.

        Args:
            topics: Kafka topic name(s) to read from.
            bootstrap_servers: Kafka broker addresses (string or list of strings).
            start_offset: Starting position. Can be:
                - int: Offset number
                - datetime: Read from the first message at or after this time.
                  datetimes with no timezone info are treated as UTC.
                - str: "earliest"
            end_offset: Ending position (exclusive). Can be:
                - int: Offset number
                - datetime: Read up to (but not including) the first message
                  at or after this time. datetimes with no timezone info are treated as UTC.
                - str: "latest"
            kafka_auth_config: Authentication configuration (kafka-python style). Deprecated; prefer consumer_config with Confluent keys. Mutually exclusive with consumer_config.
            consumer_config: Confluent/librdkafka consumer configuration dict.
                Keys and values are passed through to the underlying client. The
                `bootstrap.servers` option is derived from `bootstrap_servers` and
                cannot be overridden here.
            timeout_ms: Timeout in milliseconds for every read task to poll until reaching end_offset (default 10000ms).
                If the read task does not reach end_offset within the timeout, it will stop polling and return the messages
                it has read so far.

        Raises:
            ValueError: If required configuration is missing.
            ImportError: If confluent-kafka is not installed.
        """
        _check_import(self, module="confluent_kafka", package="confluent-kafka")

        if not topics:
            raise ValueError("topics cannot be empty")

        if not bootstrap_servers:
            raise ValueError("bootstrap_servers cannot be empty")

        if timeout_ms <= 0:
            raise ValueError("timeout_ms must be positive")

        if isinstance(start_offset, int) and isinstance(end_offset, int):
            if start_offset > end_offset:
                raise ValueError("start_offset must be less than end_offset")

        if isinstance(start_offset, datetime) and isinstance(end_offset, datetime):
            if _datetime_to_ms(start_offset) > _datetime_to_ms(end_offset):
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

        # Disallow specifying both config styles at once to avoid ambiguity.
        if kafka_auth_config is not None and consumer_config is not None:
            raise ValueError(
                "Provide only one of kafka_auth_config (deprecated) or consumer_config, not both."
            )

        self._topics = topics if isinstance(topics, list) else [topics]
        self._bootstrap_servers = (
            bootstrap_servers
            if isinstance(bootstrap_servers, list)
            else [bootstrap_servers]
        )
        self._start_offset = start_offset
        self._end_offset = end_offset
        self._kafka_auth_config = kafka_auth_config
        self._consumer_config = consumer_config
        self._timeout_ms = timeout_ms
        self._target_max_block_size = DataContext.get_current().target_max_block_size

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory data size, or None if unknown."""
        return None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        """Create read tasks for Kafka partitions.

        Creates one read task per partition.
        Each task reads from a single partition of a single topic.

        Args:
            parallelism: This argument is deprecated.
            per_task_row_limit: Maximum number of rows per read task.
            data_context: The data context to use to get read tasks. This is not used by this datasource.

        Returns:
            List of ReadTask objects, one per partition.
        """
        from confluent_kafka import Consumer

        consumer_config = _build_consumer_config_for_read(
            self._bootstrap_servers, self._kafka_auth_config, self._consumer_config
        )
        discovery_consumer = Consumer(consumer_config)
        try:
            metadata = discovery_consumer.list_topics(
                timeout=KAFKA_TOPIC_METADATA_TIMEOUT_S
            )

            topic_partitions: List[Tuple[str, int]] = []
            for topic in self._topics:
                if topic not in metadata.topics:
                    raise ValueError(
                        f"Topic {topic} has no partitions or doesn't exist"
                    )
                topic_meta = metadata.topics[topic]
                if not topic_meta.partitions:
                    raise ValueError(
                        f"Topic {topic} has no partitions or doesn't exist"
                    )
                for partition_id in topic_meta.partitions.keys():
                    topic_partitions.append((topic, partition_id))
        finally:
            discovery_consumer.close()

        bootstrap_servers = self._bootstrap_servers
        start_offset = self._start_offset
        end_offset = self._end_offset
        timeout_ms = self._timeout_ms
        target_max_block_size = self._target_max_block_size

        tasks = []
        for topic_name, partition_id in topic_partitions:

            def create_kafka_read_fn(
                topic_name: str = topic_name,
                partition_id: int = partition_id,
                bootstrap_servers: List[str] = bootstrap_servers,
                start_offset: Optional[
                    Union[int, datetime, Literal["earliest"]]
                ] = start_offset,
                end_offset: Optional[
                    Union[int, datetime, Literal["latest"]]
                ] = end_offset,
                kafka_auth_config: Optional[KafkaAuthConfig] = self._kafka_auth_config,
                user_consumer_config: Optional[Dict[str, Any]] = self._consumer_config,
                timeout_ms: int = timeout_ms,
                target_max_block_size: int = target_max_block_size,
            ):
                """Create a Kafka read function with captured variables."""

                def kafka_read_fn() -> Iterable[Block]:
                    """Read function for a single Kafka partition using confluent-kafka."""
                    from confluent_kafka import Consumer, TopicPartition

                    built_consumer_config = _build_consumer_config_for_read(
                        bootstrap_servers, kafka_auth_config, user_consumer_config
                    )

                    consumer = Consumer(built_consumer_config)
                    try:
                        topic_partition = TopicPartition(topic_name, partition_id)
                        resolved_start, resolved_end = _resolve_offsets(
                            consumer, topic_partition, start_offset, end_offset
                        )

                        records = []

                        output_buffer = BlockOutputBuffer(
                            OutputBlockSizeOption.of(
                                target_max_block_size=target_max_block_size
                            )
                        )

                        if resolved_start < resolved_end:
                            start_time = time.perf_counter()
                            timeout_seconds = timeout_ms / 1000.0
                            # Assign with the desired starting offset to avoid seek state errors
                            tp_with_offset = TopicPartition(
                                topic_name, partition_id, resolved_start
                            )
                            consumer.assign([tp_with_offset])
                            # Ensure assignment is made active before position checks.
                            # In librdkafka, assign() is applied asynchronously and becomes
                            # effective on the next poll/consume call. A non-blocking poll(0)
                            # drives the internal event loop so that position() and reads
                            # reflect the assigned start offset. This is not a wait; keep it 0.
                            consumer.poll(0)

                            partition_done = False
                            while not partition_done:
                                # Check if overall timeout has been reached
                                elapsed_time = time.perf_counter() - start_time
                                if elapsed_time >= timeout_seconds:
                                    logger.warning(
                                        f"Kafka read task timed out after {timeout_ms}ms while reading partition {partition_id} of topic {topic_name}; "
                                        f"end_offset {resolved_end} was not reached. Returning {len(records)} messages collected in this read task so far."
                                    )
                                    break

                                # Check if we've reached the end_offset before polling
                                positions = consumer.position([topic_partition])
                                current_position = (
                                    positions[0].offset if positions else resolved_start
                                )
                                if current_position >= resolved_end:
                                    break

                                remaining_timeout_ms = int(
                                    max(0, timeout_seconds - elapsed_time) * 1000
                                )
                                poll_timeout_s = remaining_timeout_ms / 1000.0

                                # TODO: Use consume() instead of poll() to reduce round trips.
                                msg = consumer.poll(timeout=poll_timeout_s)
                                if msg is None:
                                    continue
                                if msg.error():
                                    # Preserve old semantics: skip errors
                                    continue

                                # Stop once we reached the end offset (exclusive)
                                if msg.offset() >= resolved_end:
                                    partition_done = True
                                    break

                                ts_type, ts_ms = msg.timestamp()
                                headers_list = msg.headers() or []
                                headers_dict = dict(headers_list)
                                records.append(
                                    {
                                        "offset": msg.offset(),
                                        "key": msg.key(),
                                        "value": msg.value(),
                                        "topic": msg.topic(),
                                        "partition": msg.partition(),
                                        "timestamp": ts_ms,
                                        "timestamp_type": ts_type,
                                        "headers": headers_dict,
                                    }
                                )

                                # Yield incrementally when we hit batch size
                                if len(records) >= KafkaDatasource.BATCH_SIZE_FOR_YIELD:
                                    table = pa.Table.from_pylist(records)
                                    output_buffer.add_block(table)
                                    while output_buffer.has_next():
                                        yield output_buffer.next()
                                    records = []  # Clear for next batch

                        # Yield any remaining records
                        if records:
                            table = pa.Table.from_pylist(records)
                            output_buffer.add_block(table)

                        output_buffer.finalize()
                        while output_buffer.has_next():
                            yield output_buffer.next()

                    finally:
                        consumer.close()

                return kafka_read_fn

            # TODO: We could output the offset range for every partition after the read is done.
            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                input_files=[f"kafka://{topic_name}/{partition_id}"],
                exec_stats=None,
            )

            kafka_read_fn = create_kafka_read_fn(topic_name, partition_id)
            # Create read task
            task = ReadTask(
                read_fn=kafka_read_fn,
                metadata=metadata,
                schema=KAFKA_MSG_SCHEMA,
                per_task_row_limit=per_task_row_limit,
            )
            tasks.append(task)

        return tasks
