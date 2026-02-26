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
from dataclasses import dataclass, fields
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
# KafkaAuthConfig keeps kafka-python names for backward compatibility.
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
    "ssl_check_hostname": "enable.ssl.certificate.verification",
}

KAFKA_TOPIC_METADATA_TIMEOUT_S = 10
KAFKA_QUERY_OFFSET_TIMEOUT_S = 10
# Batch size for consume() - fewer round trips than poll()
KAFKA_CONSUME_TIMEOUT_S = 10
CONSUME_BATCH_SIZE = 256
# Batch size for incremental block yielding to Ray Data
BATCH_SIZE_FOR_YIELD = 1000

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
    """Authentication configuration for Kafka connections.

    Uses parameter names compatible with kafka-python for backward compatibility.
    These are mapped to Confluent/librdkafka config when connecting.

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
    sasl_kerberos_domain_name: Kerberos domain name to use in GSSAPI
        sasl mechanism handshake. Note: This option is not supported by
        Confluent/librdkafka and will be ignored when building the client
        configuration. Prefer specifying an explicit principal via
        `sasl_kerberos_name` or rely on broker defaults.
    sasl_oauth_token_provider: OAuthBearer
        token provider instance. Default: None (Confluent uses sasl.oauthbearer.* config)
    ssl_context: Pre-configured SSLContext. Not supported by Confluent; use ssl_* file paths.
    ssl_check_hostname: Flag to configure whether ssl handshake
        should verify that the certificate matches the brokers hostname.
        Default: True. Mapped to enable.ssl.certificate.verification.
    ssl_cafile: Optional filename of ca file to use in certificate verification.
    ssl_certfile: Optional filename of file in pem format containing the client certificate.
    ssl_keyfile: Optional filename containing the client private key.
    ssl_password: Optional password to be used when loading the certificate chain.
    ssl_crlfile: Optional filename containing the CRL to check for certificate expiration.
    ssl_ciphers: optionally set the available ciphers for ssl connections.
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
    """Add authentication configuration to consumer config in-place.

    Converts KafkaAuthConfig (kafka-python style) to Confluent/librdkafka
    parameter names and updates the config dict.

    TODO: Once backward compatibility with kafka-python-style names is no longer
    needed, accept Confluent/librdkafka config directly (e.g. Dict or new dataclass)
    and remove the KafkaAuthConfig mapping layer.

    Args:
        config: Consumer config dict to modify.
        kafka_auth_config: Authentication configuration.
    """
    if not kafka_auth_config or not isinstance(kafka_auth_config, KafkaAuthConfig):
        return

    for field in fields(kafka_auth_config):
        value = getattr(kafka_auth_config, field.name)
        if value is None:
            continue

        # Skip ssl_context - Confluent doesn't support passing SSLContext directly
        if field.name == "ssl_context":
            logger.warning(
                "ssl_context is not supported by Confluent. Skipping. "
                "Use KafkaAuthConfig fields ssl_cafile, ssl_certfile, ssl_keyfile "
                "instead. See https://github.com/confluentinc/librdkafka/wiki/Using-SSL-with-librdkafka"
            )
            continue

        if field.name == "sasl_oauth_token_provider":
            logger.warning(
                "sasl_oauth_token_provider is not supported by Confluent. Skipping. "
                "Use consumer_config with sasl.oauthbearer.* options instead."
            )
            continue

        if field.name == "sasl_kerberos_domain_name":
            logger.warning(
                "sasl_kerberos_domain_name is not supported by Confluent and will be ignored. "
                "Set sasl_kerberos_name (principal) or rely on defaults."
            )
            continue

        confluent_key = _KAFKA_AUTH_TO_CONFLUENT.get(field.name)
        if confluent_key:
            config[confluent_key] = value


def _build_confluent_config(
    bootstrap_servers: List[str],
    kafka_auth_config: Optional[KafkaAuthConfig],
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build Confluent config with bootstrap servers and auth.

    Args:
        bootstrap_servers: List of Kafka broker addresses.
        kafka_auth_config: Authentication configuration.
        extra: Additional config options.

    Returns:
        Confluent configuration dict.
    """
    config: Dict[str, Any] = {
        "bootstrap.servers": ",".join(bootstrap_servers),
    }
    _add_authentication_to_config(config, kafka_auth_config)
    if extra:
        config.update(extra)
    return config


def _build_consumer_config(
    bootstrap_servers: List[str],
    kafka_auth_config: Optional[KafkaAuthConfig],
) -> Dict[str, Any]:
    """Build Consumer config for reading messages.

    Confluent Consumer returns raw bytes by default (no deserializers needed).
    """
    return _build_confluent_config(
        bootstrap_servers,
        kafka_auth_config,
        extra={
            "enable.auto.commit": False,
            "group.id": "ray-data-kafka-reader",  # Required by Confluent but we use assign()
            "enable.partition.eof": True,
        },
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


def _resolve_offsets_with_consumer(
    consumer: "Consumer",
    topic_partition: "TopicPartition",
    start_offset: Union[int, datetime, Literal["earliest"]],
    end_offset: Union[int, datetime, Literal["latest"]],
) -> Tuple[int, int]:
    """Resolve start and end offsets using Consumer.

    Uses get_watermark_offsets for earliest/latest and offsets_for_times for datetime.

    Args:
        consumer: Confluent Consumer instance (must have partition assigned).
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
    elif isinstance(start_offset, int) and start_offset > latest_offset:
        raise ValueError(
            f"start_offset {start_offset} is greater than latest_offset {latest_offset} for partition {topic_partition.partition} in topic {topic_partition.topic}"
        )

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
    elif isinstance(end_offset, int) and end_offset > latest_offset:
        raise ValueError(
            f"end_offset {end_offset} is greater than latest_offset {latest_offset} for partition {topic_partition.partition} in topic {topic_partition.topic}"
        )

    return start_offset, end_offset


class KafkaDatasource(Datasource):
    """Kafka datasource for reading from Kafka topics with bounded reads."""

    def __init__(
        self,
        topics: Union[str, List[str]],
        bootstrap_servers: Union[str, List[str]],
        start_offset: Union[int, datetime, Literal["earliest"]] = "earliest",
        end_offset: Union[int, datetime, Literal["latest"]] = "latest",
        kafka_auth_config: Optional[KafkaAuthConfig] = None,
        timeout_ms: int = -1,
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
            kafka_auth_config: Authentication configuration. See KafkaAuthConfig for details.
            timeout_ms: Timeout in milliseconds for the entire read task per partition.
                Use -1 for no timeout (run until end_offset is reached).
                The timeout applies to the whole task, not per message or per batch.
                If the task does not reach end_offset within the timeout, raises TimeoutError.

        Raises:
            ValueError: If required configuration is missing.
            ImportError: If confluent-kafka is not installed.
            TimeoutError: If the read task exceeds timeout_ms before reaching end_offset.
        """
        _check_import(self, module="confluent_kafka", package="confluent-kafka")

        if not topics:
            raise ValueError("topics cannot be empty")

        if not bootstrap_servers:
            raise ValueError("bootstrap_servers cannot be empty")

        if timeout_ms != -1 and timeout_ms <= 0:
            raise ValueError("timeout_ms must be positive, or -1 for no timeout")

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

        self._topics = topics if isinstance(topics, list) else [topics]
        self._bootstrap_servers = (
            bootstrap_servers
            if isinstance(bootstrap_servers, list)
            else [bootstrap_servers]
        )
        self._start_offset = start_offset
        self._end_offset = end_offset
        self._kafka_auth_config = kafka_auth_config
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

        consumer_config = _build_consumer_config(
            self._bootstrap_servers, self._kafka_auth_config
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
        kafka_auth_config = self._kafka_auth_config
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
                kafka_auth_config: Optional[KafkaAuthConfig] = kafka_auth_config,
                timeout_ms: Optional[int] = timeout_ms,
                target_max_block_size: int = target_max_block_size,
                consume_batch_size: int = CONSUME_BATCH_SIZE,
            ):
                """Create a Kafka read function with captured variables."""

                def kafka_read_fn() -> Iterable[Block]:
                    """Read function for a single Kafka partition using confluent-kafka."""
                    from confluent_kafka import (
                        Consumer,
                        KafkaError,
                        KafkaException,
                        TopicPartition,
                    )

                    consumer_config = _build_consumer_config(
                        bootstrap_servers, kafka_auth_config
                    )

                    consumer = Consumer(consumer_config)
                    try:
                        topic_partition = TopicPartition(topic_name, partition_id)
                        start_off, end_off = _resolve_offsets_with_consumer(
                            consumer,
                            topic_partition,
                            start_offset,
                            end_offset,
                        )

                        records = []
                        output_buffer = BlockOutputBuffer(
                            OutputBlockSizeOption.of(
                                target_max_block_size=target_max_block_size
                            )
                        )

                        if start_off < end_off:
                            messages_to_read = end_off - start_off
                            total_read = 0
                            start_time = time.time()
                            tp_with_offset = TopicPartition(
                                topic_name, partition_id, start_off
                            )
                            consumer.assign([tp_with_offset])
                            timeout_seconds = (
                                timeout_ms / 1000.0
                                if (timeout_ms is not None and timeout_ms > 0)
                                else -1.0
                            )

                            while total_read < messages_to_read:
                                elapsed_time = time.time() - start_time
                                if (
                                    timeout_seconds >= 0
                                    and elapsed_time >= timeout_seconds
                                ):
                                    positions = consumer.position([topic_partition])
                                    last_offset = (
                                        positions[0].offset - 1 if positions else -1
                                    )
                                    raise TimeoutError(
                                        f"Kafka read task timed out after {timeout_ms}ms. "
                                        f"topic={topic_name} partition={partition_id} "
                                        f"last_offset_read={last_offset} end_offset={end_off} "
                                        f"messages_returned={len(records)}"
                                    )

                                msgs = consumer.consume(
                                    num_messages=min(
                                        consume_batch_size,
                                        messages_to_read - total_read,
                                    ),
                                    timeout=KAFKA_CONSUME_TIMEOUT_S,
                                )

                                partition_done = False
                                for msg in msgs:
                                    if msg.error():
                                        if (
                                            msg.error().code()
                                            == KafkaError._PARTITION_EOF
                                        ):
                                            partition_done = True
                                            break
                                        raise KafkaException(msg.error())

                                    if msg.offset() >= end_off:
                                        partition_done = True
                                        break

                                    ts_type, ts_ms = msg.timestamp()
                                    headers_list = msg.headers() or []
                                    headers_dict = {
                                        k
                                        if isinstance(k, str)
                                        else k.decode("utf-8"): v
                                        for k, v in headers_list
                                    }
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
                                    total_read += 1
                                    if total_read >= messages_to_read:
                                        partition_done = True
                                        break

                                if partition_done:
                                    break

                                if len(records) >= BATCH_SIZE_FOR_YIELD:
                                    table = pa.Table.from_pylist(records)
                                    output_buffer.add_block(table)
                                    while output_buffer.has_next():
                                        yield output_buffer.next()
                                    records = []

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

            task = ReadTask(
                read_fn=kafka_read_fn,
                metadata=metadata,
                schema=KAFKA_MSG_SCHEMA,
                per_task_row_limit=per_task_row_limit,
            )
            tasks.append(task)

        return tasks
