"""Kafka datasink

This module provides a Kafka datasink implementation for Ray Data.

Requires:
    - confluent-kafka: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/
"""

import json
import logging
from collections.abc import Iterable, Mapping
from enum import Enum
from typing import Any, Optional

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

logger = logging.getLogger(__name__)

# Polling/flush constants.  These are intentionally conservative defaults
# that work well for typical workloads.  All three can be tuned indirectly
# through producer_config (e.g. queue.buffering.max.messages, message.timeout.ms)
# or overridden by subclassing if needed.

# Number of messages to produce before polling for delivery reports.
# At ~1.5 KB per message (a common upper bound), 10 000 messages ≈ 15 MB
# of buffered data — well within librdkafka's default queue limits while
# keeping Python→C crossing overhead low.
_POLL_BATCH_SIZE = 10000
# Timeout (seconds) for the final flush that waits for all in-flight messages
_FLUSH_TIMEOUT_S = 30
# Timeout (seconds) when polling to drain the queue after a BufferError
_BUFFER_FULL_POLL_TIMEOUT_S = 10


class SerializerFormat(str, Enum):
    """Supported serialization formats for Kafka message keys and values."""

    JSON = "json"
    STRING = "string"
    BYTES = "bytes"


def _serialize(data: Any, serializer: SerializerFormat) -> bytes:
    """Serialize *data* according to *serializer*.

    This is a standalone function so it can be used without a class instance.
    """
    if serializer == SerializerFormat.JSON:
        return json.dumps(data).encode("utf-8")
    elif serializer == SerializerFormat.STRING:
        return str(data).encode("utf-8")
    else:  # BYTES
        return data if isinstance(data, bytes) else str(data).encode("utf-8")


class KafkaDatasink(Datasink):
    """
    Ray Data sink for writing to Apache Kafka topics using confluent-kafka.

    Writes blocks of data to Kafka with configurable serialization
    and producer settings.
    """

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        key_field: Optional[str] = None,
        key_serializer: str = SerializerFormat.STRING,
        value_serializer: str = SerializerFormat.JSON,
        producer_config: Optional[dict[str, Any]] = None,
    ):
        """
        Initialize Kafka sink.

        Args:
            topic: Kafka topic name
            bootstrap_servers: Comma-separated Kafka broker addresses (e.g., 'localhost:9092')
            key_field: Optional field name to use as message key
            key_serializer: Key serialization format ('json', 'string', or 'bytes')
            value_serializer: Value serialization format ('json', 'string', or 'bytes')
            producer_config: Additional Kafka producer configuration.
                Uses confluent-kafka / librdkafka configuration keys
                (see https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
                Example: ``{"linger.ms": 5, "acks": "all"}``.
                The ``bootstrap.servers`` option is derived from ``bootstrap_servers``
                and cannot be overridden here.
        """
        _check_import(self, module="confluent_kafka", package="confluent-kafka")

        try:
            key_serializer = SerializerFormat(key_serializer)
        except ValueError:
            raise ValueError(
                f"key_serializer must be one of "
                f"{[s.value for s in SerializerFormat]}, "
                f"got '{key_serializer}'"
            )
        try:
            value_serializer = SerializerFormat(value_serializer)
        except ValueError:
            raise ValueError(
                f"value_serializer must be one of "
                f"{[s.value for s in SerializerFormat]}, "
                f"got '{value_serializer}'"
            )

        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.key_field = key_field
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.producer_config = producer_config or {}

    @staticmethod
    def _row_to_dict(row: Any) -> Any:
        """Convert a Ray data row to a plain dict if possible.

        Handles Ray's internal row types (ArrowRow, PandasRow), namedtuples,
        and generic Mappings.  Returns the input unchanged for primitives.
        """
        if isinstance(row, dict):
            return row
        if hasattr(row, "as_pydict"):
            return row.as_pydict()
        if hasattr(row, "_asdict"):
            return row._asdict()
        if isinstance(row, Mapping):
            return dict(row)
        return row

    def _serialize_value(self, value: Any) -> bytes:
        """Serialize value based on configured format."""
        return _serialize(value, self.value_serializer)

    def _serialize_key(self, key: Any) -> bytes:
        """Serialize key based on configured format."""
        return _serialize(key, self.key_serializer)

    def _extract_key(self, row_dict: Any) -> Optional[bytes]:
        """Extract and serialize the message key from a row dict.

        Returns ``None`` when no ``key_field`` is configured, when the row is
        not a dict, or when the key field is absent/``None`` in the row.  A
        ``None`` key tells the Kafka producer to use the default partitioner
        (round-robin or sticky partitioning depending on librdkafka version),
        distributing messages evenly across partitions.
        """
        if self.key_field and isinstance(row_dict, dict):
            key_value = row_dict.get(self.key_field)
            if key_value is not None:
                return self._serialize_key(key_value)
        return None

    def _produce_with_retry(self, producer, value, key, on_delivery):
        """Produce a single message, retrying once on ``BufferError``.

        ``producer.produce()`` is asynchronous — it enqueues the message into
        librdkafka's internal buffer and returns immediately.  Actual delivery
        happens in a background thread; results are reported via the
        *on_delivery* callback when ``producer.poll()`` or ``producer.flush()``
        is called.

        If the internal buffer is full, a ``BufferError`` is raised.  We handle
        this by polling to drain completed deliveries (which frees buffer
        space) and retrying once.
        """
        try:
            producer.produce(
                self.topic,
                value=value,
                key=key,
                on_delivery=on_delivery,
            )
        except BufferError:
            # Internal queue is full — poll to serve delivery callbacks and
            # free space, then retry.  The poll timeout caps how long we block
            # waiting for the broker to acknowledge in-flight messages.
            producer.poll(_BUFFER_FULL_POLL_TIMEOUT_S)
            try:
                producer.produce(
                    self.topic,
                    value=value,
                    key=key,
                    on_delivery=on_delivery,
                )
            except BufferError:
                raise RuntimeError(
                    f"Kafka producer queue is still full after "
                    f"{_BUFFER_FULL_POLL_TIMEOUT_S}s of polling "
                    f"for topic '{self.topic}'. "
                    f"Consider increasing queue.buffering.max.messages "
                    f"in producer_config."
                )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Any:
        """
        Write blocks of data to Kafka.

        Args:
            blocks: Iterable of Ray data blocks
            ctx: Ray data context

        Returns:
            Dict with ``total_records`` and ``failed_messages`` counts.
        """
        from confluent_kafka import KafkaException, Producer

        # Build confluent config
        config: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
        }
        for k, v in self.producer_config.items():
            if k == "bootstrap.servers":
                logger.warning(
                    "Ignoring 'bootstrap.servers' from producer_config; "
                    "use bootstrap_servers parameter instead."
                )
                continue
            config[k] = v

        producer = Producer(config)
        total_records = 0
        remaining = 0
        # Mutable container so on_delivery callback can update without nonlocal
        delivery_state = {"failed": 0, "first_exception": None}

        def on_delivery(err, msg):
            if err is not None:
                delivery_state["failed"] += 1
                if delivery_state["first_exception"] is None:
                    delivery_state["first_exception"] = KafkaException(err)

        try:
            for block in blocks:
                block_accessor = BlockAccessor.for_block(block)

                for row in block_accessor.iter_rows(public_row_format=False):
                    row_dict = self._row_to_dict(row)
                    key = self._extract_key(row_dict)
                    value = self._serialize_value(row_dict)

                    self._produce_with_retry(producer, value, key, on_delivery)
                    total_records += 1

                    # Periodically poll to serve delivery report callbacks
                    # and avoid unbounded internal queue growth.
                    if total_records % _POLL_BATCH_SIZE == 0:
                        producer.poll(0)

            # Final flush: blocks until all in-flight messages are delivered
            # or the timeout expires.  Returns the count of messages still
            # queued (0 = everything delivered).  Does NOT raise on timeout.
            remaining = producer.flush(timeout=_FLUSH_TIMEOUT_S)

        except KafkaException as e:
            raise RuntimeError(
                f"Failed to write to Kafka topic '{self.topic}': {e}"
            ) from e
        finally:
            producer.close()

        if remaining > 0:
            raise RuntimeError(
                f"{remaining} out of {total_records} messages were still "
                f"in-flight after flush timeout for topic '{self.topic}'. "
                f"This usually means the broker is unreachable."
            )

        failed_messages = delivery_state["failed"]
        if failed_messages > 0:
            raise RuntimeError(
                f"Failed to write {failed_messages} out of {total_records} "
                f"messages to Kafka topic '{self.topic}'."
            ) from delivery_state["first_exception"]

        # Logged once per write task (one task per data block partition).
        logger.debug(
            "Wrote %d records to Kafka topic '%s'.",
            total_records,
            self.topic,
        )

        return {"total_records": total_records, "failed_messages": failed_messages}
