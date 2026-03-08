"""Kafka datasink

This module provides a Kafka datasink implementation for Ray Data.

Requires:
    - confluent-kafka: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/
"""

import json
import logging
from collections.abc import Iterable, Mapping
from typing import Any, Optional

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

logger = logging.getLogger(__name__)

# Number of messages to produce before polling for delivery reports
_POLL_BATCH_SIZE = 10000
# Timeout (seconds) for the final flush that waits for all in-flight messages
_FLUSH_TIMEOUT_S = 30
# Timeout (seconds) when polling to drain the queue after a BufferError
_BUFFER_FULL_POLL_TIMEOUT_S = 10


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
        key_serializer: str = "string",
        value_serializer: str = "json",
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
            producer_config: Additional Kafka producer configuration
                (confluent-kafka/librdkafka format, e.g., ``{"linger.ms": 5}``).
                The ``bootstrap.servers`` option is derived from ``bootstrap_servers``
                and cannot be overridden here.
        """
        _check_import(self, module="confluent_kafka", package="confluent-kafka")

        VALID_SERIALIZERS = {"json", "string", "bytes"}
        if key_serializer not in VALID_SERIALIZERS:
            raise ValueError(
                f"key_serializer must be one of {VALID_SERIALIZERS}, "
                f"got '{key_serializer}'"
            )
        if value_serializer not in VALID_SERIALIZERS:
            raise ValueError(
                f"value_serializer must be one of {VALID_SERIALIZERS}, "
                f"got '{value_serializer}'"
            )

        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.key_field = key_field
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.producer_config = producer_config or {}

    def _row_to_dict(self, row: Any) -> Any:
        """Convert row to dict if possible, otherwise return as-is."""
        # 1. Fast path for standard dicts
        if isinstance(row, dict):
            return row

        # 2. Ray's ArrowRow/PandasRow (and other Mappings)
        # They usually implement .as_pydict() for efficient conversion
        if hasattr(row, "as_pydict"):
            return row.as_pydict()

        # 3. Standard NamedTuple (no __dict__, but has _asdict)
        if hasattr(row, "_asdict"):
            return row._asdict()

        # 4. General Mapping fallback (e.g. other dict-likes)
        if isinstance(row, Mapping):
            return dict(row)

        # 5. Fallback: return as-is (e.g. primitives, strings, bytes)
        return row

    def _serialize_value(self, value: Any) -> bytes:
        """Serialize value based on configured format."""
        if self.value_serializer == "json":
            return json.dumps(value).encode("utf-8")
        elif self.value_serializer == "string":
            return str(value).encode("utf-8")
        else:  # bytes
            return value if isinstance(value, bytes) else str(value).encode("utf-8")

    def _serialize_key(self, key: Any) -> bytes:
        """Serialize key based on configured format."""
        if self.key_serializer == "json":
            return json.dumps(key).encode("utf-8")
        elif self.key_serializer == "string":
            return str(key).encode("utf-8")
        else:  # bytes
            return key if isinstance(key, bytes) else str(key).encode("utf-8")

    def _extract_key(self, row_dict: Any) -> Optional[bytes]:
        """Extract and encode message key from row dict."""
        key = None
        if self.key_field and isinstance(row_dict, dict):
            key_value = row_dict.get(self.key_field)
            if key_value is not None:
                key = self._serialize_key(key_value)
        return key

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

                # Iterate through rows in block
                for row in block_accessor.iter_rows(public_row_format=False):
                    # Convert row to dict once
                    row_dict = self._row_to_dict(row)

                    # Extract key if specified
                    key = self._extract_key(row_dict)

                    # Serialize value
                    value = self._serialize_value(row_dict)

                    # Produce to Kafka
                    # TODO: Consider using produce_batch() to reduce per-message Python->C overhead.
                    try:
                        producer.produce(
                            self.topic,
                            value=value,
                            key=key,
                            on_delivery=on_delivery,
                        )
                    except BufferError:
                        # Internal queue is full, poll to serve callbacks
                        # and free space, then retry
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

                    total_records += 1

                    # Periodically poll to serve delivery report callbacks
                    # and avoid unbounded internal queue growth
                    if total_records % _POLL_BATCH_SIZE == 0:
                        producer.poll(0)

            # Final flush to ensure all remaining messages are delivered.
            # flush() blocks until all in-flight messages are delivered or the
            # timeout is reached.  It returns the number of messages still in
            # the internal queue (0 means everything was delivered).  It does
            # NOT raise on timeout — we check `remaining` below instead.
            remaining = producer.flush(timeout=_FLUSH_TIMEOUT_S)

        except KafkaException as e:
            raise RuntimeError(f"Failed to write to Kafka: {e}") from e
        finally:
            producer.close()

        # messages stuck in-flight after flush timeout
        if remaining > 0:
            raise RuntimeError(
                f"{remaining} out of {total_records} messages were still "
                f"in-flight after flush timeout for topic '{self.topic}'. "
                f"This usually means the broker is unreachable."
            )

        # Delivery failures: fail the task so Ray can retry
        failed_messages = delivery_state["failed"]
        if failed_messages > 0:
            raise RuntimeError(
                f"Failed to write {failed_messages} out of {total_records} "
                f"messages to Kafka topic '{self.topic}'."
            ) from delivery_state["first_exception"]

        logger.info(
            "Wrote %d records to Kafka topic '%s'.",
            total_records,
            self.topic,
        )

        return {"total_records": total_records, "failed_messages": failed_messages}
