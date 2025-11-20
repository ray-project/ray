"""Kafka datasink for writing Ray Datasets to Apache Kafka topics.

This module provides a Kafka datasink implementation for Ray Data that supports
distributed writes by serializing each row as JSON and sending it to Kafka.

Requires:
    - confluent-kafka: https://github.com/confluentinc/confluent-kafka-python
"""

import logging
import math
from datetime import datetime
from json import JSONEncoder, dumps
from threading import Lock
from typing import TYPE_CHECKING, Callable, Iterable, Optional

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from confluent_kafka import Producer as KafkaProducer

logger = logging.getLogger(__name__)


class CustomJSONEncoder(JSONEncoder):
    """Custom JSON encoder that handles datetime and other special types.

    This encoder extends the standard JSONEncoder to handle types that aren't
    natively JSON-serializable, such as datetime objects and special float values.
    """

    def default(self, obj):
        """Convert non-serializable objects to JSON-compatible types.

        Args:
            obj: The object to serialize.

        Returns:
            A JSON-serializable representation of the object.
        """
        # Convert datetime objects to ISO format strings
        if isinstance(obj, datetime):
            return obj.isoformat()

        # Convert NaN and Infinity to None (null in JSON)
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None

        # Convert objects with __dict__ to string representation
        if hasattr(obj, "__dict__"):
            return str(obj)

        # Fall back to default JSON encoder behavior
        return JSONEncoder.default(self, obj)


def _no_key(row: dict) -> Optional[str]:
    """Default key function that returns None for all rows.

    This is used when no key_for_row function is provided by the user.
    Messages sent without keys will be distributed across partitions
    using Kafka's default partitioner (round-robin).

    Args:
        row: The row dictionary (unused).

    Returns:
        None, indicating no key should be used.
    """
    return None


@DeveloperAPI
class KafkaDatasink(Datasink[int]):
    """Datasink for writing Ray Datasets to Apache Kafka topics.

    This datasink writes data to Kafka topics by serializing each row as JSON.
    Supports distributed writes across multiple Ray tasks.

    For confluent-kafka configuration options, see:
    https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

    Args:
        kafka_config: Dictionary of configuration options for the Kafka producer.
            These are passed directly to confluent_kafka.Producer. Common options
            include 'bootstrap.servers', 'security.protocol', 'sasl.mechanisms',
            'sasl.username', 'sasl.password', etc. Recommended: set 'message.max.bytes'
            to match your Kafka broker's max.message.bytes setting.
        topic: The Kafka topic to write messages to.
        key_for_row: Optional callable that takes a row dictionary and returns
            a string key for the Kafka message. If None, messages are sent without
            keys. Defaults to None.
        flush_timeout_seconds: Maximum time to wait for flush() to complete.
            Defaults to 30 seconds.
    """

    def __init__(
        self,
        kafka_config: dict,
        topic: str,
        key_for_row: Optional[Callable[[dict], Optional[str]]] = None,
        flush_timeout_seconds: float = 30.0,
    ) -> None:
        _check_import(self, module="confluent_kafka", package="confluent-kafka")
        if not isinstance(topic, str) or not topic:
            raise ValueError("topic must be a non-empty string")
        if not isinstance(kafka_config, dict):
            raise ValueError("kafka_config must be a dictionary")
        self.kafka_config = kafka_config.copy()
        self.topic = topic
        self.key_for_row = key_for_row if key_for_row is not None else _no_key
        self.flush_timeout_seconds = flush_timeout_seconds

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return 1

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> int:
        """Write blocks to Kafka topic.

        This method is called by a single write task and processes all blocks
        assigned to that task. Each row is serialized as JSON and sent to Kafka.

        Args:
            blocks: Iterable of data blocks to write.
            ctx: Task context for the write task.

        Returns:
            Total number of rows successfully delivered to Kafka.
        """
        from confluent_kafka import Producer as KafkaProducer

        # Track delivery status across async callbacks
        delivery_errors = []
        delivered_count = 0
        delivery_lock = Lock()

        def delivery_callback(err, msg):
            """Callback invoked by Kafka producer for each message delivery.

            This callback runs in a separate thread from the main producer thread,
            so we use a lock to safely update shared state.

            Args:
                err: Error object if delivery failed, None if successful.
                msg: Message object (unused, but required by callback signature).
            """
            nonlocal delivered_count
            if err is not None:
                with delivery_lock:
                    delivery_errors.append(err)
            else:
                with delivery_lock:
                    delivered_count += 1

        producer = KafkaProducer(self.kafka_config)
        try:
            max_message_bytes = self.kafka_config.get("message.max.bytes", 1000000)

            # Process each block
            for block in blocks:
                self._write_block(block, producer, delivery_callback, max_message_bytes)

            # Wait for all messages to be delivered
            self._wait_for_delivery(producer, delivery_lock, delivery_errors)

            return delivered_count

        finally:
            # Ensure all buffered messages are flushed, even on error
            producer.flush(0)

    def _write_block(
        self,
        block: Block,
        producer: "KafkaProducer",  # type: ignore[name-defined]
        delivery_callback: Callable,
        max_message_bytes: int,
    ) -> None:
        """Write a single block to Kafka.

        Args:
            block: The data block to write.
            producer: The Kafka producer instance.
            delivery_callback: Callback function for delivery status.
            max_message_bytes: Maximum allowed message size in bytes.
        """
        block_accessor = BlockAccessor.for_block(block)

        # Skip empty blocks
        if block_accessor.num_rows() == 0:
            return

        # Process each row in the block
        for row in block_accessor.iter_rows(public_row_format=True):
            self._write_row(row, producer, delivery_callback, max_message_bytes)

        # Poll after processing block to process any pending callbacks
        producer.poll(0.1)

    def _write_row(
        self,
        row: dict,
        producer: "KafkaProducer",  # type: ignore[name-defined]
        delivery_callback: Callable,
        max_message_bytes: int,
    ) -> None:
        """Write a single row to Kafka.

        Args:
            row: The row dictionary to write.
            producer: The Kafka producer instance.
            delivery_callback: Callback function for delivery status.
            max_message_bytes: Maximum allowed message size in bytes.

        Raises:
            ValueError: If key extraction, serialization, or message size validation fails.
        """
        # Extract and validate the message key
        key = self._extract_key(row)

        # Serialize row to JSON
        value_bytes = self._serialize_row(row, max_message_bytes)

        # Send message to Kafka (with retry on buffer full)
        self._produce_message(producer, value_bytes, key, delivery_callback)

        # Poll immediately to process callbacks and avoid buffer buildup
        producer.poll(0)

    def _extract_key(self, row: dict) -> Optional[bytes]:
        """Extract and encode the Kafka message key from a row.

        Args:
            row: The row dictionary.

        Returns:
            The encoded key as bytes, or None if no key should be used.

        Raises:
            ValueError: If the key_for_row function raises an exception.
        """
        try:
            key = self.key_for_row(row)

            # Convert non-string/bytes keys to string
            if key is not None and not isinstance(key, (str, bytes)):
                key = str(key)

            # Encode string keys to bytes
            if isinstance(key, str):
                return key.encode("utf-8")

            return key

        except Exception as e:
            raise ValueError(f"key_for_row function raised exception: {e}") from e

    def _serialize_row(self, row: dict, max_message_bytes: int) -> bytes:
        """Serialize a row dictionary to JSON bytes.

        Args:
            row: The row dictionary to serialize.
            max_message_bytes: Maximum allowed message size in bytes.

        Returns:
            The serialized row as UTF-8 encoded bytes.

        Raises:
            ValueError: If serialization fails or message exceeds size limit.
        """
        try:
            # Serialize row to JSON string
            value = dumps(row, cls=CustomJSONEncoder)
        except (TypeError, ValueError) as e:
            # Provide helpful error message with row structure
            row_keys = list(row.keys()) if isinstance(row, dict) else "N/A"
            raise ValueError(
                f"Failed to serialize row to JSON: {e}. Row keys: {row_keys}"
            ) from e

        # Encode to bytes and validate size
        value_bytes = value.encode("utf-8")
        if len(value_bytes) > max_message_bytes:
            raise ValueError(
                f"Message size {len(value_bytes)} bytes exceeds "
                f"max message size {max_message_bytes} bytes. "
                f"Consider increasing 'message.max.bytes' in kafka_config."
            )

        return value_bytes

    def _produce_message(
        self,
        producer: "KafkaProducer",  # type: ignore[name-defined]
        value_bytes: bytes,
        key: Optional[bytes],
        delivery_callback: Callable,
    ) -> None:
        """Produce a single message to Kafka with buffer error handling.

        If the producer's internal buffer is full, this method will retry
        after polling to make room. This provides backpressure handling.

        Args:
            producer: The Kafka producer instance.
            value_bytes: The message value as bytes.
            key: The message key as bytes, or None.
            delivery_callback: Callback function for delivery status.
        """
        while True:
            try:
                producer.produce(
                    topic=self.topic,
                    value=value_bytes,
                    key=key,
                    callback=delivery_callback,
                )
                # Successfully queued the message
                break
            except BufferError:
                # Buffer is full - poll to process pending messages and retry
                producer.poll(0.1)

    def _wait_for_delivery(
        self,
        producer: "KafkaProducer",  # type: ignore[name-defined]
        delivery_lock: Lock,
        delivery_errors: list,
    ) -> None:
        """Wait for all messages to be delivered and check for errors.

        This method flushes the producer and verifies that all messages
        were successfully delivered. If any delivery errors occurred,
        it raises a RuntimeError with details.

        Args:
            producer: The Kafka producer instance.
            delivery_lock: Lock protecting shared delivery state.
            delivery_errors: List to collect delivery errors.

        Raises:
            RuntimeError: If flush times out or delivery errors occurred.
        """
        # Flush all pending messages with timeout
        remaining = producer.flush(self.flush_timeout_seconds)
        if remaining > 0:
            raise RuntimeError(
                f"Failed to flush {remaining} messages within "
                f"{self.flush_timeout_seconds} seconds. "
                f"Kafka may be unavailable or overloaded."
            )

        # Check for delivery errors reported by callbacks
        with delivery_lock:
            if delivery_errors:
                # Format error message with first 5 errors
                error_msg = "; ".join(str(err) for err in delivery_errors[:5])
                if len(delivery_errors) > 5:
                    error_msg += f" ... and {len(delivery_errors) - 5} more errors"
                raise RuntimeError(
                    f"Failed to deliver {len(delivery_errors)} messages to Kafka: {error_msg}"
                )
