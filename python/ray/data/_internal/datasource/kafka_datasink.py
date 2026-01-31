"""Kafka datasink

This module provides a Kafka datasink implementation for Ray Data.

Requires:
    - kafka-python: https://kafka-python.readthedocs.io/
"""

import json
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError, KafkaTimeoutError

from ray.data import Datasink
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor


class KafkaDatasink(Datasink):
    """
    Ray Data sink for writing to Apache Kafka topics using kafka-python.

    Writes blocks of data to Kafka with configurable serialization
    and producer settings.
    """

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        key_field: str | None = None,
        value_serializer: str = "json",
        producer_config: dict[str, Any] | None = None,
        batch_size: int = 100,
        delivery_callback: Callable | None = None,
    ):
        """
        Initialize Kafka sink.

        Args:
            topic: Kafka topic name
            bootstrap_servers: Comma-separated Kafka broker addresses (e.g., 'localhost:9092')
            key_field: Optional field name to use as message key
            value_serializer: Serialization format ('json', 'string', or 'bytes')
            producer_config: Additional Kafka producer configuration (kafka-python format)
            batch_size: Number of records to batch before flushing
            delivery_callback: Optional callback for delivery reports (called with metadata or exception)
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.key_field = key_field
        self.value_serializer = value_serializer
        self.producer_config = producer_config or {}
        self.batch_size = batch_size
        self.delivery_callback = delivery_callback

    def _row_to_dict(self, row: Any) -> Any:
        """Convert row to dict if it's an ArrowRow, otherwise return as-is."""
        # Handle ArrowRow objects from Ray Data
        if hasattr(row, "as_pydict"):
            return row.as_pydict()
        elif hasattr(row, "__dict__") and hasattr(row, "_fields"):
            # Handle named tuples or similar
            return dict(row._asdict()) if hasattr(row, "_asdict") else row
        else:
            return row

    def _serialize_value(self, value: Any) -> bytes:
        """Serialize value based on configured format."""
        # Convert ArrowRow to dict first
        value = self._row_to_dict(value)

        if self.value_serializer == "json":
            return json.dumps(value).encode("utf-8")
        elif self.value_serializer == "string":
            return str(value).encode("utf-8")
        else:  # bytes
            return value if isinstance(value, bytes) else str(value).encode("utf-8")

    def _extract_key(self, row: Any) -> bytes | None:
        """Extract and encode message key from row."""
        # Convert ArrowRow to dict first
        row_dict = self._row_to_dict(row)

        key = None
        if self.key_field and isinstance(row_dict, dict):
            key_value = row_dict.get(self.key_field)
            if key_value is not None:
                key = str(key_value).encode("utf-8")
        return key

    def _default_delivery_callback(
        self, metadata: Any = None, exception: Exception | None = None
    ) -> None:
        """Default delivery report callback."""
        if exception is not None:
            raise KafkaError(f"Message delivery failed: {exception}")

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
            Write statistics (total records written)
        """
        # Create producer with config
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            **self.producer_config,
        )
        total_records = 0
        batch_count = 0
        failed_messages = 0
        futures = []

        # Use provided callback or default
        callback = self.delivery_callback or self._default_delivery_callback

        try:
            for block in blocks:
                block_accessor = BlockAccessor.for_block(block)

                # Iterate through rows in block
                for row in block_accessor.iter_rows(public_row_format=False):
                    # Extract key if specified
                    key = self._extract_key(row)

                    # Serialize value
                    value = self._serialize_value(row)

                    # Produce to Kafka
                    try:
                        future = producer.send(self.topic, value=value, key=key)
                        # Add callback if provided
                        future.add_callback(lambda m: callback(metadata=m))
                        future.add_errback(lambda e: callback(exception=e))
                        futures.append(future)
                        total_records += 1
                        batch_count += 1

                    except BufferError:
                        # Queue is full, flush and retry
                        producer.flush(timeout=1.0)
                        future = producer.send(self.topic, value=value, key=key)
                        future.add_callback(lambda m: callback(metadata=m))
                        future.add_errback(lambda e: callback(exception=e))
                        futures.append(future)
                        total_records += 1
                        batch_count += 1

                    # Flush periodically to manage memory
                    if batch_count >= self.batch_size:
                        producer.flush(timeout=10.0)
                        batch_count = 0

            # Final flush to ensure all messages are sent
            producer.flush(timeout=30.0)

            # Check for any failed futures
            for future in futures:
                try:
                    future.get(timeout=0)  # Non-blocking check since we already flushed
                except Exception:
                    failed_messages += 1

        except KafkaTimeoutError as e:
            raise RuntimeError(f"Failed to write to Kafka: {e}") from e
        except KafkaError as e:
            raise RuntimeError(f"Failed to write to Kafka: {e}") from e
        finally:
            # Close the producer
            producer.close(timeout=5.0)

        return {"total_records": total_records, "failed_messages": failed_messages}
