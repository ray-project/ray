"""Unbound read task creation utilities.

This module provides utilities for creating read tasks for unbound datasources.
"""

import time
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, Iterator, Optional

import pyarrow as pa

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import ReadTask


def create_unbound_read_task(
    partition_id: str,
    unbound_config: Dict[str, Any],
    read_source_fn: Callable[[], Iterator[Dict[str, Any]]],
    get_position_fn: Callable[[], str],
    get_schema_fn: Callable[[], pa.Schema],
    start_position: Optional[str] = None,
    end_position: Optional[str] = None,
    max_records: Optional[int] = None,
    enrichment_fn: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
) -> ReadTask:
    """Factory function to create unbound read tasks.

    Args:
        partition_id: Unique identifier for partition.
        unbound_config: Source-specific configuration.
        read_source_fn: Function that reads from source and yields records.
        get_position_fn: Function that returns current position.
        get_schema_fn: Function that returns the schema.
        start_position: Starting position.
        end_position: Ending position.
        max_records: Maximum records per read.
        enrichment_fn: Optional function to enrich records with metadata.

    Returns:
        ReadTask configured for unbound reading.
    """

    def read_fn() -> Iterable[Block]:
        """Read function that converts source records to blocks."""
        # Get Ray Data context for retry configuration
        from ray.data.context import DataContext

        data_context = DataContext.get_current()
        max_retries = getattr(data_context, "unbound_max_retries", 3)
        retry_delay = getattr(data_context, "unbound_retry_delay", 1.0)

        # Use context default if max_records not specified
        local_max_records = max_records or getattr(
            data_context, "unbound_batch_size", 1000
        )

        records = []
        records_read = 0
        retry_count = 0

        while retry_count <= max_retries:
            try:
                for record in read_source_fn():
                    if end_position and _should_stop_reading(record, end_position):
                        break

                    if records_read >= local_max_records:
                        break

                    # Enrich record with metadata
                    enriched_record = record.copy()
                    enriched_record.update(
                        {
                            "partition_id": partition_id,
                            "read_timestamp": datetime.now(),
                            "current_position": get_position_fn(),
                        }
                    )

                    # Apply custom enrichment if provided
                    if enrichment_fn:
                        enriched_record = enrichment_fn(enriched_record)

                    records.append(enriched_record)
                    records_read += 1

                # Convert to PyArrow table
                if records:
                    table = pa.Table.from_pylist(records)
                    yield table
                else:
                    # Yield empty table with correct schema
                    schema = get_schema_fn()
                    yield pa.Table.from_arrays([[] for _ in schema], schema=schema)

                # Success - break out of retry loop
                break

            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise RuntimeError(f"Unbound read error: {e}") from e

    # Create metadata for this task
    estimated_rows = max_records if max_records is not None else None
    input_source = unbound_config.get("source_identifier", partition_id)

    metadata = BlockMetadata(
        num_rows=estimated_rows,
        size_bytes=None,
        input_files=[input_source] if input_source else None,
        exec_stats=None,
    )

    return ReadTask(read_fn, metadata, schema=get_schema_fn())


def _should_stop_reading(record: Dict[str, Any], end_position: str) -> bool:
    """Check if reading should stop based on end position."""
    # Default implementation - subclasses should override for source-specific logic
    return False
