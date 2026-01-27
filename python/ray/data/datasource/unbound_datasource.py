"""Unbounded datasource base class and utilities for Ray Data.

This module provides the foundation for streaming data sources that represent
unbounded data streams (Kafka, Kinesis, Flink, etc.). This is distinct from
Ray Data's "streaming execution" engine - it handles ingestion of unbounded data.

The UnboundDatasource base class provides a simple interface that subclasses
must implement to create streaming datasources.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, List, Optional

import pyarrow as pa

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class UnboundDatasource(Datasource, ABC):
    """Abstract base class for unbounded (streaming) data sources.

    This class provides the foundation for streaming datasources that represent
    unbounded data streams. Subclasses must implement `_get_read_tasks_for_partition`
    to define how to create read tasks for their specific data source.

    Example:
        .. testcode::
            :skipif: True

            from ray.data.datasource.unbound_datasource import (
                UnboundDatasource,
                create_unbound_read_task,
            )
            from ray.data.block import BlockMetadata

            class MyDatasource(UnboundDatasource):
                def __init__(self):
                    super().__init__("my_source")

                def _get_read_tasks_for_partition(self, partition_info, parallelism):
                    def read_fn():
                        yield []  # Yield data blocks

                    metadata = BlockMetadata(
                        num_rows=None, size_bytes=None,
                        input_files=None, exec_stats=None
                    )
                    return [create_unbound_read_task(read_fn, metadata)]

            ds = MyDatasource()
            tasks = ds.get_read_tasks(parallelism=2)
    """

    def __init__(self, source_type: str):
        """Initialize unbounded datasource.

        Args:
            source_type: Type of source (e.g., "kafka", "kinesis", "flink").
        """
        self._source_type = source_type

    @abstractmethod
    def _get_read_tasks_for_partition(
        self,
        partition_info: Dict[str, Any],
        parallelism: int,
    ) -> List[ReadTask]:
        """Create read tasks for data partitions.

        Subclasses must implement this to define how to create read tasks
        for their specific data source.

        Args:
            partition_info: Source-specific partition information.
            parallelism: Desired number of parallel tasks.

        Returns:
            List of ReadTask objects.
        """
        pass

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        """Get read tasks for this datasource.

        Args:
            parallelism: Desired parallelism level.
            per_task_row_limit: Optional limit on rows per task (unused for unbounded).

        Returns:
            List of read tasks.
        """
        return self._get_read_tasks_for_partition({}, parallelism)

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory data size.

        Returns:
            None for unbounded sources (size unknown).
        """
        return None

    def get_name(self) -> str:
        """Get name of this datasource.

        Returns:
            Datasource name.
        """
        return self._source_type

    @property
    def supports_distributed_reads(self) -> bool:
        """Whether this datasource supports distributed reads.

        Returns:
            True - unbounded datasources support distributed reads.
        """
        return True


@PublicAPI(stability="alpha")
def create_unbound_read_task(
    read_fn: Callable[[], Iterable[Block]],
    metadata: BlockMetadata,
    schema: Optional[pa.Schema] = None,
) -> ReadTask:
    """Create a ReadTask for unbounded datasources.

    Args:
        read_fn: Function that yields blocks (PyArrow tables).
        metadata: Block metadata (estimated).
        schema: Optional PyArrow schema.

    Returns:
        ReadTask configured for unbounded reading.
    """
    return ReadTask(read_fn=read_fn, metadata=metadata, schema=schema)


__all__ = [
    "UnboundDatasource",
    "create_unbound_read_task",
]
