"""Unbounded datasource base class and utilities for Ray Data.

This module provides the foundation for streaming data sources that represent
unbounded data streams (Kafka, Kinesis, Flink, etc.). This is distinct from
Ray Data's "streaming execution" engine - it handles ingestion of unbounded data.

The UnboundDatasource base class provides a simple interface that subclasses
must implement to create streaming datasources.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pyarrow as pa

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import PublicAPI

Checkpoint = Dict[str, Any]


@dataclass(frozen=True)
class TriggerBudget:
    """Budget that bounds a single microbatch read.

    All unbounded datasource ReadTasks MUST terminate within these bounds.
    """

    max_records: Optional[int] = None
    max_bytes: Optional[int] = None
    max_splits: Optional[int] = None
    deadline_s: Optional[float] = None


@PublicAPI(stability="alpha")
class UnboundDatasource(Datasource, ABC):
    """Abstract base class for unbounded (streaming) data sources.

    This class provides the foundation for streaming datasources that represent
    unbounded data streams. Subclasses must implement `_get_read_tasks_for_partition`
    to define how to create read tasks for their specific data source.

    Production streaming sources SHOULD override get_read_tasks() directly and
    use TriggerBudget + checkpoint + trigger + batch_id.

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

    # ---- Optional lifecycle/capabilities -----------------------------------------
    def initial_checkpoint(self) -> Optional[Checkpoint]:
        """Initial checkpoint for the stream, if any (source-specific)."""
        return None

    def supports_exactly_once(self) -> bool:
        return False

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

    # NOTE: Datasource.get_read_tasks signature is used for bounded reads. For unbounded,
    # we provide a richer signature while remaining compatible with existing call sites.
    def get_read_tasks(  # type: ignore[override]
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[Any] = None,
        *,
        checkpoint: Optional[Checkpoint] = None,
        trigger: Optional[Any] = None,
        batch_id: Optional[int] = None,
        budget: Optional[TriggerBudget] = None,
        **kwargs: Any,
    ) -> Union[List[ReadTask], Tuple[List[ReadTask], Optional[Checkpoint]]]:
        """Plan a single microbatch and return (read_tasks, planned_next_checkpoint).

        Args:
            parallelism: Desired parallelism level.
            per_task_row_limit: Optional limit on rows per task (unused for unbounded).
            data_context: Optional DataContext (for backward compatibility with base class).
            checkpoint: Last committed checkpoint (source-specific, keyword-only).
            trigger: StreamingTrigger (driver-side, keyword-only).
            batch_id: Microbatch id (monotonic, driver-side, keyword-only).
            budget: Budget for this microbatch. ReadTasks MUST terminate within it (keyword-only).

        Returns:
            List[ReadTask] for backward compatibility when called without streaming args,
            or Tuple[List[ReadTask], Optional[Checkpoint]] when called with streaming args.

        Default implementation delegates to _get_read_tasks_for_partition for backward
        compatibility and returns List[ReadTask] (not tuple) when no streaming args provided.
        """
        # Backward compatibility: if called without streaming args, return List only
        if checkpoint is None and trigger is None and batch_id is None and budget is None:
            tasks = self._get_read_tasks_for_partition({}, parallelism)
            return tasks

        # Streaming mode: return tuple
        tasks = self._get_read_tasks_for_partition({}, parallelism)
        return tasks, None

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
    "TriggerBudget",
    "Checkpoint",
]
