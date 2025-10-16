"""Base class for unbounded datasources with state management.

This module provides the base interface for streaming datasources that need
to track reading positions (offsets, sequence numbers, etc.) for fault tolerance.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)


@dataclass
class LagMetrics:
    """Metrics about data lag for autoscaling decisions.

    Attributes:
        total_lag: Total number of records/messages waiting to be read
        capacity: Current processing capacity (records/sec)
        partitions: Number of partitions/shards available
        lag_per_partition: Dictionary mapping partition ID to lag
    """

    total_lag: int
    capacity: float
    partitions: int
    lag_per_partition: Dict[str, int]


class UnboundDatasource(Datasource):
    """Base class for unbounded (streaming) datasources.

    Extends Datasource with state management capabilities for tracking
    reading positions across restarts.
    """

    def __init__(self, name: str):
        """Initialize unbounded datasource.

        Args:
            name: Name of the datasource (e.g., "kafka", "kinesis")
        """
        self._name = name

    def get_name(self) -> str:
        """Get the name of this datasource."""
        return self._name

    def get_state(self) -> Dict[str, Any]:
        """Get current reading state for checkpointing.

        Returns a dictionary containing datasource-specific reading positions
        (e.g., Kafka offsets, Kinesis sequence numbers).

        This state can be persisted and later restored via set_state() to
        resume reading from the same position after a restart.

        Returns:
            Dictionary with datasource state. Empty dict if no state to save.

        Example:
            For Kafka: {"topic-0": 12345, "topic-1": 67890}
            For Kinesis: {"shard-0": "seq-123", "shard-1": "seq-456"}
        """
        return {}

    def set_state(self, state: Dict[str, Any]) -> None:
        """Restore reading state from checkpoint.

        Args:
            state: Previously saved state dict from get_state()

        Raises:
            ValueError: If state is invalid or incompatible
        """
        pass

    def is_healthy(self) -> bool:
        """Check if datasource is healthy and accessible.

        Returns:
            True if datasource can be accessed, False otherwise
        """
        return True

    def reconnect(self) -> None:
        """Attempt to reconnect to datasource after connection failure.

        Raises:
            Exception: If reconnection fails
        """
        pass

    def get_lag_metrics(self) -> Optional[LagMetrics]:
        """Get current lag metrics for autoscaling decisions.

        Returns lag information to determine if more/fewer parallel tasks are needed.
        For example, Kafka can report how many messages are waiting across partitions.

        Returns:
            LagMetrics object with lag information, or None if not supported

        Example:
            For Kafka: total_lag = sum of unconsumed messages across all partitions
            For Kinesis: total_lag = records behind latest across all shards
        """
        return None

    def _get_read_tasks_for_partition(
        self,
        partition_info: Dict[str, Any],
        parallelism: int,
    ) -> List[ReadTask]:
        """Create read tasks for a specific partition.

        Subclasses should implement this method to create read tasks.

        Args:
            partition_info: Information about the partition to read
            parallelism: Number of parallel tasks to create

        Returns:
            List of ReadTask objects
        """
        raise NotImplementedError

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        """Get read tasks for this datasource.

        Args:
            parallelism: Number of parallel read tasks
            per_task_row_limit: Optional limit on rows per task

        Returns:
            List of ReadTask objects
        """
        # Default implementation delegates to partition-specific method
        return self._get_read_tasks_for_partition({}, parallelism)


def create_unbound_read_task(
    read_fn,
    metadata: BlockMetadata,
    schema=None,
) -> ReadTask:
    """Create a ReadTask for unbounded data sources.

    Args:
        read_fn: Function that yields blocks (PyArrow tables)
        metadata: Block metadata (estimated)
        schema: Optional PyArrow schema

    Returns:
        ReadTask configured for streaming
    """
    return ReadTask(read_fn=read_fn, metadata=metadata, schema=schema)

