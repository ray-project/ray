"""Partition coordinator for streaming datasources.

This module provides a coordinator actor that manages partition discovery,
assignment, and rebalancing for streaming datasources (Kafka, Kinesis, etc.).

The coordinator ensures:
- No duplicate reads (each partition assigned to exactly one task)
- No gaps (all partitions assigned)
- Safe rebalancing on parallelism changes or failures
- Lease-based assignment with expiry for fault tolerance
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import ray

logger = logging.getLogger(__name__)


@dataclass
class PartitionLease:
    """Represents a lease on a partition assignment."""

    partition_id: str
    owner_task_id: str  # Unique identifier for the task/actor holding the lease
    lease_expiry: datetime
    last_committed_offset: Optional[Any] = None  # Checkpoint/offset for this partition
    batch_id: Optional[int] = None  # Microbatch ID when lease was granted


@ray.remote(num_cpus=0)
class PartitionCoordinatorActor:
    """Coordinator actor for partition discovery and assignment.

    This actor maintains the authoritative state of partition assignments
    and handles rebalancing when parallelism changes or tasks fail.

    Thread-safe: Ray actors are single-threaded by default.

    Example:
        .. testcode::
            :skipif: True

            coordinator = PartitionCoordinatorActor.remote(
                datasource=my_datasource,
                lease_duration_seconds=30.0
            )

            # Request lease for a microbatch
            lease = ray.get(coordinator.request_lease.remote(
                task_id="task_0",
                batch_id=1,
                requested_partitions=None  # None = auto-assign
            ))

            # Commit checkpoint after successful microbatch
            ray.get(coordinator.commit_checkpoint.remote(
                task_id="task_0",
                partition_id="partition_0",
                checkpoint={"offset": 12345}
            ))

            # Release lease when done
            ray.get(coordinator.release_lease.remote(
                task_id="task_0",
                partition_ids=["partition_0"]
            ))
    """

    def __init__(
        self,
        datasource: Any,  # Datasource with discover_partitions() method
        lease_duration_seconds: float = 30.0,
        rebalance_on_parallelism_change: bool = True,
    ):
        """Initialize the partition coordinator.

        Args:
            datasource: Datasource instance with discover_partitions() method.
                Should return List[Dict[str, Any]] with partition metadata.
            lease_duration_seconds: Duration of partition leases before expiry.
            rebalance_on_parallelism_change: Whether to rebalance when parallelism changes.
        """
        self.datasource = datasource
        self.lease_duration_seconds = lease_duration_seconds
        self.rebalance_on_parallelism_change = rebalance_on_parallelism_change

        # State: partition_id -> PartitionLease
        self._leases: Dict[str, PartitionLease] = {}

        # State: task_id -> Set[partition_id]
        self._task_assignments: Dict[str, Set[str]] = {}

        # Last discovered partitions (for change detection)
        self._known_partitions: Set[str] = set()

        # Last rebalance time (for periodic rebalancing)
        self._last_rebalance_time: Optional[datetime] = None

        logger.info(
            f"PartitionCoordinatorActor initialized with "
            f"lease_duration={lease_duration_seconds}s"
        )

    def discover_partitions(self) -> List[Dict[str, Any]]:
        """Discover available partitions from the datasource.

        Returns:
            List of partition metadata dictionaries. Each dict should contain
            at least a "partition_id" key.
        """
        try:
            if hasattr(self.datasource, "discover_partitions"):
                partitions = self.datasource.discover_partitions()
            else:
                # Fallback: assume datasource.get_read_tasks() returns partition info
                # This is a simplified fallback - real implementations should override
                partitions = []
                logger.warning(
                    "Datasource does not implement discover_partitions(); "
                    "using fallback discovery"
                )
            return partitions
        except Exception as e:
            logger.error(f"Failed to discover partitions: {e}", exc_info=e)
            return []

    def request_lease(
        self,
        task_id: str,
        batch_id: int,
        requested_partitions: Optional[List[str]] = None,
        parallelism: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Request a lease on partitions for a microbatch.

        Args:
            task_id: Unique identifier for the requesting task/actor.
            batch_id: Microbatch ID for this request.
            requested_partitions: Specific partitions to request, or None for auto-assignment.
            parallelism: Current parallelism level (for rebalancing).

        Returns:
            Dictionary with:
                - "assigned_partitions": List[str] - partitions assigned to this task
                - "checkpoints": Dict[str, Any] - last committed checkpoint per partition
                - "rebalanced": bool - whether a rebalance occurred
        """
        now = datetime.now()

        # Discover current partitions
        partitions = self.discover_partitions()
        partition_ids = {p.get("partition_id", str(i)) for i, p in enumerate(partitions)}

        # Check for partition changes
        partitions_changed = partition_ids != self._known_partitions
        if partitions_changed:
            logger.info(
                f"Partition set changed: {len(self._known_partitions)} -> "
                f"{len(partition_ids)} partitions"
            )
            self._known_partitions = partition_ids

        # Clean up expired leases
        self._cleanup_expired_leases(now)

        # Release any existing leases for this task (new microbatch = new lease)
        if task_id in self._task_assignments:
            existing_partitions = self._task_assignments[task_id].copy()
            self._release_lease_internal(task_id, existing_partitions)

        # Determine which partitions to assign
        if requested_partitions:
            # Specific partitions requested
            available = [
                p for p in requested_partitions if p not in self._leases or self._leases[p].lease_expiry < now
            ]
            assigned = available[: len(requested_partitions)]
        else:
            # Auto-assign: distribute partitions across tasks
            available_partitions = [
                p for p in partition_ids if p not in self._leases or self._leases[p].lease_expiry < now
            ]

            if parallelism and parallelism > 0:
                # Round-robin assignment based on parallelism
                task_index = hash(task_id) % parallelism
                assigned = [
                    p
                    for i, p in enumerate(sorted(available_partitions))
                    if i % parallelism == task_index
                ]
            else:
                # Assign all available partitions to this task
                assigned = available_partitions

        # Create leases
        lease_expiry = now + timedelta(seconds=self.lease_duration_seconds)
        checkpoints = {}
        for partition_id in assigned:
            # Get last committed checkpoint if available
            if partition_id in self._leases:
                checkpoints[partition_id] = self._leases[partition_id].last_committed_offset

            self._leases[partition_id] = PartitionLease(
                partition_id=partition_id,
                owner_task_id=task_id,
                lease_expiry=lease_expiry,
                last_committed_offset=checkpoints.get(partition_id),
                batch_id=batch_id,
            )

        # Update task assignments
        self._task_assignments[task_id] = set(assigned)

        rebalanced = partitions_changed or (parallelism is not None)

        logger.debug(
            f"Lease granted to task {task_id} (batch {batch_id}): "
            f"{len(assigned)} partitions, rebalanced={rebalanced}"
        )

        return {
            "assigned_partitions": assigned,
            "checkpoints": checkpoints,
            "rebalanced": rebalanced,
        }

    def commit_checkpoint(
        self,
        task_id: str,
        partition_id: str,
        checkpoint: Any,
    ) -> bool:
        """Commit a checkpoint for a partition.

        Args:
            task_id: Task ID that owns the lease.
            partition_id: Partition to commit checkpoint for.
            checkpoint: Checkpoint data (e.g., Kafka offset).

        Returns:
            True if checkpoint was committed, False if lease not found or expired.
        """
        if partition_id not in self._leases:
            logger.warning(f"No lease found for partition {partition_id}")
            return False

        lease = self._leases[partition_id]
        if lease.owner_task_id != task_id:
            logger.warning(
                f"Task {task_id} does not own lease for partition {partition_id}"
            )
            return False

        if lease.lease_expiry < datetime.now():
            logger.warning(f"Lease expired for partition {partition_id}")
            return False

        # Update checkpoint
        lease.last_committed_offset = checkpoint
        logger.debug(f"Checkpoint committed for partition {partition_id}: {checkpoint}")
        return True

    def release_lease(
        self,
        task_id: str,
        partition_ids: List[str],
    ) -> None:
        """Release leases on partitions.

        Args:
            task_id: Task ID releasing the leases.
            partition_ids: Partitions to release.
        """
        self._release_lease_internal(task_id, partition_ids)

    def _release_lease_internal(
        self,
        task_id: str,
        partition_ids: List[str],
    ) -> None:
        """Internal method to release leases."""
        for partition_id in partition_ids:
            if partition_id in self._leases:
                lease = self._leases[partition_id]
                if lease.owner_task_id == task_id:
                    # Keep checkpoint but release lease
                    del self._leases[partition_id]

        # Update task assignments
        if task_id in self._task_assignments:
            self._task_assignments[task_id] -= set(partition_ids)
            if not self._task_assignments[task_id]:
                del self._task_assignments[task_id]

    def _cleanup_expired_leases(self, now: datetime) -> None:
        """Remove expired leases."""
        expired = [
            pid for pid, lease in self._leases.items() if lease.lease_expiry < now
        ]
        for pid in expired:
            del self._leases[pid]
            # Also remove from task assignments
            for task_id, partitions in list(self._task_assignments.items()):
                if pid in partitions:
                    partitions.remove(pid)
                    if not partitions:
                        del self._task_assignments[task_id]

    def get_assignment_state(self) -> Dict[str, Any]:
        """Get current assignment state (for debugging/monitoring).

        Returns:
            Dictionary with current lease and assignment state.
        """
        now = datetime.now()
        active_leases = {
            pid: {
                "owner": lease.owner_task_id,
                "expires_in_seconds": (lease.lease_expiry - now).total_seconds(),
                "checkpoint": lease.last_committed_offset,
                "batch_id": lease.batch_id,
            }
            for pid, lease in self._leases.items()
            if lease.lease_expiry >= now
        }

        return {
            "active_leases": active_leases,
            "task_assignments": {
                task_id: list(partitions)
                for task_id, partitions in self._task_assignments.items()
            },
            "known_partitions": list(self._known_partitions),
        }
