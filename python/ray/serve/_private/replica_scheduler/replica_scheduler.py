from abc import ABC, abstractmethod
from typing import Dict, List

from ray.serve._private.common import ReplicaID, RunningReplicaInfo
from ray.serve._private.replica_scheduler.common import (
    PendingRequest,
    ReplicaQueueLengthCache,
)
from ray.serve._private.replica_scheduler.replica_wrapper import ReplicaWrapper


class ReplicaScheduler(ABC):
    """Abstract interface for a replica scheduler (how the router calls it)."""

    @abstractmethod
    async def choose_replica_for_request(
        self, pending_request: PendingRequest, *, is_retry: bool = False
    ) -> ReplicaWrapper:
        pass

    @abstractmethod
    def create_replica_wrapper(
        self, replica_info: RunningReplicaInfo
    ) -> ReplicaWrapper:
        pass

    @abstractmethod
    def update_replicas(self, replicas: List[ReplicaWrapper]):
        pass

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        """Compatibility shim for RunningReplicaInfo datatype."""
        return self.update_replicas(
            [self.create_replica_wrapper(r) for r in running_replicas]
        )

    @abstractmethod
    def on_replica_actor_died(self, replica_id: ReplicaID):
        pass

    @abstractmethod
    def on_replica_actor_unavailable(self, replica_id: ReplicaID):
        pass

    @property
    @abstractmethod
    def replica_queue_len_cache(self) -> ReplicaQueueLengthCache:
        pass

    @property
    @abstractmethod
    def curr_replicas(self) -> Dict[ReplicaID, ReplicaWrapper]:
        pass
