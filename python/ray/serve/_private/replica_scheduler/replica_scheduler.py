import contextvars
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List

from ray.serve._private.common import ReplicaID, RunningReplicaInfo
from ray.serve._private.replica_scheduler.common import (
    PendingRequest,
    ReplicaQueueLengthCache,
)
from ray.serve._private.replica_scheduler.replica_wrapper import RunningReplica


@dataclass()
class _RequestSchedulingContext:
    tried_fewest_multiplexed_models: bool = False
    tried_first_multiplexed_models: bool = False
    tried_same_node: bool = False
    tried_same_az: bool = False
    should_backoff: bool = False


class ReplicaScheduler(ABC):
    """Abstract interface for a replica scheduler (how the router calls it)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    async def choose_replica_for_request(
        self, pending_request: PendingRequest, *, is_retry: bool = False
    ) -> RunningReplica:
        pass

    @abstractmethod
    def create_replica_wrapper(
        self, replica_info: RunningReplicaInfo
    ) -> RunningReplica:
        pass

    @abstractmethod
    def update_replicas(self, replicas: List[RunningReplica]):
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
    def curr_replicas(self) -> Dict[ReplicaID, RunningReplica]:
        pass


_request_scheduling_context = contextvars.ContextVar(
    "Serve internal request scheduling context variable", default=None
)


def _get_request_scheduling_context() -> _RequestSchedulingContext:
    """Get the current request scheduling context.

    Returns:
        The current request context
    """

    if _request_scheduling_context.get() is None:
        _request_scheduling_context.set(_RequestSchedulingContext())
    return _request_scheduling_context.get()


def _set_request_scheduling_context(
    tried_fewest_multiplexed_models: bool = False,
    tried_first_multiplexed_models: bool = False,
    tried_same_node: bool = False,
    tried_same_az: bool = False,
    should_backoff: bool = False,
):
    """Set the request context. If the value is not set,
    the current context value will be used."""

    current_context = _get_request_scheduling_context()

    _request_scheduling_context.set(
        _RequestSchedulingContext(
            tried_fewest_multiplexed_models=tried_fewest_multiplexed_models
            or current_context.tried_fewest_multiplexed_models,
            tried_first_multiplexed_models=tried_first_multiplexed_models
            or current_context.tried_first_multiplexed_models,
            tried_same_node=tried_same_node or current_context.tried_same_node,
            tried_same_az=tried_same_az or current_context.tried_same_az,
            should_backoff=should_backoff or current_context.should_backoff,
        )
    )
