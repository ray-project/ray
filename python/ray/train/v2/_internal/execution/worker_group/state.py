import logging
from dataclasses import dataclass
from typing import List, Optional

import ray
from ray.actor import ActorHandle
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor
from ray.train.v2._internal.execution.worker_group.worker import Worker
from ray.train.v2._internal.util import time_monotonic
from ray.util.placement_group import PlacementGroup, remove_placement_group
from ray.util.tpu import SlicePlacementGroup

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerGroupState:
    """Ongoing state of an active worker group.

    Attributes:
        start_time: The time when the worker group was started.
        workers: The workers in the worker group.
            These should always be in sorted order by world rank.
        placement_group: The placement group for the worker group.
        sync_actor: The synchronization actor for the worker group.
    """

    start_time: float
    placement_group: PlacementGroup
    workers: List[Worker]
    sync_actor: ActorHandle
    slice_placement_group: Optional[SlicePlacementGroup] = None

    @property
    def num_workers(self) -> int:
        return len(self.workers)

    def shutdown(self):
        _shutdown_workers(self.workers)
        _shutdown_sync_actor(self.sync_actor)

        if self.slice_placement_group:
            self.slice_placement_group.shutdown()
        else:
            _shutdown_placement_group(self.placement_group)


class WorkerGroupStateBuilder:
    """Builder for WorkerGroupState.

    Example usage:
        ```python
        builder = WorkerGroupStateBuilder()
        builder.with_placement_group(placement_group)
        builder.with_workers(workers)
        builder.with_sync_actor(sync_actor)
        state = builder.build()

        builder.shutdown(patience_s=10)
        ```
    """

    def __init__(self):
        self.placement_group = None
        self.workers = None
        self.sync_actor = None
        self.slice_placement_group = None

    def with_placement_group(
        self, placement_group: PlacementGroup
    ) -> "WorkerGroupStateBuilder":
        self.placement_group = placement_group
        return self

    def with_slice_placement_group(
        self, slice_placement_group: SlicePlacementGroup
    ) -> "WorkerGroupStateBuilder":
        self.slice_placement_group = slice_placement_group
        return self

    def with_workers(self, workers: List[Worker]) -> "WorkerGroupStateBuilder":
        self.workers = workers
        return self

    def with_sync_actor(
        self, sync_actor: SynchronizationActor
    ) -> "WorkerGroupStateBuilder":
        self.sync_actor = sync_actor
        return self

    def build(self) -> WorkerGroupState:
        required_attrs = {
            "placement_group": self.placement_group,
            "workers": self.workers,
            "sync_actor": self.sync_actor,
        }
        missing = [name for name, attr in required_attrs.items() if attr is None]
        if missing:
            raise ValueError(
                f"Cannot build incomplete state. Missing: {', '.join(missing)}"
            )
        return WorkerGroupState(
            start_time=time_monotonic(),
            placement_group=self.placement_group,
            workers=self.workers,
            sync_actor=self.sync_actor,
            slice_placement_group=self.slice_placement_group,
        )

    def shutdown(self):
        if self.workers:
            _shutdown_workers(self.workers)
            self.workers = None

        if self.sync_actor:
            _shutdown_sync_actor(self.sync_actor)
            self.sync_actor = None

        if self.slice_placement_group:
            self.slice_placement_group.shutdown()
            self.slice_placement_group = None
            self.placement_group = None
        elif self.placement_group:
            _shutdown_placement_group(self.placement_group)
            self.placement_group = None


def _shutdown_workers(workers: List[Worker], patience_s: float = 5):
    """Shuts down workers after allowing a maximum of patience_s seconds for shutdown hooks to run."""
    if patience_s < 0:
        raise ValueError("Invalid patience_s: must be non-negative")

    done_refs = [w.actor.shutdown.remote() for w in workers]

    logger.debug(f"Shutting down {len(workers)} workers.")

    ray.wait(done_refs, num_returns=len(done_refs), timeout=patience_s)

    for worker in workers:
        ray.kill(worker.actor)


def _shutdown_sync_actor(sync_actor: SynchronizationActor):
    ray.kill(sync_actor)


def _shutdown_placement_group(placement_group: PlacementGroup):
    remove_placement_group(placement_group)
