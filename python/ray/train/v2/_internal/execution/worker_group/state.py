import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, List, Optional

import ray
from ray.actor import ActorHandle
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor
from ray.train.v2._internal.execution.worker_group.worker import Worker
from ray.train.v2._internal.util import ObjectRefWrapper, time_monotonic

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.worker_group.placement_group_handle import (
        PlacementGroupHandle,
    )

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerGroupContext:
    """Context for a worker group.

    This stores the context that is shared when starting a worker group.

    Attributes:
        run_attempt_id: The ID of the run attempt.
        train_fn_ref: An object store reference to the training function to execute.
        num_workers: The number of workers in the worker group.
        resources_per_worker: The resources per worker.
        placement_strategy: Strategy for placing workers.
        label_selector: Optional label selectors to apply per-bundle for workers.
        num_slices: The number of TPU slices (if using TPU). Defaults to 1.
    """

    run_attempt_id: str
    train_fn_ref: ObjectRefWrapper[Callable[[], None]]
    num_workers: int
    resources_per_worker: Dict[str, float]
    placement_strategy: str = "PACK"
    label_selector: Optional[List[Dict[str, str]]] = None
    num_slices: int = 1


@dataclass(frozen=True)
class WorkerGroupState:
    """Ongoing state of an active worker group.

    Attributes:
        start_time: The time when the worker group was started.
        workers: The workers in the worker group.
            These should always be in sorted order by world rank.
        placement_group_handle: The placement group handle for the worker group.
        sync_actor: The synchronization actor for the worker group.
    """

    start_time: float
    placement_group_handle: "PlacementGroupHandle"
    workers: List[Worker]
    sync_actor: ActorHandle

    @property
    def num_workers(self) -> int:
        return len(self.workers)

    def replace_workers(
        self, old_workers: List[Worker], new_workers: List[Worker]
    ) -> "WorkerGroupState":
        """Return a new WorkerGroupState with old_workers replaced by new_workers."""
        current_workers = list(self.workers)
        for old_w, new_w in zip(old_workers, new_workers):
            idx = current_workers.index(old_w)
            current_workers[idx] = new_w
        return WorkerGroupState(
            start_time=self.start_time,
            placement_group_handle=self.placement_group_handle,
            workers=current_workers,
            sync_actor=self.sync_actor,
        )

    def shutdown(self):
        _shutdown_workers(self.workers)
        _shutdown_sync_actor(self.sync_actor)
        self.placement_group_handle.shutdown()


class WorkerGroupStateBuilder:
    """Builder for WorkerGroupState.

    Example usage:
        ```python
        builder = WorkerGroupStateBuilder()
        builder.with_placement_group_handle(placement_group_handle)
        builder.with_workers(workers)
        builder.with_sync_actor(sync_actor)
        state = builder.build()

        builder.shutdown(patience_s=10)
        ```
    """

    def __init__(self):
        self.placement_group_handle = None
        self.workers = None
        self.sync_actor = None

    def with_placement_group_handle(
        self, placement_group_handle: "PlacementGroupHandle"
    ) -> "WorkerGroupStateBuilder":
        self.placement_group_handle = placement_group_handle
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
            "placement_group_handle": self.placement_group_handle,
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
            placement_group_handle=self.placement_group_handle,
            workers=self.workers,
            sync_actor=self.sync_actor,
        )

    def shutdown(self):
        if self.workers:
            _shutdown_workers(self.workers)
            self.workers = None

        if self.sync_actor:
            _shutdown_sync_actor(self.sync_actor)
            self.sync_actor = None

        if self.placement_group_handle:
            self.placement_group_handle.shutdown()
            self.placement_group_handle = None


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
