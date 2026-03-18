import abc
from typing import TYPE_CHECKING, Callable, List, Optional, TypeVar

import ray
from ray.train._internal.base_worker_group import BaseWorkerGroup
from ray.train.v2._internal.execution.worker_group.state import _shutdown_workers
from ray.train.v2._internal.execution.worker_group.worker import Worker
from ray.types import ObjectRef

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.callback import ReplicaGroupCallback
    from ray.train.v2._internal.execution.worker_group.poll import (
        WorldRankToOngoingPoll,
    )
    from ray.train.v2._internal.execution.worker_group.state import WorkerGroupContext

T = TypeVar("T")


class ExecutionGroup(BaseWorkerGroup):
    """Base class for groups that can execute functions on workers.

    Provides concrete implementations of the 4 execution methods and __len__
    based on two abstract primitives: _assert_active() and get_workers().
    """

    @abc.abstractmethod
    def _assert_active(self):
        """Assert that this execution group is active."""
        pass

    @abc.abstractmethod
    def get_workers(self) -> List[Worker]:
        """Return the list of workers in this group."""
        pass

    def execute_async(self, fn: Callable, *fn_args, **fn_kwargs) -> List[ObjectRef]:
        self._assert_active()
        workers = self.get_workers()

        return [worker.execute_async(fn, *fn_args, **fn_kwargs) for worker in workers]

    def execute(self, fn: Callable[..., T], *fn_args, **fn_kwargs) -> List[T]:
        return ray.get(self.execute_async(fn, *fn_args, **fn_kwargs))

    def execute_single_async(
        self, rank: int, fn: Callable[..., T], *fn_args, **fn_kwargs
    ) -> ObjectRef:
        self._assert_active()
        workers = self.get_workers()

        if rank >= len(workers):
            raise ValueError(
                f"The provided {rank=} is " f"not valid for {len(workers)} workers."
            )

        return workers[rank].execute_async(fn, *fn_args, **fn_kwargs)

    def execute_single(
        self, rank: int, fn: Callable[..., T], *fn_args, **fn_kwargs
    ) -> T:
        return ray.get(self.execute_single_async(rank, fn, *fn_args, **fn_kwargs))

    def __len__(self) -> int:
        self._assert_active()
        return len(self.get_workers())


class ReplicaGroup(ExecutionGroup):
    """A group representing a subset of workers from a WorkerGroup.

    Used to pass a replica's workers to backend methods (on_start, etc.)
    as if they were a standalone worker group.
    """

    def __init__(
        self,
        workers: List[Worker],
        resources_per_worker: dict,
        world_rank_to_ongoing_poll: Optional["WorldRankToOngoingPoll"] = None,
        callbacks: Optional[List["ReplicaGroupCallback"]] = None,
    ):
        self._workers = workers
        self._resources_per_worker = resources_per_worker
        self._world_rank_to_ongoing_poll = world_rank_to_ongoing_poll
        self._callbacks = callbacks or []
        # An inactive ReplicaGroup still needs to keep track of workers
        # so we can replace them later.
        self._active = True

    def _assert_active(self):
        if not self.is_active():
            raise ValueError("ReplicaGroup has been shut down.")

    def is_active(self) -> bool:
        return self._active

    def get_workers(self) -> List[Worker]:
        return self._workers

    def get_resources_per_worker(self) -> dict:
        return self._resources_per_worker

    def shutdown(self):
        """Shutdown all workers in this replica group and clear state."""
        if self.is_active():
            for cb in self._callbacks:
                cb.before_replica_group_shutdown(self)
            # Remove workers from ongoing poll tracking before shutdown.
            if self._world_rank_to_ongoing_poll is not None:
                ranks = [
                    w.distributed_context.world_rank
                    for w in self._workers
                    if w.distributed_context is not None
                ]
                self._world_rank_to_ongoing_poll.remove_ranks(ranks)

            _shutdown_workers(self._workers)
            self._active = False

    def start_training(self, worker_group_context: "WorkerGroupContext"):
        """Start training on all workers in this replica group."""
        for cb in self._callbacks:
            cb.after_replica_group_start(self)
        ray.get(
            [
                worker.actor.run_train_fn.remote(worker_group_context.train_fn_ref)
                for worker in self._workers
            ]
        )
