import abc
from typing import Callable, List, TypeVar

import ray
from ray.train._internal.base_worker_group import BaseWorkerGroup
from ray.train.v2._internal.execution.worker_group.worker import Worker
from ray.train.v2._internal.util import ray_get_safe
from ray.types import ObjectRef

T = TypeVar("T")


class ExecutionGroup(BaseWorkerGroup):
    """Base class for groups that can execute functions on workers.

    Provides concrete implementations of the 4 execution methods and __len__
    based on two abstract primitives: _assert_active() and _get_workers().
    """

    @abc.abstractmethod
    def _assert_active(self):
        """Assert that this execution group is active."""
        pass

    @abc.abstractmethod
    def _get_workers(self) -> List[Worker]:
        """Return the list of workers in this group."""
        pass

    def execute_async(self, fn: Callable, *fn_args, **fn_kwargs) -> List[ObjectRef]:
        self._assert_active()
        return [
            worker.execute_async(fn, *fn_args, **fn_kwargs)
            for worker in self._get_workers()
        ]

    def execute(self, fn: Callable[..., T], *fn_args, **fn_kwargs) -> List[T]:
        return ray_get_safe(self.execute_async(fn, *fn_args, **fn_kwargs))

    def execute_single_async(
        self, rank: int, fn: Callable[..., T], *fn_args, **fn_kwargs
    ) -> ObjectRef:
        self._assert_active()
        workers = self._get_workers()

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
        return len(self._get_workers())


class ReplicaGroup(ExecutionGroup):
    """A group representing a subset of workers from a WorkerGroup.

    Used to pass a replica's workers to backend methods (on_start, etc.)
    as if they were a standalone worker group.
    """

    def __init__(self, workers: List[Worker], resources_per_worker: dict):
        self._workers = workers
        self._resources_per_worker = resources_per_worker

    def _assert_active(self):
        # Expected to always be active since just created from WorkerGroup.
        return True

    def _get_workers(self) -> List[Worker]:
        return self._workers

    def get_resources_per_worker(self) -> dict:
        return self._resources_per_worker
