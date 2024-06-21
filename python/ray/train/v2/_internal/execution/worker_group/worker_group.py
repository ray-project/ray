import collections
import logging
import os
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple, TypeVar

import ray
from ray.train import Checkpoint
from ray.train.v2._internal.constants import (
    DEFAULT_MAX_CONSECUTIVE_HEALTH_CHECK_MISSES,
    MAX_CONSECUTIVE_HEALTH_CHECK_MISSES_ENV_VAR,
    get_env_vars_to_propagate,
)
from ray.train.v2._internal.exceptions import (
    WorkerHealthCheckFailedError,
    WorkerHealthCheckMissedError,
)
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor
from ray.train.v2._internal.execution.context import DistributedContext, StorageContext
from ray.train.v2._internal.execution.worker_group.worker import (
    RayTrainWorker,
    Worker,
    WorkerStatus,
)
from ray.train.v2._internal.util import bundle_to_remote_args, time_monotonic
from ray.train.v2.api.config import RunConfig
from ray.types import ObjectRef
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class WorkerGroupStatus:
    num_workers: int
    latest_start_time: float
    worker_statuses: Dict[int, WorkerStatus]

    @property
    def errors(self) -> Dict[int, Exception]:
        return {
            world_rank: status.error
            for world_rank, status in self.worker_statuses.items()
            if status.error is not None
        }

    @property
    def finished(self) -> bool:
        return self.worker_statuses and all(
            not status.running for status in self.worker_statuses.values()
        )


class WorkerGroup:
    def __init__(
        self,
        run_config: Optional[RunConfig] = None,
    ):
        self._run_config = run_config or RunConfig()
        self._storage_context = StorageContext(
            storage_path=self._run_config.storage_path,
            experiment_dir_name=self._run_config.name,
            storage_filesystem=self._run_config.storage_filesystem,
        )

        # List of workers in this worker group.
        # These should always be in sorted order by world rank.
        self._workers: List[Worker] = []

        self._latest_start_time = float("-inf")
        self._pg = None

        # Long-running actor task refs that run the training function.
        self._train_fn_tasks: List[ObjectRef] = []

        # Maps world rank to the number of consecutive health check misses.
        self._num_consecutive_poll_misses: Dict[int, int] = collections.defaultdict(int)
        self._max_consecutive_poll_misses = int(
            os.getenv(
                MAX_CONSECUTIVE_HEALTH_CHECK_MISSES_ENV_VAR,
                DEFAULT_MAX_CONSECUTIVE_HEALTH_CHECK_MISSES,
            )
        )

    def start(
        self,
        num_workers: int,
        resources_per_worker: dict,
        checkpoint: Optional[Checkpoint] = None,
    ):
        """Start the a number of workers with the given resources.

        This should also also handle rank assignment.
        """
        if self._workers:
            raise ValueError("Workers already started.")

        remote_actor_cls = ray.remote(**bundle_to_remote_args(resources_per_worker))(
            RayTrainWorker
        )
        pg = self._pg = placement_group([resources_per_worker] * num_workers)

        logger.info(f"Starting worker group of size {num_workers}.")
        actors = [
            remote_actor_cls.options(
                max_concurrency=2,
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg, placement_group_bundle_index=i
                ),
                runtime_env={"env_vars": get_env_vars_to_propagate()},
            ).remote()
            for i in range(num_workers)
        ]
        actor_metadatas = ray.get([actor.get_metadata.remote() for actor in actors])
        workers = [Worker(actor, meta) for actor, meta in zip(actors, actor_metadatas)]
        self._workers = self._sort_workers_by_node_id_and_gpu_id(workers)

        # Initialize the synchronization actor on the driver node
        self._sync_actor = SynchronizationActor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=ray.get_runtime_context().get_node_id(),
                soft=False,
            )
        ).remote()

        self._init_train_context_on_workers(checkpoint=checkpoint)

    @classmethod
    def _sort_workers_by_node_id_and_gpu_id(
        cls, workers: List[Worker], _first_id: Optional[str] = None
    ) -> List[Worker]:
        """Reorder the workers by their node id and the lowest GPU id.

        This sorted order should be used to assign world ranks to the workers.

        Example:
            Given workers with the following attributes:
                worker_0: id=1, gpu_ids=[1]
                worker_1: id=0, gpu_ids=[0]
                worker_2: id=1, gpu_ids=[0]
                worker_3: id=0, gpu_ids=[1]

            The function will perform the following steps:
                1. Group by node IP:
                    id=0: worker_1, worker_3
                    id=1: worker_0, worker_2

                2. Sort each group by GPU ID:
                    id=0: worker_1 (gpu_id=0), worker_3 (gpu_id=1)
                    id=1: worker_2 (gpu_id=0), worker_0 (gpu_id=1)

            Resulting in the order: [worker_1, worker_3, worker_2, worker_0]

        Args:
            _first_id: The first node id to group by.
        """
        node_id_to_workers = collections.defaultdict(list)

        if _first_id is not None:
            node_id_to_workers[_first_id] = []

        for worker in workers:
            node_id_to_workers[worker.metadata.node_id].append(worker)

        # Sort workers on the same node by the lowest GPU id
        # More details: https://github.com/ray-project/ray/issues/40803
        def get_lowest_gpu_id(worker) -> int:
            gpu_ids = worker.metadata.accelerator_ids.get("GPU", [])
            # If there are no GPU IDs, return 0 as a default
            if not gpu_ids:
                return 0

            # Attempt to convert GPU IDs to integers and find the minimum ID.
            # Fallback to return the minimum string-based ID
            try:
                return min(int(gpu_id) for gpu_id in gpu_ids)
            except ValueError:
                return min(gpu_ids)

        for node_id in node_id_to_workers:
            node_id_to_workers[node_id].sort(key=get_lowest_gpu_id)

        sorted_workers = []
        for workers in node_id_to_workers.values():
            sorted_workers.extend(workers)
        return sorted_workers

    def has_started(self) -> bool:
        return bool(self._workers)

    def shutdown(self, patience_s: float = 0.0):
        """Shutdown all the workers in this worker group.

        Args:
            patience_s: Attempt a graceful shutdown
                of the workers for this many seconds. Fallback to force kill
                if graceful shutdown is not complete after this time. If
                this is less than or equal to 0, immediately force kill all
                workers.
        """
        if not self._workers:
            return

        logger.debug(f"Shutting down {len(self._workers)} workers.")
        if patience_s <= 0:
            for worker in self._workers:
                ray.kill(worker.actor)
        else:
            done_refs = [w.actor.__ray_terminate__.remote() for w in self._workers]
            # Wait for actors to die gracefully.
            _, not_done = ray.wait(done_refs, timeout=patience_s)
            if not_done:
                logger.debug("Graceful termination failed. Falling back to force kill.")
                # If all actors are not able to die gracefully, then kill them.
                for worker in self._workers:
                    ray.kill(worker.actor)

        remove_placement_group(self._pg)
        ray.kill(self._sync_actor)

        logger.debug("Shutdown successful.")
        self._clear_state()

    def _clear_state(self):
        self._workers = []
        self._pg = None
        self._train_fn_tasks = []
        self._num_consecutive_poll_misses = collections.defaultdict(int)
        self._sync_actor = None

    def _assert_workers_started(self):
        if not self._workers:
            raise ValueError("Workers not started.")

    def run_train_fn(self, train_fn: Callable):
        self._assert_workers_started()

        self._train_fn_tasks = [
            worker.actor.run_train_fn.remote(train_fn) for worker in self._workers
        ]

        self._latest_start_time = time_monotonic()

    def _poll_workers_and_collect_errors(
        self, timeout: Optional[float]
    ) -> Tuple[List, List[Optional[Exception]]]:
        """Launch poll tasks on each worker and collect the results.

        The poll task should involve very little computation and should
        return almost immediately.
        If a worker does not return the result of the poll task within
        the timeout, it is considered as a missed health check.

        The timeout is set to ~seconds, so a missed health check usually
        means that something is wrong with the worker.
        If a worker misses too many consecutive health checks, it is marked as dead
        and a WorkerHealthCheckMissedError is propagated as the error in the
        worker status, for the controller to handle.
        If a worker's poll task fails, a WorkerHealthCheckFailedError is similarly
        propagated in the worker status.

        Returns:
            poll_results: A list of poll results from each worker.
            poll_errors: Poll task errors or None if the poll task succeeded.
        """
        poll_task_to_world_rank = {
            worker.actor.poll_status.remote(): i
            for i, worker in enumerate(self._workers)
        }
        done_polls, hanging_polls = ray.wait(
            list(poll_task_to_world_rank),
            num_returns=len(poll_task_to_world_rank),
            timeout=timeout,
        )

        poll_task_to_result, poll_task_to_error = {}, {}

        for hanging_poll in hanging_polls:
            hanging_rank = poll_task_to_world_rank[hanging_poll]
            self._num_consecutive_poll_misses[hanging_rank] += 1
            total_misses = self._num_consecutive_poll_misses[hanging_rank]

            if total_misses >= self._max_consecutive_poll_misses:
                error_msg = (
                    f"A worker has missed {total_misses} "
                    "consecutive health checks. Marking the worker as dead.\n"
                    f"Worker info: {self._workers[hanging_rank]}"
                )
                logger.error(error_msg)
                poll_task_to_error[hanging_poll] = WorkerHealthCheckMissedError(
                    error_msg
                )

        for done_poll in done_polls:
            done_rank = poll_task_to_world_rank[done_poll]
            try:
                poll_result = ray.get(done_poll)
                poll_task_to_result[done_poll] = poll_result
                # Reset the consecutive poll misses for the worker.
                self._num_consecutive_poll_misses[done_rank] = 0
            except Exception as e:
                error_msg = (
                    "A worker health check failed.\n"
                    f"Worker info: {self._workers[done_rank]}"
                )
                logger.exception(error_msg)
                poll_task_to_error[done_poll] = WorkerHealthCheckFailedError(
                    error_msg, e
                )

        results = [
            poll_task_to_result.get(poll_task) for poll_task in poll_task_to_world_rank
        ]
        errors = [
            poll_task_to_error.get(poll_task) for poll_task in poll_task_to_world_rank
        ]
        return results, errors

    def _poll_train_tasks(self) -> Tuple[List[bool], List[Optional[Exception]]]:
        assert self._train_fn_tasks

        done_train_tasks, _ = ray.wait(
            self._train_fn_tasks, num_returns=len(self._train_fn_tasks), timeout=0
        )

        train_task_to_error = {}
        for train_task in done_train_tasks:
            try:
                ray.get(train_task)
            except Exception as e:
                train_task_to_error[train_task] = e

        training_finished = [task in done_train_tasks for task in self._train_fn_tasks]
        errors = [train_task_to_error.get(task) for task in self._train_fn_tasks]
        return training_finished, errors

    def poll_status(self, timeout: Optional[float] = None) -> WorkerGroupStatus:
        if not self._workers:
            return WorkerGroupStatus(0, self._latest_start_time, {})

        poll_results, poll_errors = self._poll_workers_and_collect_errors(timeout)
        training_finished, train_task_errors = self._poll_train_tasks()

        # Populate the worker status errors with the errors raised from
        # either of the poll or training tasks.
        # The poll task error takes precedence if both tasks fail.
        errors = [
            poll_error or train_task_error
            for poll_error, train_task_error in zip(poll_errors, train_task_errors)
        ]

        return WorkerGroupStatus(
            num_workers=len(self._workers),
            latest_start_time=self._latest_start_time,
            worker_statuses={
                world_rank: WorkerStatus(
                    running=(not finished), error=error, training_result=result
                )
                for world_rank, (finished, error, result) in enumerate(
                    zip(training_finished, errors, poll_results)
                )
            },
        )

    def execute_async(self, fn: Callable, *fn_args, **fn_kwargs) -> List[ObjectRef]:
        """Execute ``func`` on each worker and return the futures.

        Returns:
            (List[ObjectRef]) A list of ``ObjectRef`` representing the
                output of ``func`` from each worker. The order is the same
                as ``self.workers``.

        """
        self._assert_workers_started()

        return [
            worker.actor.execute.options(name=f"execute.{fn.__name__}").remote(
                fn, *fn_args, **fn_kwargs
            )
            for worker in self._workers
        ]

    def execute(self, fn: Callable[..., T], *fn_args, **fn_kwargs) -> List[T]:
        """Execute ``func`` on each worker and return the outputs of ``func``.

        Returns:
            (List[T]) A list containing the output of ``func`` from each
                worker. The order is the same as ``self.workers``.

        """
        self._assert_workers_started()

        return ray.get(self.execute_async(fn, *fn_args, **fn_kwargs))

    def execute_single_async(
        self, rank: int, fn: Callable[..., T], *fn_args, **fn_kwargs
    ) -> ObjectRef:
        """Execute ``func`` on worker with ``rank`` and return futures.

        Returns:
            (ObjectRef) An ObjectRef representing the output of func.

        """
        self._assert_workers_started()

        if rank >= len(self._workers):
            raise ValueError(
                f"The provided {rank=} is "
                f"not valid for {len(self._workers)} workers."
            )

        return (
            self._workers[rank]
            .actor.execute.options(name=f"execute.{fn.__name__}")
            .remote(fn, *fn_args, **fn_kwargs)
        )

    def execute_single(
        self, rank: int, fn: Callable[..., T], *fn_args, **fn_kwargs
    ) -> T:
        """Execute ``func`` on worker with ``rank``.

        Returns:
            (T) The output of func.

        """
        self._assert_workers_started()

        return ray.get(self.execute_single_async(rank, fn, *fn_args, **fn_kwargs))

    def __len__(self) -> int:
        return len(self._workers)

    def get_workers(self) -> List[Worker]:
        return self._workers

    def _set_worker_group_distributed_contexts(self) -> None:
        node_ip_to_workers = collections.defaultdict(list)
        for worker in self._workers:
            node_ip_to_workers[worker.metadata.node_ip].append(worker)
        node_ips = list(node_ip_to_workers.keys())

        distributed_contexts = []
        worker_info_str = ""
        for world_rank, worker in enumerate(self._workers):
            worker_distributed_context = DistributedContext(
                local_rank=node_ip_to_workers[worker.metadata.node_ip].index(worker),
                local_world_size=len(node_ip_to_workers[worker.metadata.node_ip]),
                world_rank=world_rank,
                world_size=len(self._workers),
                node_rank=node_ips.index(worker.metadata.node_ip),
            )
            distributed_contexts.append(worker_distributed_context)

            worker_info_str += (
                f"\n- (ip={worker.metadata.node_ip}, pid={worker.metadata.pid}) "
                f"world_rank={world_rank}, "
                f"local_rank={worker_distributed_context.local_rank}, "
                f"node_rank={worker_distributed_context.node_rank}"
            )

        self._distributed_contexts = distributed_contexts
        logger.info(f"Started distributed worker group:{worker_info_str}")

    def _init_train_context_on_workers(self, checkpoint: Optional[Checkpoint] = None):
        # Set up worker group distributed contexts
        self._set_worker_group_distributed_contexts()
        context_init_tasks = [
            worker.actor.init_train_context.remote(
                run_config=self._run_config,
                distributed_context=self._distributed_contexts[i],
                synchronization_actor=self._sync_actor,
                storage_context=self._storage_context,
                checkpoint=checkpoint,
            )
            for i, worker in enumerate(self._workers)
        ]
        ray.get(context_init_tasks)

    # Testing only

    def _get_train_fn_tasks(self) -> List[ObjectRef]:
        return self._train_fn_tasks
