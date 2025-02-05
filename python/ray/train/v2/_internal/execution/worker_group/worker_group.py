import collections
import logging
import os
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union

import ray
from ray._private.ray_constants import env_float
from ray.exceptions import GetTimeoutError, RayActorError
from ray.train import Checkpoint
from ray.train.v2._internal.constants import (
    DEFAULT_REPORT_BARRIER_TIMEOUT_S,
    DEFAULT_REPORT_BARRIER_WARN_INTERVAL_S,
    DEFAULT_WORKER_GROUP_START_TIMEOUT_S,
    DEFAULT_WORKER_HEALTH_CHECK_TIMEOUT_S,
    REPORT_BARRIER_TIMEOUT_S_ENV_VAR,
    REPORT_BARRIER_WARN_INTERVAL_S_ENV_VAR,
    WORKER_GROUP_START_TIMEOUT_S_ENV_VAR,
    WORKER_HEALTH_CHECK_TIMEOUT_S_ENV_VAR,
    get_env_vars_to_propagate,
)
from ray.train.v2._internal.exceptions import (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
    WorkerHealthCheckFailedError,
    WorkerHealthCheckTimeoutError,
)
from ray.train.v2._internal.execution.callback import (
    TrainContextCallback,
    WorkerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor
from ray.train.v2._internal.execution.context import (
    DistributedContext,
    StorageContext,
    TrainRunContext,
)
from ray.train.v2._internal.execution.worker_group.worker import (
    RayTrainWorker,
    Worker,
    WorkerStatus,
)
from ray.train.v2._internal.util import (
    bundle_to_remote_args,
    invoke_context_managers,
    ray_get_safe,
    time_monotonic,
)
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


@dataclass(frozen=True)
class PollTask:
    """Represents a poll task for a worker.

    Attributes:
        start_time: The time when the poll task was started.
        task: The ObjectRef representing the poll task.
    """

    start_time: float
    task: ObjectRef


class WorkerGroup:
    _worker_cls = RayTrainWorker

    def __init__(
        self,
        train_run_context: TrainRunContext,
        callbacks: Optional[
            List[Union[WorkerGroupCallback, WorkerCallback, TrainContextCallback]]
        ] = None,
    ):
        self._train_run_context = train_run_context
        run_config = self._train_run_context.run_config
        self._storage_context = StorageContext(
            storage_path=run_config.storage_path,
            experiment_dir_name=run_config.name,
            storage_filesystem=run_config.storage_filesystem,
        )
        callbacks = callbacks or []
        # Group of callbacks that are specific to worker group itself.
        self._callbacks = [c for c in callbacks if isinstance(c, WorkerGroupCallback)]
        # Group of callbacks that will be propagated and called on the worker actors.
        self._worker_callbacks_to_propagate = [
            c
            for c in callbacks
            if isinstance(c, (WorkerCallback, TrainContextCallback))
        ]

        # List of workers in this worker group.
        # These should always be in sorted order by world rank.
        self._workers: List[Worker] = []

        self._latest_start_time = float("-inf")
        self._pg = None
        self._sync_actor = None

        # Maps world rank to the ongoing poll task.
        self._world_rank_to_ongoing_poll: Dict[int, PollTask] = {}

        # Environment variables
        self._worker_group_start_timeout_s = float(
            os.environ.get(
                WORKER_GROUP_START_TIMEOUT_S_ENV_VAR,
                DEFAULT_WORKER_GROUP_START_TIMEOUT_S,
            )
        )
        self._worker_health_check_timeout_s = float(
            os.getenv(
                WORKER_HEALTH_CHECK_TIMEOUT_S_ENV_VAR,
                DEFAULT_WORKER_HEALTH_CHECK_TIMEOUT_S,
            )
        )
        self._report_barrier_timeout_s = env_float(
            REPORT_BARRIER_TIMEOUT_S_ENV_VAR, DEFAULT_REPORT_BARRIER_TIMEOUT_S
        )
        self._report_barrier_warn_interval_s = env_float(
            REPORT_BARRIER_WARN_INTERVAL_S_ENV_VAR,
            DEFAULT_REPORT_BARRIER_WARN_INTERVAL_S,
        )

    def _create_workers(
        self,
        num_workers: int,
        worker_actor_cls: Type[RayTrainWorker],
    ) -> List[Worker]:
        assert self._pg, "Placement group must be initialized before creating workers."

        actors = [
            worker_actor_cls.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=self._pg, placement_group_bundle_index=i
                ),
                runtime_env={"env_vars": get_env_vars_to_propagate()},
            ).remote()
            for i in range(num_workers)
        ]

        try:
            actor_metadatas = ray_get_safe(
                [actor.get_metadata.remote() for actor in actors]
            )
        except RayActorError as actor_error:
            for actor in actors:
                ray.kill(actor)

            # Make sure to clear any other state (e.g., placement group) that was set.
            self.shutdown()

            error_msg = (
                "One of the worker actors failed to initialize due to error:\n"
                f"{traceback.format_exc()}"
            )
            raise WorkerGroupStartupFailedError(error_msg) from actor_error

        workers = [Worker(actor, meta) for actor, meta in zip(actors, actor_metadatas)]
        return self._assign_worker_ranks(workers)

    def _init_train_context_on_workers(
        self,
        train_context_args: Dict[str, List[Any]],
    ) -> None:
        context_init_tasks = [
            worker.actor.init_train_context.remote(
                train_run_context=self._train_run_context,
                distributed_context=worker.distributed_context,
                synchronization_actor=self._sync_actor,
                storage_context=self._storage_context,
                worker_callbacks=self._worker_callbacks_to_propagate,
                **{
                    arg: arg_values[i] for arg, arg_values in train_context_args.items()
                },
            )
            for i, worker in enumerate(self._workers)
        ]
        ray_get_safe(context_init_tasks)

    def start(
        self,
        train_fn: Callable[[], None],
        num_workers: int,
        resources_per_worker: dict,
        placement_strategy: str = "PACK",
        checkpoint: Optional[Checkpoint] = None,
    ):
        """Start the a number of workers with the given resources.

        Assign ranks, and initialize the train context on the workers.

        Raises:
            ValueError: If workers are already started.
            WorkerGroupStartupTimeoutError: If the worker group startup times out
                when requesting resources.
                `RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S` can configure the timeout.
            WorkerGroupStartupFailedError: If the worker group fails to start
                due to actors dying/failing during initialization.
        """
        # TODO: Review the order of `on_xyz_start` and `after_xyz_start` callbacks.
        # The current execution order is as follows:`on_worker_group_start` callbacks
        # are triggered before the `after_worker_group_start` callbacks.
        with invoke_context_managers(
            [callback.on_worker_group_start for callback in self._callbacks]
        ):
            if self._workers:
                raise ValueError("Workers already started.")

            remote_actor_cls = ray.remote(
                **bundle_to_remote_args(resources_per_worker)
            )(self._worker_cls)

            pg = placement_group(
                bundles=[resources_per_worker] * num_workers,
                strategy=placement_strategy,
            )
            logger.info(
                f"Attempting to start training worker group of size {num_workers} with "
                f"the following resources: [{resources_per_worker}] * {num_workers}"
            )

            # Wait for the placement group to be ready before proceeding
            # to create actors.
            # This could hang if the resources are not available, so we should
            # time out if this hangs for a while to try again with a different size.
            # For example, the controller may try to set a worker group size
            # based on stale information about cluster resources.
            try:
                ray.get(pg.ready(), timeout=self._worker_group_start_timeout_s)
            except GetTimeoutError as timeout_exc:
                remove_placement_group(pg)
                raise WorkerGroupStartupTimeoutError(
                    num_workers=num_workers
                ) from timeout_exc

            self._pg = pg

            # Initialize the synchronization actor on the driver node
            self._sync_actor = SynchronizationActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=ray.get_runtime_context().get_node_id(),
                    soft=False,
                )
            ).remote(
                timeout_s=self._report_barrier_timeout_s,
                warn_interval_s=self._report_barrier_warn_interval_s,
            )

            self._workers = self._create_workers(num_workers, remote_actor_cls)

            # All the ray.get calls in this try block can possibly error if the
            # worker actors die during initialization.
            # To prevent the driver from crashing, catch all `RayActorError`s and
            # raise a specially handled error to the controller.
            try:
                train_context_args = {"checkpoint": [checkpoint] * len(self._workers)}
                for callable in self._callbacks:
                    args = callable.before_init_train_context(self)
                    for arg, arg_values in args.items():
                        assert len(arg_values) == num_workers, (
                            f"Callback {callable} returned {arg} with "
                            f"{len(arg_values)} values, expected {num_workers}."
                        )
                        assert (
                            arg not in train_context_args
                        ), f"Callback {callable} returned {arg} which is already set."
                        train_context_args[arg] = arg_values

                self._init_train_context_on_workers(train_context_args)

                for callback in self._callbacks:
                    callback.after_worker_group_start(self)

                # Launch the training function on each worker.
                # This task should start a worker thread and return immediately.
                ray_get_safe(
                    [
                        worker.actor.run_train_fn.remote(train_fn)
                        for worker in self._workers
                    ]
                )

                for callback in self._callbacks:
                    callback.after_worker_group_training_start(self)
            except RayActorError as actor_error:
                self.shutdown()

                error_msg = "At least one of the worker actors failed to initialize."
                raise WorkerGroupStartupFailedError(error_msg) from actor_error

            self._latest_start_time = time_monotonic()

    @classmethod
    def _sort_workers_by_node_id_and_gpu_id(
        cls, workers: List[Worker], _first_id: Optional[str] = None
    ) -> List[Worker]:
        """Reorder the workers by their node id and the lowest GPU id.

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

    @classmethod
    def _assign_worker_ranks(cls, workers: List[Worker]) -> List[Worker]:
        """Assign world ranks to workers by increasing node id and GPU id.

        Initializes the `DistributedContext` for each worker.

        Returns:
            workers: Workers sorted by increasing world rank,
                with the `DistributedContext` set.
        """
        workers = cls._sort_workers_by_node_id_and_gpu_id(workers)

        node_ip_to_workers = collections.defaultdict(list)
        for worker in workers:
            node_ip_to_workers[worker.metadata.node_ip].append(worker)
        node_ips = list(node_ip_to_workers.keys())

        for world_rank, worker in enumerate(workers):
            distributed_context = DistributedContext(
                local_rank=node_ip_to_workers[worker.metadata.node_ip].index(worker),
                local_world_size=len(node_ip_to_workers[worker.metadata.node_ip]),
                world_rank=world_rank,
                world_size=len(workers),
                node_rank=node_ips.index(worker.metadata.node_ip),
            )
            worker.distributed_context = distributed_context

        return workers

    def has_started(self) -> bool:
        return bool(self._workers)

    def shutdown(self, patience_s: float = 5.0):
        """Shutdown all the workers in this worker group.

        Args:
            patience_s: Attempt a graceful shutdown
                of the workers for this many seconds. Fallback to force kill
                if graceful shutdown is not complete after this time. If
                this is less than or equal to 0, immediately force kill all
                workers.
        """
        with invoke_context_managers(
            [callback.on_worker_group_shutdown for callback in self._callbacks]
        ):
            if self._workers:
                for callback in self._callbacks:
                    callback.before_worker_group_shutdown(self)

                # Run the worker shutdown logic on each of the workers. This should
                # be a non-blocking call to realize forceful shutdown after patience_s.
                _ = [w.actor.shutdown.remote() for w in self._workers]

            logger.debug(f"Shutting down {len(self._workers)} workers.")
            if patience_s <= 0:
                for worker in self._workers:
                    ray.kill(worker.actor)
            else:
                done_refs = [w.actor.__ray_terminate__.remote() for w in self._workers]
                # Wait for actors to die gracefully.
                _, not_done = ray.wait(
                    done_refs, num_returns=len(done_refs), timeout=patience_s
                )
                if not_done:
                    logger.debug(
                        "Graceful termination failed. Falling back to force kill."
                    )
                    # If all actors are not able to die gracefully, then kill them.
                    for worker in self._workers:
                        ray.kill(worker.actor)

            if self._sync_actor:
                ray.kill(self._sync_actor)

            if self._pg:
                remove_placement_group(self._pg)

            self._clear_state()

            logger.debug("Worker group shutdown successful.")

    def _clear_state(self):
        self._workers = []
        self._pg = None
        self._world_rank_to_ongoing_poll = {}
        self._sync_actor = None

    def _assert_workers_started(self):
        if not self._workers:
            raise ValueError("Workers not started.")

    def _get_poll_tasks(self) -> List[ObjectRef]:
        """Get the poll tasks for each worker.

        If there is an ongoing poll task for a worker that did not finish
        in the timeout on the previous round, return that task instead of
        queueing up a new one.

        Spawns a new poll task for the worker if there is no ongoing poll task.
        """
        poll_tasks = []
        for i, worker in enumerate(self._workers):
            if i in self._world_rank_to_ongoing_poll:
                ongoing_poll = self._world_rank_to_ongoing_poll[i]
                poll_tasks.append(ongoing_poll.task)
            else:
                poll_tasks.append(worker.actor.poll_status.remote())
        return poll_tasks

    def _poll_workers_and_collect_errors(
        self, timeout: Optional[float]
    ) -> List[WorkerStatus]:
        """Launch poll tasks on each worker and collect the results.

        The poll task should involve very little computation and should
        return almost immediately.

        If a worker does not return the result of the poll task within
        the timeout, it is considered as a missed health check.
        The timeout is set to ~seconds, so a missed health check usually
        means that something is wrong with the worker.
        Subsequent calls to poll the worker will continue waiting on the
        hanging poll task.

        If a worker's health check hangs for too long, it is marked as dead
        and a WorkerHealthCheckTimeoutError is propagated as the error in the
        worker status for the controller to handle.

        If a worker's poll task fails, a WorkerHealthCheckFailedError is similarly
        propagated in the worker status.

        Returns:
            poll_results: A list of WorkerStatus objects.
                If polling a certain worker hangs or fails, the corresponding
                WorkerStatus object will include a system error mentioned above.
        """
        start_time = time_monotonic()
        poll_tasks = self._get_poll_tasks()
        poll_task_to_world_rank = {
            poll_task: i for i, poll_task in enumerate(poll_tasks)
        }
        done_polls, hanging_polls = ray.wait(
            list(poll_task_to_world_rank),
            num_returns=len(poll_task_to_world_rank),
            timeout=timeout,
        )

        poll_task_to_result = {}

        for hanging_poll in hanging_polls:
            hanging_rank = poll_task_to_world_rank[hanging_poll]

            # The hanging poll task should be saved and awaited in the next round.
            # Save the start time of the poll task to check for timeouts.
            # Don't overwrite the ongoing poll task if it already exists.
            ongoing_poll = self._world_rank_to_ongoing_poll.setdefault(
                hanging_rank, PollTask(start_time, hanging_poll)
            )

            error = None
            elapsed_time_s = time_monotonic() - ongoing_poll.start_time
            if elapsed_time_s > self._worker_health_check_timeout_s:
                error_msg = (
                    f"A worker health check has been hanging for {elapsed_time_s:.2f} "
                    "seconds. Marking the worker as dead.\n"
                    f"Worker info: {self._workers[hanging_rank]}"
                )
                error = WorkerHealthCheckTimeoutError(error_msg)

            poll_task_to_result[hanging_poll] = WorkerStatus(
                running=True, error=error, training_result=None
            )

        for done_poll in done_polls:
            done_rank = poll_task_to_world_rank[done_poll]

            # Remove the ongoing poll task for the worker.
            self._world_rank_to_ongoing_poll.pop(done_rank, None)

            try:
                poll_result: WorkerStatus = ray.get(done_poll)
            except Exception as e:
                error_msg = (
                    "A worker health check failed.\n"
                    f"Worker info: {self._workers[done_rank]}"
                )
                poll_result = WorkerStatus(
                    running=False,
                    error=WorkerHealthCheckFailedError(error_msg, failure=e),
                    training_result=None,
                )

            poll_task_to_result[done_poll] = poll_result

        # Collect the results and errors in the order of the workers.
        results = [
            poll_task_to_result.get(poll_task) for poll_task in poll_task_to_world_rank
        ]
        return results

    def poll_status(self, timeout: Optional[float] = None) -> WorkerGroupStatus:
        """Poll the status of all workers in the worker group.

        Args:
            timeout: The maximum time to wait for the poll tasks to complete.
        """
        if not self._workers:
            return WorkerGroupStatus(
                num_workers=0,
                latest_start_time=self._latest_start_time,
                worker_statuses={},
            )

        poll_results = self._poll_workers_and_collect_errors(timeout)

        worker_group_status = WorkerGroupStatus(
            num_workers=len(self._workers),
            latest_start_time=self._latest_start_time,
            worker_statuses={
                world_rank: worker_status
                for world_rank, worker_status in enumerate(poll_results)
            },
        )

        for callback in self._callbacks:
            callback.after_worker_group_poll_status(worker_group_status)

        return worker_group_status

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

        return ray_get_safe(self.execute_async(fn, *fn_args, **fn_kwargs))

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
