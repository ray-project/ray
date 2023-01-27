import collections
from typing import Dict, Any, Iterator, Callable, List, Tuple, Union, Optional

import ray
from ray.data.block import Block, BlockMetadata
from ray.data.context import DatasetContext, DEFAULT_SCHEDULING_STRATEGY
from ray.data._internal.execution.interfaces import (
    RefBundle,
    ExecutionResources,
    ExecutionOptions,
    PhysicalOperator,
)
from ray.data._internal.execution.operators.map_operator import (
    MapOperator,
    _map_task,
    _TaskState,
)
from ray.types import ObjectRef
from ray._raylet import ObjectRefGenerator


class ActorPoolMapOperator(MapOperator):
    """A MapOperator implementation that executes tasks on an actor pool."""

    def __init__(
        self,
        transform_fn: Callable[[Iterator[Block]], Iterator[Block]],
        input_op: PhysicalOperator,
        name: str = "ActorPoolMap",
        min_rows_per_bundle: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        pool_size: int = 1,
    ):
        """Create an ActorPoolMapOperator instance.

        Args:
            transform_fn: The function to apply to each ref bundle input.
            input_op: Operator generating input data for this op.
            name: The name of this operator.
            min_rows_per_bundle: The number of rows to gather per batch passed to the
                transform_fn, or None to use the block size. Setting the batch size is
                important for the performance of GPU-accelerated transform functions.
                The actual rows passed may be less if the dataset is small.
            ray_remote_args: Customize the ray remote args for this op's tasks.
            pool_size: The desired size of the actor pool.
        """
        super().__init__(
            transform_fn, input_op, name, min_rows_per_bundle, ray_remote_args
        )
        self._ray_remote_args = self._apply_default_remote_args(self._ray_remote_args)

        self._pool_size = pool_size
        # A map from task output futures to task state and the actor on which its
        # running.
        self._tasks: Dict[
            ObjectRef[ObjectRefGenerator], Tuple[_TaskState, ray.actor.ActorHandle]
        ] = {}
        # A pool of running actors on which we can execute mapper tasks.
        self._actor_pool = _ActorPool()
        # A queue of bundles awaiting dispatch to actors.
        self._bundle_queue = collections.deque()
        # Cached actor class.
        self._cls = None
        # Whether no more submittable bundles will be added.
        self._inputs_done = False

    def start(self, options: ExecutionOptions):
        super().start(options)

        # Create the actor workers and add them to the pool.
        self._cls = ray.remote(**self._ray_remote_args)(_MapWorker)
        for _ in range(self._pool_size):
            self._start_actor()

    def _start_actor(self):
        """Start a new actor and add it to the actor pool as a pending actor."""
        assert self._cls is not None
        actor = self._cls.remote()
        self._actor_pool.add_pending_actor(actor, actor.ready.remote())

    def _add_bundled_input(self, bundle: RefBundle):
        self._bundle_queue.append(bundle)
        # Try to dispatch all bundles in the queue, including this new bundle.
        self._dispatch_tasks()

    def _dispatch_tasks(self):
        """Try to dispatch tasks from the bundle buffer to the actor pool.

        This is called when:
            * a new input bundle is added,
            * a task finishes,
            * a new worker has been created.
        """
        while self._bundle_queue:
            # Pick an actor from the pool.
            actor = self._actor_pool.pick_actor()
            if actor is None:
                # No actors available for executing the next task.
                break
            # Submit the map task.
            bundle = self._bundle_queue.popleft()
            input_blocks = [block for block, _ in bundle.blocks]
            ref = actor.submit.options(num_returns="dynamic").remote(
                self._transform_fn_ref, *input_blocks
            )
            task = _TaskState(bundle)
            self._tasks[ref] = (task, actor)
            self._handle_task_submitted(task)

        # Kill inactive workers if there's no more work to do.
        self._kill_inactive_workers_if_done()

    def notify_work_completed(
        self, ref: Union[ObjectRef[ObjectRefGenerator], ray.ObjectRef]
    ):
        # This actor pool MapOperator implementation has both task output futures AND
        # worker started futures to handle here.
        if ref in self._tasks:
            # Get task state and set output.
            task, actor = self._tasks.pop(ref)
            task.output = self._map_ref_to_ref_bundle(ref)
            self._handle_task_done(task)
            # Return the actor that was running the task to the pool.
            self._actor_pool.return_actor(actor)
        else:
            # ref is a future for a now-ready actor; move actor from pending to the
            # active actor pool.
            has_actor = self._actor_pool.pending_to_running(ref)
            if not has_actor:
                # Actor has already been killed.
                return
        # For either a completed task or ready worker, we try to dispatch queued tasks.
        self._dispatch_tasks()

    def inputs_done(self):
        # Call base implementation to handle any leftover bundles. This may or may not
        # trigger task dispatch.
        super().inputs_done()

        # Mark inputs as done so future task dispatch will kill all inactive workers
        # once the bundle queue is exhausted.
        self._inputs_done = True

        # Manually trigger inactive worker termination in case the bundle queue is
        # alread exhausted.
        self._kill_inactive_workers_if_done()

    def _kill_inactive_workers_if_done(self):
        if self._inputs_done and not self._bundle_queue:
            # No more tasks will be submitted, so we kill all current and future
            # inactive workers.
            self._actor_pool.kill_all_inactive_actors()

    def shutdown(self):
        # We kill all actors in the pool on shutdown, even if they are busy doing work.
        self._actor_pool.kill_all_actors()
        super().shutdown()

    def get_work_refs(self) -> List[ray.ObjectRef]:
        # Work references that we wish the executor to wait on includes both task
        # futures AND worker ready futures.
        return list(self._tasks.keys()) + self._actor_pool.get_pending_actor_refs()

    def num_active_work_refs(self) -> int:
        # Active work references only includes running tasks, not pending actor starts.
        return len(self._tasks)

    def progress_str(self) -> str:
        return (
            f"{self._actor_pool.num_running_actors()} "
            f"({self._actor_pool.num_pending_actors()} pending)"
        )

    def base_resource_usage(self) -> ExecutionResources:
        min_workers = self._pool_size
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0) * min_workers,
            gpu=self._ray_remote_args.get("num_gpus", 0) * min_workers,
        )

    def current_resource_usage(self) -> ExecutionResources:
        # Both pending and running actors count towards our current resource usage.
        num_active_workers = self._actor_pool.num_total_actors()
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0) * num_active_workers,
            gpu=self._ray_remote_args.get("num_gpus", 0) * num_active_workers,
            object_store_memory=self._metrics.cur,
        )

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(cpu=0, gpu=0)

    @staticmethod
    def _apply_default_remote_args(ray_remote_args: Dict[str, Any]) -> Dict[str, Any]:
        """Apply defaults to the actor creation remote args."""
        ray_remote_args = ray_remote_args.copy()
        if "scheduling_strategy" not in ray_remote_args:
            ctx = DatasetContext.get_current()
            if ctx.scheduling_strategy == DEFAULT_SCHEDULING_STRATEGY:
                ray_remote_args["scheduling_strategy"] = "SPREAD"
            else:
                ray_remote_args["scheduling_strategy"] = ctx.scheduling_strategy
        return ray_remote_args


class _MapWorker:
    """An actor worker for MapOperator."""

    def ready(self):
        return "ok"

    def submit(
        self, fn: Callable[[Iterator[Block]], Iterator[Block]], *blocks: Block
    ) -> Iterator[Union[Block, List[BlockMetadata]]]:
        yield from _map_task(fn, *blocks)


class _ActorPool:
    """A pool of actors for map task execution.

    This class is in charge of tracking the number of in-flight tasks per actor,
    providing the least heavily loaded actor to the operator, and killing idle
    actors when the operator is done submitting work to the pool.
    """

    def __init__(self):
        # Number of tasks in flight per actor.
        self._num_tasks_in_flight: Dict[ray.actor.ActorHandle, int] = {}
        # Actors that are not yet ready (still pending creation).
        self._pending_actors: Dict[ObjectRef, ray.actor.ActorHandle] = {}
        # Whether actors that become idle should be eagerly killed. This is False until
        # the first call to kill_idle_actors().
        self._should_kill_idle_actors = False

    def add_pending_actor(self, actor: ray.actor.ActorHandle, ready_ref: ray.ObjectRef):
        """Adds a pending actor to the pool.

        This actor won't be pickable until it is marked as running via a
        pending_to_running() call.

        Args:
            actor: The not-yet-ready actor to add as pending to the pool.
            ready_ref: The ready future for the actor.
        """
        # The caller shouldn't add new actors to the pool after invoking
        # kill_inactive_actors().
        assert not self._should_kill_idle_actors
        self._pending_actors[ready_ref] = actor

    def pending_to_running(self, ready_ref: ray.ObjectRef) -> bool:
        """Mark the actor corresponding to the provided ready future as running, making
        the actor pickable.

        Args:
            ready_ref: The ready future for the actor that we wish to mark as running.

        Returns:
            Whether the actor was still pending. This can return False if the actor had
            already been killed.
        """
        if ready_ref not in self._pending_actors:
            # We assume that there was a race between killing the actor and the actor
            # ready future resolving. Since we can rely on ray.kill() eventually killing
            # the actor, we can safely drop this reference.
            return False
        actor = self._pending_actors.pop(ready_ref)
        self._num_tasks_in_flight[actor] = 0
        return True

    def pick_actor(self) -> Optional[ray.actor.ActorHandle]:
        """Provides the least heavily loaded running actor in the pool for task
        submission.

        None will be returned if all actors are still pending.
        """
        if not self._num_tasks_in_flight:
            # Actor pool is empty or all actors are still pending.
            return None

        actor = min(
            self._num_tasks_in_flight.keys(),
            key=lambda actor: self._num_tasks_in_flight[actor],
        )
        self._num_tasks_in_flight[actor] += 1
        return actor

    def return_actor(self, actor: ray.actor.ActorHandle):
        """Returns the provided actor to the pool."""
        assert actor in self._num_tasks_in_flight
        assert self._num_tasks_in_flight[actor] > 0

        self._num_tasks_in_flight[actor] -= 1
        if self._should_kill_idle_actors and self._num_tasks_in_flight[actor] == 0:
            self._kill_running_actor(actor)

    def get_pending_actor_refs(self) -> List[ray.ObjectRef]:
        return list(self._pending_actors.keys())

    def num_total_actors(self) -> int:
        """Return the total number of actors managed by this pool, including pending
        actors
        """
        return self.num_pending_actors() + self.num_running_actors()

    def num_running_actors(self) -> int:
        """Return the number of running actors in the pool."""
        return len(self._num_tasks_in_flight)

    def num_idle_actors(self) -> int:
        """Return the number of idle actors in the pool."""
        return sum(
            1 if tasks_in_flight == 0 else 0
            for tasks_in_flight in self._num_tasks_in_flight.values()
        )

    def num_pending_actors(self) -> int:
        """Return the number of pending actors in the pool."""
        return len(self._pending_actors)

    def num_active_actors(self) -> int:
        """Return the number of actors in the pool with at least one active task."""
        return sum(
            1 if num_tasks_in_flight > 0 else 0
            for num_tasks_in_flight in self._num_tasks_in_flight.values()
        )

    def kill_all_inactive_actors(self):
        """Kills all currently inactive actors and ensures that all actors that become
        idle in the future will be eagerly killed.

        This is called once the operator is done submitting work to the pool, and this
        function is idempotent. Adding new pending actors after calling this function
        will raise an error.
        """
        self._kill_all_pending_actors()
        self._kill_all_idle_actors()

    def kill_all_actors(self):
        """Kills all actors, including running/active actors.

        This is called once the operator is shutting down.
        """
        self._kill_all_pending_actors()
        self._kill_all_running_actors()

    def _kill_all_pending_actors(self):
        pending_actor_refs = list(self._pending_actors.keys())
        for ref in pending_actor_refs:
            self._kill_pending_actor(ref)

    def _kill_all_idle_actors(self):
        idle_actors = [
            actor
            for actor, tasks_in_flight in self._num_tasks_in_flight.items()
            if tasks_in_flight == 0
        ]
        for actor in idle_actors:
            self._kill_running_actor(actor)
        self._should_kill_idle_actors = True

    def _kill_all_running_actors(self):
        actors = list(self._num_tasks_in_flight.keys())
        for actor in actors:
            self._kill_running_actor(actor)

    def _kill_running_actor(self, actor: ray.actor.ActorHandle):
        """Kill the provided actor and remove it from the pool."""
        ray.kill(actor)
        del self._num_tasks_in_flight[actor]

    def _kill_pending_actor(self, ready_ref: ray.ObjectRef):
        """Kill the provided pending actor and remove it from the pool."""
        actor = self._pending_actors.pop(ready_ref)
        ray.kill(actor)
