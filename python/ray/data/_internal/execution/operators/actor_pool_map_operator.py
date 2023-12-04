import collections
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Union

import ray
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    NodeIdStr,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.operators.map_operator import MapOperator, _map_task
from ray.data._internal.execution.operators.map_transformer import MapTransformer
from ray.data._internal.execution.util import locality_string
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef

logger = DatasetLogger(__name__)

# Higher values here are better for prefetching and locality. It's ok for this to be
# fairly high since streaming backpressure prevents us from overloading actors.
DEFAULT_MAX_TASKS_IN_FLIGHT = 4

# The default time to wait for minimum requested actors
# to start before raising a timeout, in seconds.
DEFAULT_WAIT_FOR_MIN_ACTORS_SEC = 60 * 10


class ActorPoolMapOperator(MapOperator):
    """A MapOperator implementation that executes tasks on an actor pool.

    This class manages the state of a pool of actors used for task execution, as well
    as dispatch of tasks to those actors.

    It operates in two modes. In bulk mode, tasks are queued internally and executed
    when the operator has free actor slots. In streaming mode, the streaming executor
    only adds input when `should_add_input() = True` (i.e., there are free slots).
    This allows for better control of backpressure (e.g., suppose we go over memory
    limits after adding put, then there isn't any way to "take back" the inputs prior
    to actual execution).
    """

    def __init__(
        self,
        map_transformer: MapTransformer,
        input_op: PhysicalOperator,
        target_max_block_size: Optional[int],
        autoscaling_policy: "AutoscalingPolicy",
        name: str = "ActorPoolMap",
        min_rows_per_bundle: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Create an ActorPoolMapOperator instance.

        Args:
            transform_fn: The function to apply to each ref bundle input.
            init_fn: The callable class to instantiate on each actor.
            input_op: Operator generating input data for this op.
            autoscaling_policy: A policy controlling when the actor pool should be
                scaled up and scaled down.
            name: The name of this operator.
            target_max_block_size: The target maximum number of bytes to
                include in an output block.
            min_rows_per_bundle: The number of rows to gather per batch passed to the
                transform_fn, or None to use the block size. Setting the batch size is
                important for the performance of GPU-accelerated transform functions.
                The actual rows passed may be less if the dataset is small.
            ray_remote_args: Customize the ray remote args for this op's tasks.
        """
        super().__init__(
            map_transformer,
            input_op,
            name,
            target_max_block_size,
            min_rows_per_bundle,
            ray_remote_args,
        )
        self._ray_remote_args = self._apply_default_remote_args(self._ray_remote_args)
        self._min_rows_per_bundle = min_rows_per_bundle

        # Create autoscaling policy from compute strategy.
        self._autoscaling_policy = autoscaling_policy
        # A pool of running actors on which we can execute mapper tasks.
        self._actor_pool = _ActorPool(autoscaling_policy._config.max_tasks_in_flight)
        # A queue of bundles awaiting dispatch to actors.
        self._bundle_queue = collections.deque()
        # Cached actor class.
        self._cls = None
        # Whether no more submittable bundles will be added.
        self._inputs_done = False

    def internal_queue_size(self) -> int:
        return len(self._bundle_queue)

    def start(self, options: ExecutionOptions):
        self._actor_locality_enabled = options.actor_locality_enabled
        super().start(options)

        # Create the actor workers and add them to the pool.
        self._cls = ray.remote(**self._ray_remote_args)(_MapWorker)
        for _ in range(self._autoscaling_policy.min_workers):
            self._start_actor()
        refs = self._actor_pool.get_pending_actor_refs()

        # We synchronously wait for the initial number of actors to start. This avoids
        # situations where the scheduler is unable to schedule downstream operators
        # due to lack of available actors, causing an initial "pileup" of objects on
        # upstream operators, leading to a spike in memory usage prior to steady state.
        logger.get_logger().info(
            f"{self._name}: Waiting for {len(refs)} pool actors to start..."
        )
        try:
            ray.get(refs, timeout=DEFAULT_WAIT_FOR_MIN_ACTORS_SEC)
        except ray.exceptions.GetTimeoutError:
            raise ray.exceptions.GetTimeoutError(
                "Timed out while starting actors. "
                "This may mean that the cluster does not have "
                "enough resources for the requested actor pool."
            )

    def should_add_input(self) -> bool:
        return self._actor_pool.num_free_slots() > 0

    # Called by streaming executor periodically to trigger autoscaling.
    def notify_resource_usage(
        self, input_queue_size: int, under_resource_limits: bool
    ) -> None:
        free_slots = self._actor_pool.num_free_slots()
        if input_queue_size > free_slots and under_resource_limits:
            # Try to scale up if work remains in the work queue.
            self._scale_up_if_needed()
        else:
            # Try to remove any idle actors.
            self._scale_down_if_needed()

    def _start_actor(self):
        """Start a new actor and add it to the actor pool as a pending actor."""
        assert self._cls is not None
        ctx = DataContext.get_current()
        actor = self._cls.remote(
            ctx,
            src_fn_name=self.name,
            map_transformer=self._map_transformer,
        )
        res_ref = actor.get_location.remote()

        def _task_done_callback(res_ref):
            # res_ref is a future for a now-ready actor; move actor from pending to the
            # active actor pool.
            has_actor = self._actor_pool.pending_to_running(res_ref)
            if not has_actor:
                # Actor has already been killed.
                return
            # A new actor has started, we try to dispatch queued tasks.
            self._dispatch_tasks()

        self._submit_metadata_task(
            res_ref,
            lambda: _task_done_callback(res_ref),
        )
        self._actor_pool.add_pending_actor(actor, res_ref)

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
            if self._actor_locality_enabled:
                actor = self._actor_pool.pick_actor(self._bundle_queue[0])
            else:
                actor = self._actor_pool.pick_actor()
            if actor is None:
                # No actors available for executing the next task.
                break
            # Submit the map task.
            bundle = self._bundle_queue.popleft()
            input_blocks = [block for block, _ in bundle.blocks]
            ctx = TaskContext(
                task_idx=self._next_data_task_idx,
                target_max_block_size=self.actual_target_max_block_size,
            )
            gen = actor.submit.options(num_returns="streaming", name=self.name).remote(
                DataContext.get_current(), ctx, *input_blocks
            )

            def _task_done_callback(actor_to_return):
                # Return the actor that was running the task to the pool.
                self._actor_pool.return_actor(actor_to_return)
                # Dipsatch more tasks.
                self._dispatch_tasks()

            # For some reason, if we don't define a new variable `actor_to_return`,
            # the following lambda won't capture the correct `actor` variable.
            actor_to_return = actor
            self._submit_data_task(
                gen,
                bundle,
                lambda: _task_done_callback(actor_to_return),
            )

        # Needed in the bulk execution path for triggering autoscaling. This is a
        # no-op in the streaming execution case.
        if self._bundle_queue:
            # Try to scale up if work remains in the work queue.
            self._scale_up_if_needed()
        else:
            # Only try to scale down if the work queue has been fully consumed.
            self._scale_down_if_needed()

    def _scale_up_if_needed(self):
        """Try to scale up the pool if the autoscaling policy allows it."""
        while self._autoscaling_policy.should_scale_up(
            num_total_workers=self._actor_pool.num_total_actors(),
            num_running_workers=self._actor_pool.num_running_actors(),
        ):
            self._start_actor()

    def _scale_down_if_needed(self):
        """Try to scale down the pool if the autoscaling policy allows it."""
        # Kill inactive workers if there's no more work to do.
        self._kill_inactive_workers_if_done()

        while self._autoscaling_policy.should_scale_down(
            num_total_workers=self._actor_pool.num_total_actors(),
            num_idle_workers=self._actor_pool.num_idle_actors(),
        ):
            killed = self._actor_pool.kill_inactive_actor()
            if not killed:
                # This scaledown is best-effort, only killing an inactive worker if an
                # inactive worker exists. If there are no inactive workers to kill, we
                # break out of the scale-down loop.
                break

    def all_inputs_done(self):
        # Call base implementation to handle any leftover bundles. This may or may not
        # trigger task dispatch.
        super().all_inputs_done()

        # Mark inputs as done so future task dispatch will kill all inactive workers
        # once the bundle queue is exhausted.
        self._inputs_done = True

        # Try to scale pool down.
        self._scale_down_if_needed()

    def _kill_inactive_workers_if_done(self):
        if self._inputs_done and not self._bundle_queue:
            # No more tasks will be submitted, so we kill all current and future
            # inactive workers.
            self._actor_pool.kill_all_inactive_actors()

    def shutdown(self):
        # We kill all actors in the pool on shutdown, even if they are busy doing work.
        self._actor_pool.kill_all_actors()
        super().shutdown()

        # Warn if the user specified a batch or block size that prevents full
        # parallelization across the actor pool. We only know this information after
        # execution has completed.
        min_workers = self._autoscaling_policy.min_workers
        if len(self._output_metadata) < min_workers:
            # The user created a stream that has too few blocks to begin with.
            logger.get_logger().warning(
                "To ensure full parallelization across an actor pool of size "
                f"{min_workers}, the Dataset should consist of at least "
                f"{min_workers} distinct blocks. Consider increasing "
                "the parallelism when creating the Dataset."
            )

    def progress_str(self) -> str:
        base = f"{self._actor_pool.num_running_actors()} actors"
        pending = self._actor_pool.num_pending_actors()
        if pending:
            base += f" ({pending} pending)"
        if self._actor_locality_enabled:
            base += " " + locality_string(
                self._actor_pool._locality_hits, self._actor_pool._locality_misses
            )
        else:
            base += " [locality off]"
        return base

    def base_resource_usage(self) -> ExecutionResources:
        min_workers = self._autoscaling_policy.min_workers
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
            object_store_memory=self.metrics.obj_store_mem_cur,
        )

    def incremental_resource_usage(self) -> ExecutionResources:
        # We would only have nonzero incremental CPU/GPU resources if a new task would
        # require scale-up to run.
        if self._autoscaling_policy.should_scale_up(
            num_total_workers=self._actor_pool.num_total_actors(),
            num_running_workers=self._actor_pool.num_running_actors(),
        ):
            # A new task would trigger scale-up, so we include the actor resouce
            # requests in the incremental resources.
            num_cpus = self._ray_remote_args.get("num_cpus", 0)
            num_gpus = self._ray_remote_args.get("num_gpus", 0)
        else:
            # A new task wouldn't trigger scale-up, so we consider the incremental
            # compute resources to be 0.
            num_cpus = 0
            num_gpus = 0
        return ExecutionResources(
            cpu=num_cpus,
            gpu=num_gpus,
            object_store_memory=self._metrics.average_bytes_outputs_per_task,
        )

    def _extra_metrics(self) -> Dict[str, Any]:
        res = {}
        if self._actor_locality_enabled:
            res["locality_hits"] = self._actor_pool._locality_hits
            res["locality_misses"] = self._actor_pool._locality_misses
        return res

    @staticmethod
    def _apply_default_remote_args(ray_remote_args: Dict[str, Any]) -> Dict[str, Any]:
        """Apply defaults to the actor creation remote args."""
        ray_remote_args = ray_remote_args.copy()
        if "scheduling_strategy" not in ray_remote_args:
            ctx = DataContext.get_current()
            ray_remote_args["scheduling_strategy"] = ctx.scheduling_strategy
        # Enable actor fault tolerance by default, with infinite actor recreations and
        # up to N retries per task. The user can customize this in map_batches via
        # extra kwargs (e.g., map_batches(..., max_restarts=0) to disable).
        if "max_restarts" not in ray_remote_args:
            ray_remote_args["max_restarts"] = -1
        if (
            "max_task_retries" not in ray_remote_args
            and ray_remote_args.get("max_restarts") != 0
        ):
            ray_remote_args["max_task_retries"] = -1
        return ray_remote_args


class _MapWorker:
    """An actor worker for MapOperator."""

    def __init__(
        self,
        ctx: DataContext,
        src_fn_name: str,
        map_transformer: MapTransformer,
    ):
        DataContext._set_current(ctx)
        self.src_fn_name: str = src_fn_name
        self._map_transformer = map_transformer
        # Initialize state for this actor.
        self._map_transformer.init()

    def get_location(self) -> NodeIdStr:
        return ray.get_runtime_context().get_node_id()

    def submit(
        self,
        data_context: DataContext,
        ctx: TaskContext,
        *blocks: Block,
    ) -> Iterator[Union[Block, List[BlockMetadata]]]:
        yield from _map_task(
            self._map_transformer,
            data_context,
            ctx,
            *blocks,
        )

    def __repr__(self):
        return f"MapWorker({self.src_fn_name})"


# TODO(Clark): Promote this to a public config once we deprecate the legacy compute
# strategies.
@dataclass
class AutoscalingConfig:
    """Configuration for an autoscaling actor pool."""

    # Minimum number of workers in the actor pool.
    min_workers: int
    # Maximum number of workers in the actor pool.
    max_workers: int
    # Maximum number of tasks that can be in flight for a single worker.
    # TODO(Clark): Have this informed by the prefetch_batches configuration, once async
    # prefetching has been ported to this new actor pool.
    max_tasks_in_flight: int = DEFAULT_MAX_TASKS_IN_FLIGHT
    # Minimum ratio of ready workers to the total number of workers. If the pool is
    # above this ratio, it will be allowed to be scaled up.
    ready_to_total_workers_ratio: float = 0.8
    # Maximum ratio of idle workers to the total number of workers. If the pool goes
    # above this ratio, the pool will be scaled down.
    idle_to_total_workers_ratio: float = 0.5

    def __post_init__(self):
        if self.min_workers < 1:
            raise ValueError("min_workers must be >= 1, got: ", self.min_workers)
        if self.max_workers is not None and self.min_workers > self.max_workers:
            raise ValueError(
                "min_workers must be <= max_workers, got: ",
                self.min_workers,
                self.max_workers,
            )
        if self.max_tasks_in_flight < 1:
            raise ValueError(
                "max_tasks_in_flight must be >= 1, got: ",
                self.max_tasks_in_flight,
            )

    @classmethod
    def from_compute_strategy(cls, compute_strategy: ActorPoolStrategy):
        """Convert a legacy ActorPoolStrategy to an AutoscalingConfig."""
        # TODO(Clark): Remove this once the legacy compute strategies are deprecated.
        assert isinstance(compute_strategy, ActorPoolStrategy)
        return cls(
            min_workers=compute_strategy.min_size,
            max_workers=compute_strategy.max_size,
            max_tasks_in_flight=compute_strategy.max_tasks_in_flight_per_actor
            or DEFAULT_MAX_TASKS_IN_FLIGHT,
            ready_to_total_workers_ratio=compute_strategy.ready_to_total_workers_ratio,
        )


class AutoscalingPolicy:
    """Autoscaling policy for an actor pool, determining when the pool should be scaled
    up and when it should be scaled down.
    """

    def __init__(self, autoscaling_config: "AutoscalingConfig"):
        self._config = autoscaling_config

    @property
    def min_workers(self) -> int:
        """The minimum number of actors that must be in the actor pool."""
        return self._config.min_workers

    @property
    def max_workers(self) -> int:
        """The maximum number of actors that can be added to the actor pool."""
        return self._config.max_workers

    def should_scale_up(self, num_total_workers: int, num_running_workers: int) -> bool:
        """Whether the actor pool should scale up by adding a new actor.

        Args:
            num_total_workers: Total number of workers in actor pool.
            num_running_workers: Number of currently running workers in actor pool.

        Returns:
            Whether the actor pool should be scaled up by one actor.
        """
        # TODO: Replace the ready-to-total-ratio heuristic with a a work queue
        # heuristic such that scale-up is only triggered if the current pool doesn't
        # have enough worker slots to process the work queue.
        # TODO: Use profiling of the bundle arrival rate, worker startup
        # time, and task execution time to tailor the work queue heuristic to the
        # running workload and observed Ray performance. E.g. this could be done via an
        # augmented EMA using a queueing model
        if num_total_workers < self._config.min_workers:
            # The actor pool does not reach the configured minimum size.
            return True
        else:
            return (
                # 1. The actor pool will not exceed the configured maximum size.
                num_total_workers < self._config.max_workers
                # TODO: Remove this once we have a good work queue heuristic and our
                # resource-based backpressure is working well.
                # 2. At least 80% of the workers in the pool have already started.
                # This will ensure that workers will be launched in parallel while
                # bounding the worker pool to requesting 125% of the cluster's
                # available resources.
                and num_total_workers > 0
                and num_running_workers / num_total_workers
                > self._config.ready_to_total_workers_ratio
            )

    def should_scale_down(
        self,
        num_total_workers: int,
        num_idle_workers: int,
    ) -> bool:
        """Whether the actor pool should scale down by terminating an inactive actor.

        Args:
            num_total_workers: Total number of workers in actor pool.
            num_idle_workers: Number of currently idle workers in the actor pool.

        Returns:
            Whether the actor pool should be scaled down by one actor.
        """
        # TODO(Clark): Add an idleness timeout-based scale-down.
        # TODO(Clark): Make the idleness timeout dynamically determined by bundle
        # arrival rate, worker startup time, and task execution time.
        return (
            # 1. The actor pool will not go below the configured minimum size.
            num_total_workers > self._config.min_workers
            # 2. The actor pool contains more than 50% idle workers.
            and num_idle_workers / num_total_workers
            > self._config.idle_to_total_workers_ratio
        )


class _ActorPool:
    """A pool of actors for map task execution.

    This class is in charge of tracking the number of in-flight tasks per actor,
    providing the least heavily loaded actor to the operator, and killing idle
    actors when the operator is done submitting work to the pool.
    """

    def __init__(self, max_tasks_in_flight: int = DEFAULT_MAX_TASKS_IN_FLIGHT):
        self._max_tasks_in_flight = max_tasks_in_flight
        # Number of tasks in flight per actor.
        self._num_tasks_in_flight: Dict[ray.actor.ActorHandle, int] = {}
        # Node id of each ready actor.
        self._actor_locations: Dict[ray.actor.ActorHandle, str] = {}
        # Actors that are not yet ready (still pending creation).
        self._pending_actors: Dict[ObjectRef, ray.actor.ActorHandle] = {}
        # Whether actors that become idle should be eagerly killed. This is False until
        # the first call to kill_idle_actors().
        self._should_kill_idle_actors = False
        # Track locality matching stats.
        self._locality_hits: int = 0
        self._locality_misses: int = 0

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
        self._actor_locations[actor] = ray.get(ready_ref)
        return True

    def pick_actor(
        self, locality_hint: Optional[RefBundle] = None
    ) -> Optional[ray.actor.ActorHandle]:
        """Picks an actor for task submission based on busyness and locality.

        None will be returned if all actors are either at capacity (according to
        max_tasks_in_flight) or are still pending.

        Args:
            locality_hint: Try to pick an actor that is local for this bundle.
        """
        if not self._num_tasks_in_flight:
            # Actor pool is empty or all actors are still pending.
            return None

        if locality_hint:
            preferred_loc = self._get_location(locality_hint)
        else:
            preferred_loc = None

        def penalty_key(actor):
            """Returns the key that should be minimized for the best actor.

            We prioritize valid actors, those with argument locality, and those that
            are not busy, in that order.
            """
            busyness = self._num_tasks_in_flight[actor]
            invalid = busyness >= self._max_tasks_in_flight
            requires_remote_fetch = self._actor_locations[actor] != preferred_loc
            return invalid, requires_remote_fetch, busyness

        actor = min(self._num_tasks_in_flight.keys(), key=penalty_key)
        if self._num_tasks_in_flight[actor] >= self._max_tasks_in_flight:
            # All actors are at capacity.
            return None

        if locality_hint:
            if self._actor_locations[actor] == preferred_loc:
                self._locality_hits += 1
            else:
                self._locality_misses += 1
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

    def num_free_slots(self) -> int:
        """Return the number of free slots for task execution."""
        if not self._num_tasks_in_flight:
            return 0
        return sum(
            max(0, self._max_tasks_in_flight - num_tasks_in_flight)
            for num_tasks_in_flight in self._num_tasks_in_flight.values()
        )

    def num_active_actors(self) -> int:
        """Return the number of actors in the pool with at least one active task."""
        return sum(
            1 if num_tasks_in_flight > 0 else 0
            for num_tasks_in_flight in self._num_tasks_in_flight.values()
        )

    def kill_inactive_actor(self) -> bool:
        """Kills a single pending or idle actor, if any actors are pending/idle.

        Returns whether an inactive actor was actually killed.
        """
        # We prioritize killing pending actors over idle actors to reduce actor starting
        # churn.
        killed = self._maybe_kill_pending_actor()
        if not killed:
            # If no pending actor was killed, so kill actor.
            killed = self._maybe_kill_idle_actor()
        return killed

    def _maybe_kill_pending_actor(self) -> bool:
        if self._pending_actors:
            # At least one pending actor, so kill first one.
            self._kill_pending_actor(next(iter(self._pending_actors.keys())))
            return True
        # No pending actors, so indicate to the caller that no actors were killed.
        return False

    def _maybe_kill_idle_actor(self) -> bool:
        for actor, tasks_in_flight in self._num_tasks_in_flight.items():
            if tasks_in_flight == 0:
                # At least one idle actor, so kill first one found.
                self._kill_running_actor(actor)
                return True
        # No idle actors, so indicate to the caller that no actors were killed.
        return False

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

    def _get_location(self, bundle: RefBundle) -> Optional[NodeIdStr]:
        """Ask Ray for the node id of the given bundle.

        This method may be overriden for testing.

        Returns:
            A node id associated with the bundle, or None if unknown.
        """
        return bundle.get_cached_location()
