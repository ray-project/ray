import logging
import time
import uuid
import warnings
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import ray
from ray.actor import ActorHandle
from ray.core.generated import gcs_pb2
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.autoscaler import AutoscalingActorPool
from ray.data._internal.execution.bundle_queue import create_bundle_queue
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    NodeIdStr,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.physical_operator import _ActorPoolInfo
from ray.data._internal.execution.operators.map_operator import MapOperator, _map_task
from ray.data._internal.execution.operators.map_transformer import MapTransformer
from ray.data._internal.execution.util import locality_string
from ray.data._internal.remote_fn import _add_system_error_to_retry_exceptions
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef
from ray.util.common import INT32_MAX

logger = logging.getLogger(__name__)

# Higher values here are better for prefetching and locality. It's ok for this to be
# fairly high since streaming backpressure prevents us from overloading actors.
DEFAULT_MAX_TASKS_IN_FLIGHT = 4


class ActorPoolMapOperator(MapOperator):
    """A MapOperator implementation that executes tasks on an actor pool.

    NOTE: This class is NOT thread-safe

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
        data_context: DataContext,
        target_max_block_size: Optional[int],
        compute_strategy: ActorPoolStrategy,
        name: str = "ActorPoolMap",
        min_rows_per_bundle: Optional[int] = None,
        supports_fusion: bool = True,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Create an ActorPoolMapOperator instance.

        Args:
            transform_fn: The function to apply to each ref bundle input.
            init_fn: The callable class to instantiate on each actor.
            input_op: Operator generating input data for this op.
            compute_strategy: ComputeStrategy used for this operator.
            name: The name of this operator.
            target_max_block_size: The target maximum number of bytes to
                include in an output block.
            min_rows_per_bundle: The number of rows to gather per batch passed to the
                transform_fn, or None to use the block size. Setting the batch size is
                important for the performance of GPU-accelerated transform functions.
                The actual rows passed may be less if the dataset is small.
            supports_fusion: Whether this operator supports fusion with other operators.
            ray_remote_args_fn: A function that returns a dictionary of remote args
                passed to each map worker. The purpose of this argument is to generate
                dynamic arguments for each actor/task, and will be called each time
                prior to initializing the worker. Args returned from this dict will
                always override the args in ``ray_remote_args``. Note: this is an
                advanced, experimental feature.
            ray_remote_args: Customize the ray remote args for this op's tasks.
                See :func:`ray.remote` for details.
        """
        super().__init__(
            map_transformer,
            input_op,
            data_context,
            name,
            target_max_block_size,
            min_rows_per_bundle,
            supports_fusion,
            ray_remote_args_fn,
            ray_remote_args,
        )
        self._ray_actor_task_remote_args = {}
        actor_task_errors = self.data_context.actor_task_retry_on_errors
        if actor_task_errors:
            self._ray_actor_task_remote_args["retry_exceptions"] = actor_task_errors
        _add_system_error_to_retry_exceptions(self._ray_actor_task_remote_args)

        if (
            "_generator_backpressure_num_objects"
            not in self._ray_actor_task_remote_args
            and self.data_context._max_num_blocks_in_streaming_gen_buffer is not None
        ):
            # The `_generator_backpressure_num_objects` parameter should be
            # `2 * _max_num_blocks_in_streaming_gen_buffer` because we yield
            # 2 objects for each block: the block and the block metadata.
            self._ray_actor_task_remote_args["_generator_backpressure_num_objects"] = (
                2 * self.data_context._max_num_blocks_in_streaming_gen_buffer
            )

        self._min_rows_per_bundle = min_rows_per_bundle
        self._ray_remote_args_fn = ray_remote_args_fn
        self._ray_remote_args = self._apply_default_remote_args(
            self._ray_remote_args, self.data_context
        )

        per_actor_resource_usage = ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0),
            gpu=self._ray_remote_args.get("num_gpus", 0),
        )
        self._actor_pool = _ActorPool(
            compute_strategy,
            self._start_actor,
            per_actor_resource_usage,
            self.data_context._enable_actor_pool_on_exit_hook,
        )
        # A queue of bundles awaiting dispatch to actors.
        self._bundle_queue = create_bundle_queue()
        # Cached actor class.
        self._cls = None
        # Whether no more submittable bundles will be added.
        self._inputs_done = False

    def internal_queue_size(self) -> int:
        # NOTE: Internal queue size for ``ActorPoolMapOperator`` includes both
        #   - Input blocks bundler, alas
        #   - Own bundle's queue
        return self._block_ref_bundler.num_bundles() + len(self._bundle_queue)

    def completed(self) -> bool:
        # TODO separate marking as completed from the check
        return (
            self._inputs_complete
            and self._bundle_queue.is_empty()
            and super().completed()
        )

    def start(self, options: ExecutionOptions):
        self._actor_locality_enabled = options.actor_locality_enabled
        super().start(options)

        # Create the actor workers and add them to the pool.
        self._cls = ray.remote(**self._ray_remote_args)(_MapWorker)
        self._actor_pool.scale_up(
            self._actor_pool.min_size(), reason="scaling to min size"
        )

        # If `wait_for_min_actors_s` is specified and is positive, then
        # Actor Pool will block until min number of actors is provisioned.
        #
        # Otherwise, all actors will be provisioned asynchronously.
        if self.data_context.wait_for_min_actors_s > 0:
            refs = self._actor_pool.get_pending_actor_refs()

            logger.debug(
                f"{self._name}: Waiting for {len(refs)} pool actors to start "
                f"(for {self.data_context.wait_for_min_actors_s}s)..."
            )

            try:
                timeout = self.data_context.wait_for_min_actors_s
                ray.get(refs, timeout=timeout)
            except ray.exceptions.GetTimeoutError:
                raise ray.exceptions.GetTimeoutError(
                    "Timed out while starting actors. "
                    "This may mean that the cluster does not have "
                    "enough resources for the requested actor pool."
                )

    def should_add_input(self) -> bool:
        return self._actor_pool.num_free_slots() > 0

    def _start_actor(self, labels: Dict[str, str]) -> Tuple[ActorHandle, ObjectRef]:
        """Start a new actor and add it to the actor pool as a pending actor.

        Args:
            labels: The key-value labels to launch the actor with.

        Returns:
            A tuple of the actor handle and the object ref to the actor's location.
        """
        assert self._cls is not None
        ctx = self.data_context
        if self._ray_remote_args_fn:
            self._refresh_actor_cls()
        actor = self._cls.options(
            _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **labels}
        ).remote(
            ctx,
            src_fn_name=self.name,
            map_transformer=self._map_transformer,
        )
        res_ref = actor.get_location.options(name=f"{self.name}.get_location").remote()

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
        return actor, res_ref

    def _add_bundled_input(self, bundle: RefBundle):
        self._bundle_queue.add(bundle)
        self._metrics.on_input_queued(bundle)
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
                actor = self._actor_pool.pick_actor(self._bundle_queue.peek())
            else:
                actor = self._actor_pool.pick_actor()
            if actor is None:
                # No actors available for executing the next task.
                break
            # Submit the map task.
            bundle = self._bundle_queue.pop()
            self._metrics.on_input_dequeued(bundle)
            input_blocks = [block for block, _ in bundle.blocks]
            ctx = TaskContext(
                task_idx=self._next_data_task_idx,
                op_name=self.name,
                target_max_block_size=self.actual_target_max_block_size,
            )
            gen = actor.submit.options(
                num_returns="streaming",
                name=f"{self.name}.submit",
                **self._ray_actor_task_remote_args,
            ).remote(
                self.data_context,
                ctx,
                *input_blocks,
                **self.get_map_task_kwargs(),
            )

            def _task_done_callback(actor_to_return):
                # Return the actor that was running the task to the pool.
                self._actor_pool.return_actor(actor_to_return)
                # Dipsatch more tasks.
                self._dispatch_tasks()

            from functools import partial

            self._submit_data_task(
                gen, bundle, partial(_task_done_callback, actor_to_return=actor)
            )

    def _refresh_actor_cls(self):
        """When `self._ray_remote_args_fn` is specified, this method should
        be called prior to initializing the new worker in order to get new
        remote args passed to the worker. It updates `self.cls` with the same
        `_MapWorker` class, but with the new remote args from
        `self._ray_remote_args_fn`."""
        assert self._ray_remote_args_fn, "_ray_remote_args_fn must be provided"
        remote_args = self._ray_remote_args.copy()
        new_remote_args = self._ray_remote_args_fn()

        # Override args from user-defined remote args function.
        new_and_overriden_remote_args = {}
        for k, v in new_remote_args.items():
            remote_args[k] = v
            new_and_overriden_remote_args[k] = v
        self._cls = ray.remote(**remote_args)(_MapWorker)
        return new_and_overriden_remote_args

    def all_inputs_done(self):
        # Call base implementation to handle any leftover bundles. This may or may not
        # trigger task dispatch.
        super().all_inputs_done()

        # Mark inputs as done so future task dispatch will kill all inactive workers
        # once the bundle queue is exhausted.
        self._inputs_done = True

        if self._metrics.num_inputs_received < self._actor_pool.min_size():
            warnings.warn(
                f"The minimum number of concurrent actors for '{self.name}' is set to "
                f"{self._actor_pool.min_size()}, but the operator only received "
                f"{self._metrics.num_inputs_received} input(s). This means that the "
                f"operator can launch at most {self._metrics.num_inputs_received} "
                f"task(s), and won't fully utilize the available concurrency. "
                "You might be able to increase the number of concurrent tasks by "
                "configuring `override_num_blocks` earlier in the pipeline."
            )

    def _do_shutdown(self, force: bool = False):
        self._actor_pool.shutdown(force=force)
        # NOTE: It's critical for Actor Pool to release actors before calling into
        #       the base method that will attempt to cancel and join pending.
        super()._do_shutdown(force)

    def progress_str(self) -> str:
        if self._actor_locality_enabled:
            return locality_string(
                self._actor_pool._locality_hits,
                self._actor_pool._locality_misses,
            )
        return "[locality off]"

    def min_max_resource_requirements(
        self,
    ) -> Tuple[ExecutionResources, ExecutionResources]:
        min_actors = self._actor_pool.min_size()
        assert min_actors is not None, min_actors

        num_cpus_per_actor = self._ray_remote_args.get("num_cpus", 0)
        num_gpus_per_actor = self._ray_remote_args.get("num_gpus", 0)
        memory_per_actor = self._ray_remote_args.get("memory", 0)

        min_resource_usage = ExecutionResources(
            cpu=num_cpus_per_actor * min_actors,
            gpu=num_gpus_per_actor * min_actors,
            memory=memory_per_actor * min_actors,
            # To ensure that all actors are utilized, reserve enough resource budget
            # to launch one task for each worker.
            object_store_memory=self._metrics.obj_store_mem_max_pending_output_per_task
            * min_actors,
        )

        return min_resource_usage, ExecutionResources.for_limits()

    def current_processor_usage(self) -> ExecutionResources:
        # Both pending and running actors count towards our current resource usage.
        num_active_workers = self._actor_pool.current_size()
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0) * num_active_workers,
            gpu=self._ray_remote_args.get("num_gpus", 0) * num_active_workers,
        )

    def pending_processor_usage(self) -> ExecutionResources:
        # Both pending and restarting actors count towards pending processor usage
        num_pending_workers = (
            self._actor_pool.num_pending_actors()
            + self._actor_pool.num_restarting_actors()
        )
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0) * num_pending_workers,
            gpu=self._ray_remote_args.get("num_gpus", 0) * num_pending_workers,
        )

    def incremental_resource_usage(self) -> ExecutionResources:
        # Submitting tasks to existing actors doesn't require additional
        # CPU/GPU resources.
        return ExecutionResources(
            cpu=0,
            gpu=0,
            object_store_memory=self._metrics.obj_store_mem_max_pending_output_per_task
            or 0,
        )

    def _extra_metrics(self) -> Dict[str, Any]:
        res = {}
        if self._actor_locality_enabled:
            res["locality_hits"] = self._actor_pool._locality_hits
            res["locality_misses"] = self._actor_pool._locality_misses
        res["pending_actors"] = self._actor_pool.num_pending_actors()
        res["restarting_actors"] = self._actor_pool.num_restarting_actors()
        return res

    @staticmethod
    def _apply_default_remote_args(
        ray_remote_args: Dict[str, Any], data_context: DataContext
    ) -> Dict[str, Any]:
        """Apply defaults to the actor creation remote args."""
        ray_remote_args = ray_remote_args.copy()
        if "scheduling_strategy" not in ray_remote_args:
            ray_remote_args["scheduling_strategy"] = data_context.scheduling_strategy
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

    def get_autoscaling_actor_pools(self) -> List[AutoscalingActorPool]:
        return [self._actor_pool]

    def update_resource_usage(self) -> None:
        """Updates resources usage."""
        for actor in self._actor_pool.get_running_actor_refs():
            actor_state = actor._get_local_state()
            if actor_state in (None, gcs_pb2.ActorTableData.ActorState.DEAD):
                # actor._get_local_state can return None if the state is Unknown
                # If actor_state is None or dead, there is nothing to do.
                if actor_state == gcs_pb2.ActorTableData.ActorState.DEAD:
                    # Indefinite task retries have been disabled.
                    assert self._ray_remote_args["max_restarts"] != -1
                continue
            elif actor_state != gcs_pb2.ActorTableData.ActorState.ALIVE:
                # The actors can be either ALIVE or RESTARTING here because they will
                # be restarted indefinitely until execution finishes.
                assert (
                    actor_state == gcs_pb2.ActorTableData.ActorState.RESTARTING
                ), actor_state
                self._actor_pool.update_running_actor_state(actor, True)
            else:
                self._actor_pool.update_running_actor_state(actor, False)

    def get_actor_info(self) -> _ActorPoolInfo:
        """Returns Actor counts for Alive, Restarting and Pending Actors."""
        return self._actor_pool.get_actor_info()


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
        **kwargs: Dict[str, Any],
    ) -> Iterator[Union[Block, List[BlockMetadata]]]:
        yield from _map_task(
            self._map_transformer,
            data_context,
            ctx,
            *blocks,
            **kwargs,
        )

    def __repr__(self):
        return f"MapWorker({self.src_fn_name})"

    def on_exit(self):
        """Called when the actor is about to exist.
        This enables performing cleanup operations via `UDF.__del__`.

        Note, this only ensures cleanup is performed when the job exists gracefully.
        If the driver or the actor is forcefully killed, `__del__` will not be called.
        """
        # `_map_actor_context` is a global variable that references the UDF object.
        # Delete it to trigger `UDF.__del__`.
        del ray.data._map_actor_context
        ray.data._map_actor_context = None


@dataclass
class _ActorState:
    """Actor state"""

    # Number of tasks in flight per actor
    num_tasks_in_flight: int

    # Node id of each ready actor
    actor_location: str

    # Is Actor state restarting or alive
    is_restarting: bool


class _ActorPool(AutoscalingActorPool):
    """A pool of actors for map task execution.

    This class is in charge of tracking the number of in-flight tasks per actor,
    providing the least heavily loaded actor to the operator, and killing idle
    actors when the operator is done submitting work to the pool.
    """

    _ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S = 30
    _ACTOR_POOL_GRACEFUL_SHUTDOWN_TIMEOUT_S = 30
    _LOGICAL_ACTOR_ID_LABEL_KEY = "__ray_data_logical_actor_id"

    def __init__(
        self,
        compute_strategy: ActorPoolStrategy,
        create_actor_fn: Callable[[Dict[str, str]], Tuple[ActorHandle, ObjectRef[Any]]],
        per_actor_resource_usage: ExecutionResources,
        _enable_actor_pool_on_exit_hook: bool = False,
    ):
        """Initialize the actor pool.

        Args:
            compute_strategy: The autoscaling configuration to use.
            create_actor_fn: This function should take key-value labels as input, and
                create an actor with those labels. The function should return the actor
                handle and a reference to the actor's node ID.
            per_actor_resource_usage: The resource usage per actor.
            _enable_actor_pool_on_exit_hook: Whether to enable the actor pool on exit
                hook.
        """

        self._min_size: int = compute_strategy.min_size
        self._max_size: int = compute_strategy.max_size
        self._max_tasks_in_flight: int = (
            compute_strategy.max_tasks_in_flight_per_actor
            or DEFAULT_MAX_TASKS_IN_FLIGHT
        )
        self._create_actor_fn = create_actor_fn
        self._per_actor_resource_usage = per_actor_resource_usage
        assert self._min_size >= 1
        assert self._max_size >= self._min_size
        assert self._max_tasks_in_flight >= 1
        assert self._create_actor_fn is not None

        # Timestamp of the last scale up action
        self._last_scaling_up_ts: Optional[float] = None
        # Actors that have started running, including alive and restarting actors.
        self._running_actors: Dict[ray.actor.ActorHandle, _ActorState] = {}
        # Actors that are not yet ready (still pending creation).
        self._pending_actors: Dict[ObjectRef, ray.actor.ActorHandle] = {}
        # Map from actor handle to its logical ID.
        self._actor_to_logical_id: Dict[ray.actor.ActorHandle, str] = {}
        # Track locality matching stats.
        self._locality_hits: int = 0
        self._locality_misses: int = 0
        self._enable_actor_pool_on_exit_hook = _enable_actor_pool_on_exit_hook

    # === Overriding methods of AutoscalingActorPool ===

    def min_size(self) -> int:
        return self._min_size

    def max_size(self) -> int:
        return self._max_size

    def current_size(self) -> int:
        return self.num_pending_actors() + self.num_running_actors()

    def num_running_actors(self) -> int:
        return len(self._running_actors)

    def num_restarting_actors(self) -> int:
        """Restarting actors are all the running actors not in ALIVE state."""
        return sum(
            actor_state.is_restarting for actor_state in self._running_actors.values()
        )

    def num_active_actors(self) -> int:
        """Active actors are all the running actors with inflight tasks."""
        return sum(
            1 if actor_state.num_tasks_in_flight > 0 else 0
            for actor_state in self._running_actors.values()
        )

    def num_alive_actors(self) -> int:
        """Alive actors are all the running actors in ALIVE state."""
        return sum(
            not actor_state.is_restarting
            for actor_state in self._running_actors.values()
        )

    def num_pending_actors(self) -> int:
        return len(self._pending_actors)

    def max_tasks_in_flight_per_actor(self) -> int:
        return self._max_tasks_in_flight

    def current_in_flight_tasks(self) -> int:
        return sum(
            actor_state.num_tasks_in_flight
            for actor_state in self._running_actors.values()
        )

    def can_scale_down(self):
        """Returns whether Actor Pool is able to scale down.

        To prevent bouncing back and forth, we disallow scale down for
        a "cool-off" period after the most recent scaling up, with an intention
        to allow application to actually utilize newly provisioned resources
        before making decisions on subsequent actions.

        Note that this action is unidirectional and doesn't apply to
        scaling up, ie if actor pool just scaled down, it'd still be able
        to scale back up immediately.
        """

        return (
            self._last_scaling_up_ts is None
            or time.time()
            >= self._last_scaling_up_ts + self._ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S
        )

    def scale_up(self, num_actors: int, *, reason: Optional[str] = None) -> int:
        logger.info(
            f"Scaling up actor pool by {num_actors} "
            f"(reason={reason}, {self.get_actor_info()})"
        )

        for _ in range(num_actors):
            actor, ready_ref = self._create_actor()
            self.add_pending_actor(actor, ready_ref)

        # Capture last scale up timestamp
        self._last_scaling_up_ts = time.time()

        return num_actors

    def _create_actor(self) -> Tuple[ray.actor.ActorHandle, ObjectRef]:
        logical_actor_id = str(uuid.uuid4())
        labels = {self.get_logical_id_label_key(): logical_actor_id}
        actor, ready_ref = self._create_actor_fn(labels)
        self._actor_to_logical_id[actor] = logical_actor_id
        return actor, ready_ref

    def scale_down(self, num_actors: int, *, reason: Optional[str] = None) -> int:
        num_released = 0
        for _ in range(num_actors):
            if self._remove_inactive_actor():
                num_released += 1

        if num_released > 0:
            logger.info(
                f"Scaled down actor pool by {num_released} "
                f"(reason={reason}; {self.get_actor_info()})"
            )

        return num_released

    # === End of overriding methods of AutoscalingActorPool ===

    def update_running_actor_state(
        self, actor: ray.actor.ActorHandle, is_restarting: bool
    ):
        """Update running actor state.

        Args:
            actor: The running actor that needs state update.
            is_restarting: Whether running actor is restarting or alive.
        """
        assert actor in self._running_actors
        self._running_actors[actor].is_restarting = is_restarting

    def add_pending_actor(self, actor: ray.actor.ActorHandle, ready_ref: ray.ObjectRef):
        """Adds a pending actor to the pool.

        This actor won't be pickable until it is marked as running via a
        pending_to_running() call.

        Args:
            actor: The not-yet-ready actor to add as pending to the pool.
            ready_ref: The ready future for the actor.
        """
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
            # The actor has been removed from the pool before becoming running.
            return False
        actor = self._pending_actors.pop(ready_ref)
        self._running_actors[actor] = _ActorState(
            num_tasks_in_flight=0,
            actor_location=ray.get(ready_ref),
            is_restarting=False,
        )
        return True

    def pick_actor(
        self, bundle: Optional[RefBundle] = None
    ) -> Optional[ray.actor.ActorHandle]:
        """Picks an actor for task submission based on busyness and locality.

        None will be returned if all actors are either at capacity (according to
        max_tasks_in_flight) or are still pending.

        Args:
            bundle: Try to pick an actor that is local for this bundle.
        """
        if not self._running_actors:
            # Actor pool is empty or all actors are still pending.
            return None

        # Filter out actors that are invalid, i.e. actors with number of tasks in
        # flight >= _max_tasks_in_flight or actor_state is not ALIVE.
        valid_actors = [
            actor
            for actor in self._running_actors
            if self._running_actors[actor].num_tasks_in_flight
            < self._max_tasks_in_flight
            and not self._running_actors[actor].is_restarting
        ]

        if not valid_actors:
            # All actors are at capacity or actor state is not ALIVE.
            return None

        # Rank all valid actors
        ranks = self._rank_actors(valid_actors, bundle)

        assert len(ranks) == len(valid_actors), f"{len(ranks)} != {len(valid_actors)}"

        # Pick the actor with the highest rank (lower value, higher rank)
        target_actor_idx = min(range(len(valid_actors)), key=lambda idx: ranks[idx])

        target_actor = valid_actors[target_actor_idx]
        locality_rank, _ = ranks[target_actor_idx]

        if bundle and locality_rank != INT32_MAX:
            self._locality_hits += 1
        else:
            self._locality_misses += 1

        self._running_actors[target_actor].num_tasks_in_flight += 1

        return target_actor

    def _rank_actors(
        self,
        actors: List[ActorHandle],
        bundle: Optional[RefBundle],
    ) -> List[Tuple[int, int]]:
        """Return ranks for each actor based on node affinity with the blocks in the provided
        bundle and current Actor's load.

        The rank for each actor is a tuple of

            1. Locality rank: a rank of a node Actor is scheduled on determined based on
            the ranking of preferred locations for provided ``RefBundle`` (defined by
            ``RefBundle.get_preferred_locations``). Lower is better.
            2. Number of tasks currently executed by Actor. Lower is better.

        Args:
            actors: List of actors to rank
            bundle: Optional bundle whose locality preferences should be considered

        Returns:
            List of (locality_rank, num_tasks) tuples, one per input actor
        """
        locs_priorities = (
            {
                # NOTE: We're negating total bytes to maintain an invariant
                #       of the rank used -- lower value corresponding to a higher rank
                node_id: -total_bytes
                for node_id, total_bytes in bundle.get_preferred_object_locations().items()
            }
            if bundle is not None
            else {}
        )

        ranks = [
            (
                # Priority/rank of the location (based on the object size).
                # Defaults to int32 max value (ie no rank)
                locs_priorities.get(
                    self._running_actors[actor].actor_location, INT32_MAX
                ),
                # Number of tasks currently in flight at the given actor
                self._running_actors[actor].num_tasks_in_flight,
            )
            for actor in actors
        ]

        return ranks

    def return_actor(self, actor: ray.actor.ActorHandle):
        """Returns the provided actor to the pool."""
        assert actor in self._running_actors
        assert self._running_actors[actor].num_tasks_in_flight > 0
        self._running_actors[actor].num_tasks_in_flight -= 1

    def get_pending_actor_refs(self) -> List[ray.ObjectRef]:
        return list(self._pending_actors.keys())

    def get_running_actor_refs(self) -> List[ray.ObjectRef]:
        return list(self._running_actors.keys())

    def get_logical_ids(self) -> List[str]:
        """Get the logical IDs for pending and running actors in the actor pool.

        We can’t use Ray Core actor IDs because we need to identify actors by labels,
        but labels must be set before creation, and actor IDs aren’t available until
        after.
        """
        return list(self._actor_to_logical_id.values())

    def get_logical_id_label_key(self) -> str:
        """Get the label key for the logical actor ID.

        Actors launched by this pool should have this label.
        """
        return self._LOGICAL_ACTOR_ID_LABEL_KEY

    def num_idle_actors(self) -> int:
        """Return the number of idle actors in the pool."""
        return sum(
            1 if running_actor.num_tasks_in_flight == 0 else 0
            for running_actor in self._running_actors.values()
        )

    def num_free_slots(self) -> int:
        """Return the number of free slots for task execution."""
        if not self._running_actors:
            return 0
        return sum(
            max(0, self._max_tasks_in_flight - running_actor.num_tasks_in_flight)
            for running_actor in self._running_actors.values()
        )

    def _remove_inactive_actor(self) -> bool:
        """Kills a single pending or idle actor, if any actors are pending/idle.

        Returns whether an inactive actor was actually released.
        """
        # We prioritize killing pending actors over idle actors to reduce actor starting
        # churn.
        released = self._try_remove_pending_actor()
        if not released:
            # If no pending actor was released, so kill actor.
            released = self._try_remove_idle_actor()
        return released

    def _try_remove_pending_actor(self) -> bool:
        if self._pending_actors:
            # At least one pending actor, so kill first one.
            ready_ref = next(iter(self._pending_actors.keys()))
            actor = self._pending_actors.pop(ready_ref)
            del self._actor_to_logical_id[actor]
            return True
        # No pending actors, so indicate to the caller that no actors were killed.
        return False

    def _try_remove_idle_actor(self) -> bool:
        for actor, state in self._running_actors.items():
            if state.num_tasks_in_flight == 0:
                # At least one idle actor, so kill first one found.
                # NOTE: This is a fire-and-forget op
                self._release_running_actor(actor)
                return True
        # No idle actors, so indicate to the caller that no actors were killed.
        return False

    def shutdown(self, force: bool = False):
        """Kills all actors, including running/active actors.

        This is called once the operator is shutting down.
        """
        self._release_pending_actors(force=force)
        self._release_running_actors(force=force)

    def _release_pending_actors(self, force: bool):
        # Release pending actors from the set of pending ones
        pending = dict(self._pending_actors)
        self._pending_actors.clear()

        if force:
            for _, actor in pending.items():
                # NOTE: Actors can't be brought back after being ``ray.kill``-ed,
                #       hence we're only doing that if this is a forced release
                ray.kill(actor)

    def _release_running_actors(self, force: bool):
        running = list(self._running_actors.keys())

        on_exit_refs = []

        # First release actors and collect their shutdown hook object-refs
        for actor in running:
            ref = self._release_running_actor(actor)
            if ref:
                on_exit_refs.append(ref)

        # Wait for all actors to shutdown gracefully before killing them
        ray.wait(on_exit_refs, timeout=self._ACTOR_POOL_GRACEFUL_SHUTDOWN_TIMEOUT_S)

        # NOTE: Actors can't be brought back after being ``ray.kill``-ed,
        #       hence we're only doing that if this is a forced release
        if force:
            for actor in running:
                ray.kill(actor)

    def _release_running_actor(
        self, actor: ray.actor.ActorHandle
    ) -> Optional[ObjectRef]:
        """Remove the given actor from the pool and trigger its `on_exit` callback.

        This method returns a ``ref`` to the result
        """
        # NOTE: By default, we remove references to the actor and let ref counting
        # garbage collect the actor, instead of using ray.kill.
        #
        # Otherwise, actor cannot be reconstructed for the purposes of produced
        # object's lineage reconstruction.
        if actor not in self._running_actors:
            return None

        if self._enable_actor_pool_on_exit_hook:
            # Call `on_exit` to trigger `UDF.__del__` which may perform
            # cleanup operations.
            ref = actor.on_exit.remote()
        else:
            ref = None
        del self._running_actors[actor]
        del self._actor_to_logical_id[actor]

        return ref

    def get_actor_info(self) -> _ActorPoolInfo:
        """Returns current snapshot of actors' being used in the pool"""
        return _ActorPoolInfo(
            running=self.num_alive_actors(),
            pending=self.num_pending_actors(),
            restarting=self.num_restarting_actors(),
        )

    def per_actor_resource_usage(self) -> ExecutionResources:
        """Per actor resource usage."""
        return self._per_actor_resource_usage
