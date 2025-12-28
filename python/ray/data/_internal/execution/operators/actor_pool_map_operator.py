import abc
import logging
import time
import uuid
import warnings
from abc import abstractmethod
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

if TYPE_CHECKING:
    import pyarrow as pa

import ray
from ray.actor import ActorHandle
from ray.core.generated import gcs_pb2
from ray.data._internal.actor_autoscaler import (
    AutoscalingActorPool,
)
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    ActorPoolScalingRequest,
)
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.bundle_queue import create_bundle_queue
from ray.data._internal.execution.bundle_queue.bundle_queue import BundleQueue
from ray.data._internal.execution.interfaces import (
    BlockSlice,
    ExecutionOptions,
    ExecutionResources,
    NodeIdStr,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.physical_operator import _ActorPoolInfo
from ray.data._internal.execution.node_trackers.actor_location import (
    ActorLocationTracker,
    get_or_create_actor_location_tracker,
)
from ray.data._internal.execution.operators.map_operator import (
    BaseRefBundler,
    MapOperator,
    _map_task,
)
from ray.data._internal.execution.operators.map_transformer import MapTransformer
from ray.data._internal.execution.util import locality_string
from ray.data._internal.remote_fn import _add_system_error_to_retry_exceptions
from ray.data.block import Block, BlockMetadata
from ray.data.context import (
    DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR,
    DataContext,
)
from ray.types import ObjectRef
from ray.util.common import INT32_MAX

logger = logging.getLogger(__name__)


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
        compute_strategy: ActorPoolStrategy,
        name: str = "ActorPoolMap",
        min_rows_per_bundle: Optional[int] = None,
        ref_bundler: Optional[BaseRefBundler] = None,
        supports_fusion: bool = True,
        map_task_kwargs: Optional[Dict[str, Any]] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        ray_actor_task_remote_args: Optional[Dict[str, Any]] = None,
        target_max_block_size_override: Optional[int] = None,
        on_start: Optional[Callable[[Optional["pa.Schema"]], None]] = None,
    ):
        """Create an ActorPoolMapOperator instance.

        Args:
            map_transformer: Instance of `MapTransformer` that will be applied
                to each ref bundle input.
            input_op: Operator generating input data for this op.
            data_context: The DataContext instance containing configuration settings.
            compute_strategy: `ComputeStrategy` used for this operator.
            name: The name of this operator.
            min_rows_per_bundle: The number of rows to gather per batch passed to the
                transform_fn, or None to use the block size. Setting the batch size is
                important for the performance of GPU-accelerated transform functions.
                The actual rows passed may be less if the dataset is small.
            ref_bundler: The ref bundler to use for this operator.
            supports_fusion: Whether this operator supports fusion with other operators.
            map_task_kwargs: A dictionary of kwargs to pass to the map task. You can
                access these kwargs through the `TaskContext.kwargs` dictionary.
            ray_remote_args_fn: A function that returns a dictionary of remote args
                passed to each map worker. The purpose of this argument is to generate
                dynamic arguments for each actor/task, and will be called each time
                prior to initializing the worker. Args returned from this dict will
                always override the args in ``ray_remote_args``. Note: this is an
                advanced, experimental feature.
            ray_remote_args: Customize the ray remote args for this op's tasks.
                See :func:`ray.remote` for details.
            ray_actor_task_remote_args: Ray Core options passed to map actor tasks.
            target_max_block_size_override: The target maximum number of bytes to
                include in an output block.
            on_start: Optional callback invoked with the schema from the first input
                bundle before any tasks are submitted.
        """
        super().__init__(
            map_transformer,
            input_op,
            data_context,
            name,
            target_max_block_size_override,
            min_rows_per_bundle,
            ref_bundler,
            supports_fusion,
            map_task_kwargs,
            ray_remote_args_fn,
            ray_remote_args,
            on_start,
        )

        self._min_rows_per_bundle = min_rows_per_bundle
        self._ray_remote_args_fn = ray_remote_args_fn
        self._ray_remote_args = self._apply_default_remote_args(
            self._ray_remote_args, self.data_context
        )
        self._ray_actor_task_remote_args = self._apply_default_actor_task_remote_args(
            ray_actor_task_remote_args, self.data_context
        )

        per_actor_resource_usage = ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus"),
            gpu=self._ray_remote_args.get("num_gpus"),
            memory=self._ray_remote_args.get("memory"),
        )

        max_actor_concurrency = self._ray_remote_args.get("max_concurrency", 1)

        self._actor_pool = _ActorPool(
            self._start_actor,
            per_actor_resource_usage,
            min_size=compute_strategy.min_size,
            max_size=compute_strategy.max_size,
            initial_size=compute_strategy.initial_size,
            max_actor_concurrency=max_actor_concurrency,
            max_tasks_in_flight_per_actor=(
                # Unless explicitly overridden by the user, max tasks-in-flight config
                # will fall back to be:
                #
                #   DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR * max_concurrency,
                compute_strategy.max_tasks_in_flight_per_actor
                or data_context.max_tasks_in_flight_per_actor
                or max_actor_concurrency
                * DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR
            ),
            _enable_actor_pool_on_exit_hook=self.data_context._enable_actor_pool_on_exit_hook,
        )
        self._actor_task_selector = self._create_task_selector(self._actor_pool)
        # A queue of bundles awaiting dispatch to actors.
        self._bundle_queue = create_bundle_queue()
        # HACK: Without this, all actors show up as `_MapWorker` in Grafana, so we can’t
        # tell which operator they belong to. To fix that, we dynamically create a new
        # class per operator with a unique name.
        self._map_worker_cls = type(f"MapWorker({self.name})", (_MapWorker,), {})
        # Cached actor class.
        self._actor_cls = None
        # Whether no more submittable bundles will be added.
        self._inputs_done = False
        self._actor_locality_enabled: Optional[bool] = None

        # Locality metrics
        self._locality_hits = 0
        self._locality_misses = 0

    @staticmethod
    def _create_task_selector(actor_pool: "_ActorPool") -> "_ActorTaskSelector":
        return _ActorTaskSelectorImpl(actor_pool)

    @staticmethod
    def _apply_default_actor_task_remote_args(
        ray_actor_task_remote_args: Optional[Dict[str, Any]], data_context: DataContext
    ) -> Dict[str, Any]:
        """Apply defaults to the actor task remote args."""
        if ray_actor_task_remote_args is None:
            ray_actor_task_remote_args = {}

        ray_actor_task_remote_args = ray_actor_task_remote_args.copy()

        actor_task_errors = data_context.actor_task_retry_on_errors
        if actor_task_errors:
            ray_actor_task_remote_args["retry_exceptions"] = actor_task_errors
        _add_system_error_to_retry_exceptions(ray_actor_task_remote_args)

        if (
            "_generator_backpressure_num_objects" not in ray_actor_task_remote_args
            and data_context._max_num_blocks_in_streaming_gen_buffer is not None
        ):
            # The `_generator_backpressure_num_objects` parameter should be
            # `2 * _max_num_blocks_in_streaming_gen_buffer` because we yield
            # 2 objects for each block: the block and the block metadata.
            ray_actor_task_remote_args["_generator_backpressure_num_objects"] = (
                2 * data_context._max_num_blocks_in_streaming_gen_buffer
            )

        return ray_actor_task_remote_args

    def internal_input_queue_num_blocks(self) -> int:
        # NOTE: Internal queue size for ``ActorPoolMapOperator`` includes both
        #   - Input blocks bundler, alas
        #   - Own bundle's queue
        return self._block_ref_bundler.num_blocks() + self._bundle_queue.num_blocks()

    def internal_input_queue_num_bytes(self) -> int:
        return (
            self._bundle_queue.estimate_size_bytes()
            + self._block_ref_bundler.size_bytes()
        )

    def start(self, options: ExecutionOptions):
        self._actor_locality_enabled = options.actor_locality_enabled
        super().start(options)

        self._actor_cls = ray.remote(**self._ray_remote_args)(self._map_worker_cls)
        # Trigger the large UDF warning check by accessing the property.
        # This is needed because ActorPoolMapOperator passes _map_transformer
        # directly to actors, never accessing the lazy _map_transformer_ref property.
        _ = self._map_transformer_ref
        self._actor_pool.scale(
            ActorPoolScalingRequest(
                delta=self._actor_pool.initial_size(), reason="scaling to initial size"
            )
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
        return self._actor_pool.num_free_task_slots() > 0

    def _start_actor(
        self, labels: Dict[str, str], logical_actor_id: str
    ) -> Tuple[ActorHandle, ObjectRef]:
        """Start a new actor and add it to the actor pool as a pending actor.

        Args:
            labels: The key-value labels to launch the actor with.
            logical_actor_id: The logical id of the actor.

        Returns:
            A tuple of the actor handle and the object ref to the actor's location.
        """
        assert self._actor_cls is not None
        ctx = self.data_context
        if self._ray_remote_args_fn:
            self._refresh_actor_cls()
        actor = self._actor_cls.options(
            _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **labels}
        ).remote(
            ctx=ctx,
            logical_actor_id=logical_actor_id,
            src_fn_name=self.name,
            map_transformer=self._map_transformer,
            actor_location_tracker=get_or_create_actor_location_tracker(),
        )
        res_ref = actor.get_location.remote()

        def _task_done_callback(res_ref):
            # res_ref is a future for a now-ready actor; move actor from pending to the
            # active actor pool.
            has_actor = self._actor_pool.pending_to_running(res_ref)
            if not has_actor:
                # Actor has already been killed.
                return
            # Try to dispatch queued tasks.
            self._dispatch_tasks()

        self._submit_metadata_task(
            res_ref,
            lambda: _task_done_callback(res_ref),
        )
        return actor, res_ref

    def _add_bundled_input(self, bundle: RefBundle):
        # Notify first input for deferred initialization (e.g., Iceberg schema evolution).
        self._notify_first_input(bundle)
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
        for bundle, actor in self._actor_task_selector.select_actors(
            self._bundle_queue, self._actor_locality_enabled
        ):
            # Submit the map task.
            self._metrics.on_input_dequeued(bundle)
            input_blocks = [block for block, _ in bundle.blocks]
            self._actor_pool.on_task_submitted(actor)

            ctx = TaskContext(
                task_idx=self._next_data_task_idx,
                op_name=self.name,
                target_max_block_size_override=self.target_max_block_size_override,
            )
            gen = actor.submit.options(
                num_returns="streaming",
                **self._ray_actor_task_remote_args,
            ).remote(
                self.data_context,
                ctx,
                *input_blocks,
                slices=bundle.slices,
                **self.get_map_task_kwargs(),
            )

            def _task_done_callback(actor_to_return):
                # Return the actor that was running the task to the pool.
                self._actor_pool.on_task_completed(actor_to_return)
                # Dipsatch more tasks.
                self._dispatch_tasks()

            from functools import partial

            self._submit_data_task(
                gen, bundle, partial(_task_done_callback, actor_to_return=actor)
            )

            # Update locality metrics
            if (
                self._actor_pool.running_actors()[actor].actor_location
                in bundle.get_preferred_object_locations()
            ):
                self._locality_hits += 1
            else:
                self._locality_misses += 1

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
        self._actor_cls = ray.remote(**remote_args)(self._map_worker_cls)
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

    def clear_internal_input_queue(self) -> None:
        """Clear internal input queues for the actor-pool map operator.

        In addition to clearing the base class' internal queues, this method clears
        the local bundle queue used to stage input bundles for actors.
        """
        super().clear_internal_input_queue()

        while self._bundle_queue.has_next():
            bundle = self._bundle_queue.get_next()
            self._metrics.on_input_dequeued(bundle)

    def _do_shutdown(self, force: bool = False):
        self._actor_pool.shutdown(force=force)
        # NOTE: It's critical for Actor Pool to release actors before calling into
        #       the base method that will attempt to cancel and join pending.
        super()._do_shutdown(force)

    def progress_str(self) -> str:
        if self._actor_locality_enabled:
            return locality_string(
                self._locality_hits,
                self._locality_misses,
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
            object_store_memory=(
                self._metrics.obj_store_mem_max_pending_output_per_task or 0
            )
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
            res["locality_hits"] = self._locality_hits
            res["locality_misses"] = self._locality_misses
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

        # Allow actor tasks to execute out of order by default. This prevents actors
        # from idling when the first actor task is blocked.
        #
        # `MapOperator` should still respect `preserve_order` in this case.
        if "allow_out_of_order_execution" not in ray_remote_args:
            ray_remote_args["allow_out_of_order_execution"] = True

        return ray_remote_args

    def get_autoscaling_actor_pools(self) -> List[AutoscalingActorPool]:
        return [self._actor_pool]

    def per_task_resource_allocation(
        self: "PhysicalOperator",
    ) -> ExecutionResources:
        # For Actor tasks resource allocation is determined as:
        #   - Per actor resource allocation divided by
        #   - Actor's max task concurrency
        max_concurrency = self._actor_pool.max_actor_concurrency()
        per_actor_resource_usage = self._actor_pool.per_actor_resource_usage()
        return per_actor_resource_usage.scale(1 / max_concurrency)

    def min_scheduling_resources(
        self: "PhysicalOperator",
    ) -> ExecutionResources:
        return self._actor_pool.per_actor_resource_usage()

    def update_resource_usage(self) -> None:
        """Updates resources usage."""
        for actor in self._actor_pool.get_running_actor_refs():
            actor_state = actor._get_local_state()
            if actor_state in (None, gcs_pb2.ActorTableData.ActorState.DEAD):
                # actor._get_local_state can return None if the state is Unknown
                # If actor_state is None or dead, there is nothing to do.
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

    def get_max_concurrency_limit(self) -> Optional[int]:
        return self._actor_pool.max_size() * self._actor_pool.max_actor_concurrency()


class _MapWorker:
    """An actor worker for MapOperator."""

    def __init__(
        self,
        ctx: DataContext,
        src_fn_name: str,
        map_transformer: MapTransformer,
        logical_actor_id: str,
        actor_location_tracker: ray.actor.ActorHandle[ActorLocationTracker],
    ):
        self.src_fn_name: str = src_fn_name
        self._map_transformer = map_transformer
        # Initialize the data context for this actor after setting the src_fn_name in order to not
        # break __repr__. It's possible that logging setup fails.
        DataContext._set_current(ctx)
        # Initialize state for this actor with retry logic for UDF init failures.
        self._init_udf_with_retries(ctx)
        self._logical_actor_id = logical_actor_id
        actor_location_tracker.update_actor_location.remote(
            self._logical_actor_id, ray.get_runtime_context().get_node_id()
        )

    def _init_udf_with_retries(self, ctx: DataContext) -> None:
        """Initialize the UDF with retry logic for transient failures."""
        max_retries = (
            ctx.actor_init_max_retries if ctx.actor_init_retry_on_errors else 0
        )
        # -1 means infinite retries
        last_exception = None
        attempt = 0
        while max_retries < 0 or attempt <= max_retries:
            try:
                self._map_transformer.init()
                return  # Success
            except Exception as e:
                last_exception = e
                logger.debug(
                    f"Failed to initialize UDF on attempt {attempt + 1} "
                    f"(max_retries={'infinite' if max_retries < 0 else max_retries}): {e}",
                    exc_info=True,
                )
                attempt += 1
        # All retries exhausted
        raise last_exception

    def get_location(self) -> NodeIdStr:
        return ray.get_runtime_context().get_node_id()

    def submit(
        self,
        data_context: DataContext,
        ctx: TaskContext,
        *blocks: Block,
        slices: Optional[List[BlockSlice]] = None,
        **kwargs: Dict[str, Any],
    ) -> Iterator[Union[Block, List[BlockMetadata]]]:
        yield from _map_task(
            self._map_transformer,
            data_context,
            ctx,
            *blocks,
            slices=slices,
            **kwargs,
        )

    def __repr__(self):
        # Use getattr to handle case where __init__ failed before src_fn_name was set.
        # This can happen during actor restarts or initialization failures.
        return f"MapWorker({getattr(self, 'src_fn_name', '<initializing>')})"

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


class _ActorTaskSelector(abc.ABC):
    def __init__(self, actor_pool: "_ActorPool"):
        """Initialize the actor task selector.

        Args:
            actor_pool: The actor pool to select tasks from.
        """
        self._actor_pool = actor_pool

    @abstractmethod
    def select_actors(
        self, input_queue: BundleQueue, actor_locality_enabled: bool
    ) -> Iterator[Tuple[RefBundle, ActorHandle]]:
        """Select actors for bundles in the input queue.

        Args:
            input_queue: The input queue to select actors for.
            actor_locality_enabled: Whether actor locality is enabled.

        Returns:
            Iterator of tuples of the bundle and the selected actor for that bundle.
            Iteration stops when there are no more bundles to be selected in the input queue
        """
        pass


class _ActorTaskSelectorImpl(_ActorTaskSelector):
    def __init__(self, actor_pool: "_ActorPool"):
        super().__init__(actor_pool)

    def select_actors(
        self, input_queue: BundleQueue, actor_locality_enabled: bool
    ) -> Iterator[Tuple[RefBundle, ActorHandle]]:
        """Picks actors for task submission based on busyness and locality."""
        if not self._actor_pool.running_actors():
            # Actor pool is empty or all actors are still pending.
            return

        while input_queue:
            # Filter out actors that are invalid, i.e. actors with number of tasks in
            # flight >= _max_tasks_in_flight or actor_state is not ALIVE.
            bundle = input_queue.peek_next()
            valid_actors = [
                actor
                for actor in self._actor_pool.running_actors()
                if self._actor_pool.running_actors()[actor].num_tasks_in_flight
                < self._actor_pool.max_tasks_in_flight_per_actor()
                and not self._actor_pool.running_actors()[actor].is_restarting
            ]

            if not valid_actors:
                # All actors are at capacity or actor state is not ALIVE.
                return

            # Rank all valid actors
            ranks = self._rank_actors(
                valid_actors, bundle if actor_locality_enabled else None
            )

            assert len(ranks) == len(
                valid_actors
            ), f"{len(ranks)} != {len(valid_actors)}"

            # Pick the actor with the highest rank (lower value, higher rank)
            target_actor_idx = min(range(len(valid_actors)), key=lambda idx: ranks[idx])

            target_actor = valid_actors[target_actor_idx]

            # We remove the bundle and yield the actor to the operator. We do not use pop()
            # in case the queue has changed the order of the bundles.
            input_queue.remove(bundle)
            yield bundle, target_actor

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

        # NOTE: Ranks are ordered in descending order (ie rank[0] is the highest
        #       and rank[-1] is the lowest)
        ranks = [
            (
                # Priority/rank of the location (based on the object size).
                # Defaults to int32 max value (ie no rank)
                locs_priorities.get(
                    self._actor_pool.running_actors()[actor].actor_location, INT32_MAX
                ),
                # Number of tasks currently in flight at the given actor
                self._actor_pool.running_actors()[actor].num_tasks_in_flight,
            )
            for actor in actors
        ]

        return ranks


class _ActorPool(AutoscalingActorPool):
    """A pool of actors for map task execution.

    This class is in charge of tracking the number of in-flight tasks per actor,
    providing the least heavily loaded actor to the operator, and killing idle
    actors when the operator is done submitting work to the pool.
    """

    _ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S = 10
    _ACTOR_POOL_GRACEFUL_SHUTDOWN_TIMEOUT_S = 30
    _LOGICAL_ACTOR_ID_LABEL_KEY = "__ray_data_logical_actor_id"

    def __init__(
        self,
        create_actor_fn: "Callable[[Dict[str, str]], Tuple[ActorHandle, ObjectRef[Any]]]",
        per_actor_resource_usage: ExecutionResources,
        *,
        min_size: int,
        max_size: int,
        initial_size: int,
        max_actor_concurrency: int,
        max_tasks_in_flight_per_actor: int,
        _enable_actor_pool_on_exit_hook: bool = False,
    ):
        """Initialize the actor pool.

        Args:
            create_actor_fn: This function should take key-value labels as input, and
                create an actor with those labels. The function should return the actor
                handle and a reference to the actor's node ID.
            per_actor_resource_usage: The resource usage per actor.
            min_size: The minimum number of running actors to be maintained
                in the pool. Note, that this constraint could be violated when
                no new work is available for scheduling in the actor pool (ie
                when operator completes execution).
            max_size: The maximum number of running actors to be maintained
                in the pool.
            initial_size: The initial number of actors to start with.
            max_actor_concurrency: The maximum number of concurrent tasks a
                single actor can execute (derived from `ray_remote_args`
                passed to the operator).
            max_tasks_in_flight_per_actor: The maximum number of tasks that can
                be submitted to a single actor at any given time.
            _enable_actor_pool_on_exit_hook: Whether to enable the actor pool
                on exit hook.
        """

        self._min_size: int = min_size
        self._max_size: int = max_size
        self._initial_size: int = initial_size
        self._max_actor_concurrency: int = max_actor_concurrency
        self._max_tasks_in_flight: int = max_tasks_in_flight_per_actor
        self._create_actor_fn = create_actor_fn
        self._per_actor_resource_usage = per_actor_resource_usage

        assert self._min_size >= 1
        assert self._max_size >= self._min_size
        assert self._initial_size <= self._max_size
        assert self._initial_size >= self._min_size
        assert self._max_tasks_in_flight >= 1
        assert self._create_actor_fn is not None

        # Timestamp of the last scale up action
        self._last_upscaled_at: Optional[float] = None
        self._last_downscaling_debounce_warning_ts: Optional[float] = None
        # Actors that have started running, including alive and restarting actors.
        self._running_actors: Dict[ray.actor.ActorHandle, _ActorState] = {}
        # Actors that are not yet ready (still pending creation).
        self._pending_actors: Dict[ObjectRef, ray.actor.ActorHandle] = {}
        # Map from actor handle to its logical ID.
        self._actor_to_logical_id: Dict[ray.actor.ActorHandle, str] = {}
        self._enable_actor_pool_on_exit_hook = _enable_actor_pool_on_exit_hook
        # Cached values for actor / task counts
        self._num_restarting_actors: int = 0
        self._num_active_actors: int = 0
        self._total_num_tasks_in_flight: int = 0

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
        return self._num_restarting_actors

    def num_active_actors(self) -> int:
        """Active actors are all the running actors with inflight tasks."""
        return self._num_active_actors

    def num_alive_actors(self) -> int:
        """Alive actors are all the running actors in ALIVE state."""
        return len(self._running_actors) - self._num_restarting_actors

    def num_pending_actors(self) -> int:
        return len(self._pending_actors)

    def max_tasks_in_flight_per_actor(self) -> int:
        return self._max_tasks_in_flight

    def max_actor_concurrency(self) -> int:
        return self._max_actor_concurrency

    def num_tasks_in_flight(self) -> int:
        return self._total_num_tasks_in_flight

    def initial_size(self) -> int:
        return self._initial_size

    def _can_apply(self, config: ActorPoolScalingRequest) -> bool:
        """Returns whether Actor Pool is able to execute scaling request"""

        if config.delta < 0:
            # To prevent bouncing back and forth, we disallow scale down for
            # a "cool-off" period after the most recent scaling up, with an intention
            # to allow application to actually utilize newly provisioned resources
            # before making decisions on subsequent actions.
            #
            # Note that this action is unidirectional and doesn't apply to
            # scaling up, ie if actor pool just scaled down, it'd still be able
            # to scale back up immediately.
            if (
                not config.force
                and self._last_upscaled_at is not None
                and (
                    time.time()
                    <= self._last_upscaled_at
                    + self._ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S
                )
            ):
                # NOTE: To avoid spamming logs unnecessarily, debounce log is produced once
                #       per upscaling event
                if self._last_upscaled_at != self._last_downscaling_debounce_warning_ts:
                    logger.debug(
                        f"Ignoring scaling down request (request={config}; reason=debounced from scaling up at {self._last_upscaled_at})"
                    )
                    self._last_downscaling_debounce_warning_ts = self._last_upscaled_at

                return False

        return True

    def scale(self, req: ActorPoolScalingRequest) -> Optional[int]:
        # Verify request could be applied
        if not self._can_apply(req):
            return 0

        if req.delta > 0:
            target_num_actors = req.delta

            logger.debug(
                f"Scaling up actor pool by {target_num_actors} (reason={req.reason}, "
                f"{self.get_actor_info()})"
            )

            for _ in range(target_num_actors):
                actor, ready_ref = self._create_actor()
                self.add_pending_actor(actor, ready_ref)

            # Capture last scale up timestamp
            self._last_upscaled_at = time.time()

            return target_num_actors

        elif req.delta < 0:
            num_released = 0
            target_num_actors = abs(req.delta)

            for _ in range(target_num_actors):
                if self._remove_inactive_actor():
                    num_released += 1

            if num_released > 0:
                logger.debug(
                    f"Scaled down actor pool by {num_released} "
                    f"(reason={req.reason}; {self.get_actor_info()})"
                )

            return -num_released

        return None

    def _create_actor(self) -> Tuple[ray.actor.ActorHandle, ObjectRef]:
        logical_actor_id = str(uuid.uuid4())
        labels = {self.get_logical_id_label_key(): logical_actor_id}
        actor, ready_ref = self._create_actor_fn(labels, logical_actor_id)
        self._actor_to_logical_id[actor] = logical_actor_id
        return actor, ready_ref

    # === End of overriding methods of AutoscalingActorPool ===

    def running_actors(self) -> Dict[ray.actor.ActorHandle, _ActorState]:
        return self._running_actors

    def on_task_submitted(self, actor: ray.actor.ActorHandle):
        self._running_actors[actor].num_tasks_in_flight += 1
        self._total_num_tasks_in_flight += 1

        if self._running_actors[actor].num_tasks_in_flight == 1:
            self._num_active_actors += 1

    def update_running_actor_state(
        self, actor: ray.actor.ActorHandle, is_restarting: bool
    ) -> None:
        """Update running actor state.

        Args:
            actor: The running actor that needs state update.
            is_restarting: Whether running actor is restarting or alive.
        """
        assert actor in self._running_actors
        if self._running_actors[actor].is_restarting == is_restarting:
            return

        self._running_actors[actor].is_restarting = is_restarting
        if is_restarting:
            self._num_restarting_actors += 1
        else:
            self._num_restarting_actors -= 1

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

        Raises:
            RayError: If the actor initialization failed. The actor is cleaned up
                from internal tracking before re-raising.
        """
        if ready_ref not in self._pending_actors:
            # The actor has been removed from the pool before becoming running.
            return False
        actor = self._pending_actors.pop(ready_ref)
        try:
            actor_location = ray.get(ready_ref)
        except Exception:
            # Actor init failed - clean up the actor from _actor_to_logical_id
            # This must happen for all exceptions, not just RayError, to prevent
            # memory leaks where dead actor handles remain in _actor_to_logical_id.
            self._actor_to_logical_id.pop(actor, None)
            raise
        self._running_actors[actor] = _ActorState(
            num_tasks_in_flight=0,
            actor_location=actor_location,
            is_restarting=False,
        )
        return True

    def on_task_completed(self, actor: ray.actor.ActorHandle):
        """Called when a task completes. Returns the provided actor to the pool."""
        assert actor in self._running_actors
        assert self._running_actors[actor].num_tasks_in_flight > 0
        self._running_actors[actor].num_tasks_in_flight -= 1
        self._total_num_tasks_in_flight -= 1
        if not self._running_actors[actor].num_tasks_in_flight:
            self._num_active_actors -= 1

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
        return len(self._running_actors) - self._num_active_actors

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

        # Update cached statistics before removing the actor
        actor_state = self._running_actors[actor]

        # Update total tasks in flight
        self._total_num_tasks_in_flight -= actor_state.num_tasks_in_flight

        # Update active actors count
        if actor_state.num_tasks_in_flight > 0:
            self._num_active_actors -= 1

        # Update restarting actors count
        if actor_state.is_restarting:
            self._num_restarting_actors -= 1

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
