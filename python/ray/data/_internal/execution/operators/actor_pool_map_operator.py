from __future__ import annotations

import logging
import time
import uuid
import warnings
from collections import defaultdict
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    DefaultDict,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from typing_extensions import override

if TYPE_CHECKING:
    import pyarrow as pa
import ray
from ray.actor import ActorHandle
from ray.core.generated import gcs_pb2
from ray.data._internal.actor_autoscaler import (
    AutoscalingActorPool,
)
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    ActorPoolInfo,
    ActorPoolScalingRequest,
    AutoscalingActorConfig,
)
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.bundle_queue import (
    RebundleQueue,
    create_bundle_queue,
)
from ray.data._internal.execution.interfaces import (
    BlockSlice,
    ExecutionOptions,
    ExecutionResources,
    NodeIdStr,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.node_trackers.actor_location import (
    ActorLocationTracker,
    get_or_create_actor_location_tracker,
)
from ray.data._internal.execution.operators.map_operator import (
    MapOperator,
    _map_task,
)
from ray.data._internal.execution.operators.map_transformer import MapTransformer
from ray.data._internal.execution.util import locality_string
from ray.data._internal.remote_fn import _add_system_error_to_retry_exceptions
from ray.data._internal.utils.heapdict import heapdict
from ray.data.block import Block, BlockMetadata
from ray.data.context import (
    DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR,
    DataContext,
)
from ray.types import ObjectRef

logger = logging.getLogger(__name__)

_ACTOR_STATE_DEAD = gcs_pb2.ActorTableData.ActorState.DEAD
_ACTOR_STATE_ALIVE = gcs_pb2.ActorTableData.ActorState.ALIVE
_ACTOR_STATE_RESTARTING = gcs_pb2.ActorTableData.ActorState.RESTARTING

# Type alias for the logical identifier of an actor (used in labels and actor-to-id maps).
LogicalActorId = str


def get_map_worker_cls_name(op_name: str) -> str:
    """Return the dynamic class name used for actor pool map workers."""
    return f"MapWorker({op_name})"


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
        ref_bundler: Optional[RebundleQueue] = None,
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
        map_worker_cls_name = get_map_worker_cls_name(self.name)
        # We set the actor class name to include operator name to disambiguate
        # logs in the Actor Pool
        self._map_worker_cls_name = map_worker_cls_name
        # HACK: Without this, all actors show up as `_MapWorker` in Grafana, so we can’t
        # tell which operator they belong to. To fix that, we dynamically create a new
        # class per operator with a unique name.
        self._map_worker_cls = type(map_worker_cls_name, (_MapWorker,), {})

        self._actor_pool = self._create_actor_pool(compute_strategy)
        # A queue of bundles awaiting dispatch to actors.
        self._bundle_queue = create_bundle_queue()
        # Cached actor class.
        self._actor_cls = None
        self._actor_locality_enabled: Optional[bool] = None

        # Locality metrics
        self._locality_hits = 0
        self._locality_misses = 0

    def _create_actor_pool(
        self, compute_strategy: ActorPoolStrategy
    ) -> "AutoscalingActorPool":
        config = self._create_actor_pool_config(compute_strategy)
        return _ActorPool(
            create_actor_fn=self._start_actor,
            config=config,
            map_worker_cls_name=self._map_worker_cls_name,
        )

    def _create_actor_pool_config(
        self, compute_strategy: ActorPoolStrategy
    ) -> "AutoscalingActorConfig":
        per_actor_resource_usage = ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus"),
            gpu=self._ray_remote_args.get("num_gpus"),
            memory=self._ray_remote_args.get("memory"),
        )
        max_actor_concurrency = self._ray_remote_args.get("max_concurrency", 1)
        config = AutoscalingActorConfig(
            min_size=compute_strategy.min_size,
            max_size=compute_strategy.max_size,
            initial_size=compute_strategy.initial_size,
            max_tasks_in_flight_per_actor=compute_strategy.max_tasks_in_flight_per_actor
            or self.data_context.max_tasks_in_flight_per_actor
            or max_actor_concurrency
            * DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR,
            max_actor_concurrency=max_actor_concurrency,
            per_actor_resource_usage=per_actor_resource_usage,
        )
        return config

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
            + self._block_ref_bundler.estimate_size_bytes()
        )

    def start(self, options: ExecutionOptions):
        self._actor_locality_enabled = options.actor_locality_enabled
        super().start(options)

        self._actor_cls = ray.remote(**self._ray_remote_args)(self._map_worker_cls)
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

    def can_add_input(self) -> bool:
        """NOTE: PLEASE READ CAREFULLY

        This method has to abide by the following contract to guarantee Operator's
        ability to handle all provided inputs (liveness):

            - This method should only return `True` when operator is guaranteed
            to be able to launch a task, meaning that subsequent `op.add_input(...)`
            should be able to launch a task.

        """
        return self._actor_pool.can_schedule_task()

    def _start_actor(
        self, labels: Dict[str, str], logical_actor_id: LogicalActorId
    ) -> Tuple[ActorHandle, ObjectRef, ExecutionResources]:
        """Start a new actor and add it to the actor pool as a pending actor.

        Args:
            labels: The key-value labels to launch the actor with.
            logical_actor_id: The logical id of the actor.

        Returns:
            A tuple of the actor handle, the object ref to the actor's location,
            and the actual resource usage for this actor.
        """
        assert self._actor_cls is not None
        actual_remote_args = dict(self._merge_ray_remote_args())
        extra_labels = actual_remote_args.pop("_labels", {})
        actor_resource_usage = ExecutionResources(
            cpu=actual_remote_args.get("num_cpus", 0),
            gpu=actual_remote_args.get("num_gpus", 0),
            memory=actual_remote_args.get("memory", 0),
        )
        actor = self._actor_cls.options(
            _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **labels, **extra_labels},
            **actual_remote_args,
        ).remote(
            ctx=self._data_context_ref,
            logical_actor_id=logical_actor_id,
            src_fn_name=self.name,
            map_transformer=self._map_transformer_ref,
            actor_location_tracker=get_or_create_actor_location_tracker(),
        )
        res_ref = actor.get_location.options(
            _labels={self._OPERATOR_ID_LABEL_KEY: self.id}
        ).remote()

        def _task_done_callback(res_ref):
            # res_ref is a future for a now-ready actor; move actor from pending to the
            # active actor pool.
            has_actor = self._actor_pool.pending_to_running(res_ref) is not None
            if not has_actor:
                # Actor has already been killed.
                return

        self._submit_metadata_task(
            res_ref,
            lambda: _task_done_callback(res_ref),
        )
        return actor, res_ref, actor_resource_usage

    def _try_schedule_task(self, bundle: RefBundle, strict: bool):
        # Notify first input for deferred initialization (e.g., Iceberg schema evolution).
        self._notify_first_input(bundle)
        # Enqueue input bundle
        self._bundle_queue.add(bundle)
        self._metrics.on_input_queued(bundle, input_index=0)

        if strict:
            # NOTE: In case of strict input handling protocol at least 1 task
            #       must be launched. Therefore, we assert here that it was
            #       verified that the task could be scheduled before invoking
            #       this method
            assert self.can_add_input(), f"Operator {self} can not handle input!"

        # Try to dispatch new tasks
        submitted = self._try_schedule_tasks_internal()

        if strict:
            assert (
                submitted >= 1
            ), f"Expected at least 1 task launched (launched {submitted})"

    def _try_schedule_tasks_internal(self) -> int:
        """Try to dispatch tasks from the internal queue. Returns the # of tasks submitted"""

        num_submitted_tasks = 0
        while self._bundle_queue.has_next():

            bundle = self._bundle_queue.peek_next()
            actor = self._actor_pool.select_actors(
                bundle=bundle,
                actor_locality_enabled=self._actor_locality_enabled,
            )
            if actor is None:
                break

            self._bundle_queue.remove(bundle)

            self._metrics.on_input_dequeued(bundle, input_index=0)
            input_blocks = [block for block, _ in bundle.blocks]
            self._actor_pool.on_task_submitted(actor)

            ctx = TaskContext(
                task_idx=self._next_data_task_idx,
                op_name=self.name,
                target_max_block_size_override=self.target_max_block_size_override,
            )
            actor_task_args = dict(self._ray_actor_task_remote_args)
            extra_labels = actor_task_args.pop("_labels", None) or {}
            gen = actor.submit.options(
                num_returns="streaming",
                _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **extra_labels},
                **actor_task_args,
            ).remote(
                self._data_context_ref,
                ctx,
                *input_blocks,
                slices=bundle.slices,
                **self.get_map_task_kwargs(),
            )

            def _task_done_callback(actor_to_return):
                # Return the actor that was running the task to the pool.
                self._actor_pool.on_task_completed(actor_to_return)

            from functools import partial

            self._submit_data_task(
                gen, bundle, partial(_task_done_callback, actor_to_return=actor)
            )

            num_submitted_tasks += 1

            # Update locality metrics
            if (
                self._actor_pool.get_actor_location(actor)
                in bundle.get_preferred_object_locations()
            ):
                self._locality_hits += 1
            else:
                self._locality_misses += 1

        return num_submitted_tasks

    def _merge_ray_remote_args(self) -> Dict[str, Any]:
        """When `self._ray_remote_args_fn` is specified, this method should
        be called prior to initializing the new worker in order to get new
        remote args passed to the worker.

        Returns:
            The merged remote args used to create the actor class.
        """
        remote_args = self._ray_remote_args.copy()
        if self._ray_remote_args_fn:
            remote_args.update(self._ray_remote_args_fn())
        return remote_args

    def has_next(self) -> bool:
        # In case there are still enqueued bundles remaining, try to
        # dispatch tasks.
        if self._inputs_complete and self._bundle_queue.num_blocks() > 0:
            # NOTE: That no more than 1 bundle is expected to be in the queue
            #       upon inputs completion (the one that was pending in the bundler)
            if self._bundle_queue.num_bundles() > 1:
                logger.warning(
                    f"Expected 1 bundle to remain in the input queue of {self} "
                    f"(found {self._bundle_queue.num_bundles()} bundles, with {self._bundle_queue.num_blocks()} blocks)"
                )

            # Schedule tasks handling remaining bundles
            self._try_schedule_tasks_internal()

        return super().has_next()

    def all_inputs_done(self):
        # Call base implementation to handle any leftover bundles. This may or may not
        # trigger task dispatch.
        super().all_inputs_done()

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
            self._metrics.on_input_dequeued(bundle, input_index=0)

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
        max_actors = self._actor_pool.max_size()
        assert min_actors is not None, min_actors

        num_cpus_per_actor = self._ray_remote_args.get("num_cpus", 0)
        num_gpus_per_actor = self._ray_remote_args.get("num_gpus", 0)
        memory_per_actor = self._ray_remote_args.get("memory", 0)

        min_resource_usage = ExecutionResources(
            cpu=num_cpus_per_actor * min_actors,
            gpu=num_gpus_per_actor * min_actors,
            memory=memory_per_actor * min_actors,
        )

        # Cap resources to 0 if this operator doesn't use them.
        # This prevents operators from hoarding resource budget they don't need.
        max_resource_usage = ExecutionResources(
            cpu=0 if num_cpus_per_actor == 0 else num_cpus_per_actor * max_actors,
            gpu=0 if num_gpus_per_actor == 0 else num_gpus_per_actor * max_actors,
            memory=0 if memory_per_actor == 0 else memory_per_actor * max_actors,
            # Set the max `object_store_memory` requirement to 'inf', because we
            # don't know how much data each task can output.
            object_store_memory=float("inf"),
        )

        return min_resource_usage, max_resource_usage

    def current_logical_usage(self) -> ExecutionResources:
        # Both pending and running actors count towards our current resource usage.
        return self._actor_pool.current_logical_usage()

    def pending_logical_usage(self) -> ExecutionResources:
        # Both pending and restarting actors count towards pending processor usage.
        return self._actor_pool.pending_logical_usage()

    def incremental_resource_usage(self) -> ExecutionResources:
        # Submitting tasks to existing actors doesn't require additional
        # CPU/GPU resources.
        return ExecutionResources(
            cpu=0,
            gpu=0,
            object_store_memory=0,
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

    def per_task_resource_allocation(self) -> ExecutionResources:
        # For Actor tasks resource allocation is determined as:
        #   - Per actor resource allocation divided by
        #   - Actor's max task concurrency
        max_concurrency = self._actor_pool.max_actor_concurrency()
        per_actor_resource_usage = self._actor_pool.per_actor_resource_usage()
        return per_actor_resource_usage.scale(1 / max_concurrency)

    def min_scheduling_resources(self) -> ExecutionResources:
        return self._actor_pool.per_actor_resource_usage()

    def refresh_state(self):
        """Updates internal state"""

        # Trigger Actor Pool's state refresh
        self._actor_pool.refresh_actor_state()

    def get_actor_info(self) -> ActorPoolInfo:
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
        logical_actor_id: LogicalActorId,
        actor_location_tracker: ActorHandle[ActorLocationTracker],
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

    def __ray_shutdown__(self):
        """Called by Ray Core when the actor exits gracefully.

        Triggered when all Python actor handles go out of scope and the handle
        is collected by reference counting.

        During graceful shutdown, ActorPoolMapOperator clears _data_tasks and
        drops pool references so handles become collectible immediately.
        Ray Core guarantees this is called after all pending tasks complete
        and before the actor process exits.

        Note: this is NOT called if the actor is forcefully killed (e.g. via
        `ray.kill(actor)`) or crashes unexpectedly.
        """
        # `_map_actor_context` is a global variable that references the UDF object.
        # Delete it to trigger `UDF.__del__`.
        del ray.data._map_actor_context
        ray.data._map_actor_context = None


@dataclass
class _ActorState:
    """Actor state. Not to be confused with Ray Core actor state that tracks
    DEAD, RESTARTING, or ALIVE statuses, but rather, tracks additional info
    in order to inform Ray Data scheduling decisions."""

    # Number of tasks in flight per actor
    num_tasks_in_flight: int

    # Node id of each ready actor
    actor_location: str

    # Is Actor state restarting or alive
    is_restarting: bool


# num tasks in flight (fewer = higher ranked = preferred)
_ActorRank = int


class _ActorPool(AutoscalingActorPool):
    """A pool of actors for map task execution.

    This class is in charge of tracking the number of in-flight tasks per actor,
    providing the least heavily loaded actor to the operator, and killing idle
    actors when the operator is done submitting work to the pool.
    """

    _ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S = 10

    def __init__(
        self,
        create_actor_fn: "Callable[[Dict[str, str]], Tuple[ActorHandle, ObjectRef[Any], ExecutionResources]]",
        config: AutoscalingActorConfig,
        map_worker_cls_name: str = "MapWorker",
        debounce_period_s: int = _ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S,
    ):
        """Initialize the actor pool.

        Args:
            create_actor_fn: Callable that takes key-value labels as input and
                creates an actor with those labels. Returns the actor handle, a
                reference to the actor's node ID, and the actor's resource usage.
            config: Configuration for the autoscaling actor pool, including
                min/max/initial pool sizes, concurrency, and resource usage.
            map_worker_cls_name: Name of the map worker class for logging
                purposes.
            debounce_period_s: Debounce period for scaling down after scaling
                up.
        """
        super().__init__(config=config)

        self._create_actor_fn = create_actor_fn
        self._map_worker_cls_name = map_worker_cls_name
        self._debounce_period_s = debounce_period_s
        # Timestamp of the last scale up action
        self._last_upscaled_at: Optional[float] = None
        self._last_downscaling_debounce_warning_ts: Optional[float] = None
        # Actors that have started running, including alive and restarting actors.
        self._running_actors: Dict[ActorHandle, _ActorState] = {}
        # Actors that are not yet ready (still pending creation).
        self._pending_actors: Dict[ObjectRef, ActorHandle] = {}
        # Map from actor handle to its logical ID.
        self._actor_to_logical_id: Dict[ActorHandle, LogicalActorId] = {}
        # Per-actor resource usage, needed because ray_remote_args_fn can
        # produce different resources for each actor.
        self._actor_resource_usage: Dict[ActorHandle, ExecutionResources] = {}
        # Cached aggregate resource counters.
        self._total_usage = ExecutionResources.zero()
        self._pending_or_restarting_usage = ExecutionResources.zero()
        # Cached values for actor / task counts
        self._num_restarting_actors: int = 0
        self._num_active_actors: int = 0
        self._total_num_tasks_in_flight: int = 0

        # ALIVE Actor on ACTIVE node -> _ActorRank(num_tasks_in_flight)
        # heap gives highest rank (lowest comparable value) first.
        # This means we prefer actors with FEWER num tasks inflight.
        # NOTE: This is needed as a fallback when locality is disabled,
        # or when no actors are scheduled on a node with the current bundle.
        self._alive_actors_to_in_flight_tasks_heap: heapdict[
            ActorHandle, _ActorRank
        ] = heapdict()

        # ACTIVE node -> per-node heap of ALIVE actors ranked by num_tasks_in_flight.
        # Peek gives the least-busy actor on each node in O(1).
        self._alive_node_to_actor_heap: DefaultDict[
            NodeIdStr, heapdict[ActorHandle, _ActorRank]
        ] = defaultdict(heapdict)

    @property
    def map_worker_cls_name(self) -> str:
        return self._map_worker_cls_name

    # === Overriding methods of AutoscalingActorPool ===

    @override
    def num_running_actors(self) -> int:
        return len(self._running_actors)

    @override
    def num_restarting_actors(self) -> int:
        """Restarting actors are all the running actors not in ALIVE state."""
        return self._num_restarting_actors

    @override
    def num_active_actors(self) -> int:
        """Active actors are all the running actors with inflight tasks."""
        return self._num_active_actors

    @override
    def num_pending_actors(self) -> int:
        return len(self._pending_actors)

    @override
    def num_tasks_in_flight(self) -> int:
        return self._total_num_tasks_in_flight

    @override
    def scale(self, req: ActorPoolScalingRequest) -> Optional[int]:
        # Verify request could be applied
        if not self._can_apply_request(req):
            return 0

        map_worker_cls_name = self.map_worker_cls_name

        if req.delta > 0:
            target_num_actors = req.delta

            logger.debug(
                f"Scaling up {map_worker_cls_name} actor pool by {target_num_actors} (reason={req.reason}, "
                f"{self.get_actor_info()})"
            )

            for _ in range(target_num_actors):
                actor, ready_ref, resource_usage = self._create_actor()
                self._add_pending_actor(actor, ready_ref, resource_usage)

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
                    f"Scaled down {map_worker_cls_name} actor pool by {num_released} "
                    f"(reason={req.reason}; {self.get_actor_info()})"
                )

            return -num_released

        return None

    @override
    def refresh_actor_state(self):
        self._alive_node_to_actor_heap.clear()
        for actor in self._running_actors:
            self._update_running_actor_state(actor)

    @override
    def on_task_submitted(self, actor: ActorHandle):
        state = self._running_actors[actor]
        state.num_tasks_in_flight += 1
        self._total_num_tasks_in_flight += 1

        if state.num_tasks_in_flight == 1:
            self._num_active_actors += 1

        assert actor in self._alive_actors_to_in_flight_tasks_heap
        rank = _ActorRank(state.num_tasks_in_flight)
        self._alive_actors_to_in_flight_tasks_heap[actor] = rank
        node_heap = self._alive_node_to_actor_heap.get(state.actor_location)
        if node_heap is not None and actor in node_heap:
            node_heap[actor] = rank

    @override
    def get_actor_location(self, actor: ActorHandle) -> NodeIdStr:
        return self._running_actors[actor].actor_location

    @override
    def shutdown(self, force: bool = False):
        """Kills all actors, including running/active actors.

        This is called once the operator is shutting down.
        """
        self._release_pending_actors(force=force)
        self._release_running_actors(force=force)

    @override
    def pending_to_running(self, ready_ref: ray.ObjectRef) -> Optional[ActorHandle]:
        """Mark the actor corresponding to the provided ready future as running, making
        the actor pickable.

        Args:
            ready_ref: The ready future for the actor that we wish to mark as running.

        Returns:
            The actor handle of the pending/now ready actor. Otherwise, returns `None`
            if actor has already been killed

        Raises:
            RayError: If the actor initialization failed. The actor is cleaned up
                from internal tracking before re-raising.
        """
        if ready_ref not in self._pending_actors:
            # The actor has been removed from the pool before becoming running.
            return None
        actor = self._pending_actors.pop(ready_ref)
        try:
            actor_location: NodeIdStr = ray.get(ready_ref)
        except Exception:
            # Actor init failed - clean up the actor from _actor_to_logical_id
            # This must happen for all exceptions, not just RayError, to prevent
            # memory leaks where dead actor handles remain in _actor_to_logical_id.
            usage = self._actor_resource_usage.pop(actor)
            self._total_usage = self._total_usage.subtract(usage)
            self._pending_or_restarting_usage = (
                self._pending_or_restarting_usage.subtract(usage)
            )
            self._actor_to_logical_id.pop(actor, None)
            raise
        self._running_actors[actor] = _ActorState(
            num_tasks_in_flight=0,
            actor_location=actor_location,
            is_restarting=False,
        )
        # Actor is no longer pending — subtract from pending usage.
        self._pending_or_restarting_usage = self._pending_or_restarting_usage.subtract(
            self._actor_resource_usage[actor]
        )
        # NOTE: We assume any actor that goes from pending to running is ALIVE
        self._alive_actors_to_in_flight_tasks_heap[actor] = _ActorRank(0)
        self._alive_node_to_actor_heap[actor_location][actor] = _ActorRank(0)
        return actor

    @override
    def get_pending_actor_refs(self) -> List[ray.ObjectRef]:
        return list(self._pending_actors.keys())

    @override
    def select_actors(
        self,
        bundle: Optional[RefBundle] = None,
        actor_locality_enabled: bool = False,
    ) -> Optional[ActorHandle]:
        """Select an actor to process the given bundle.

        When ``bundle`` is ``None``, returns any available actor with spare
        capacity (used by ``can_schedule_task`` to probe schedulability).
        When ``bundle`` is provided, returns the best actor for that bundle
        (considering locality when ``actor_locality_enabled`` is True).

        This method peeks (does not pop) from the heap, so
        ``on_task_submitted()`` must be called for the returned actor before
        the next ``select_actors()`` call.  Otherwise the same
        actor will be selected repeatedly.  The caller in
        ``ActorPoolMapOperator._dispatch_tasks()`` upholds this contract.

        TODO: Change this to support N bundles, M actors. This will significantly
        help with actor locality, since we'll have more knowledge in scheduling.

        Args:
            bundle: The bundle to schedule. When ``None``, returns any
                available actor with spare capacity.
            actor_locality_enabled: Whether to consider data locality
                when selecting an actor.

        Returns:
            An actor handle if an actor with capacity is available, otherwise
            ``None``.
        """
        if not self._alive_actors_to_in_flight_tasks_heap:
            return None

        _, least_busy_rank = self._alive_actors_to_in_flight_tasks_heap.peekitem()
        if least_busy_rank >= self.max_tasks_in_flight_per_actor():
            return None

        target_actor: Optional[ActorHandle] = None

        if bundle is not None and actor_locality_enabled:
            target_actor = self._find_actor_with_locality(bundle)

        if target_actor is None:
            target_actor, _ = self._alive_actors_to_in_flight_tasks_heap.peekitem()

        return target_actor

    @override
    def on_task_completed(self, actor: ActorHandle):
        """Called when a task completes. Returns the provided actor to the pool."""
        state = self._running_actors[actor]
        assert state.num_tasks_in_flight > 0
        state.num_tasks_in_flight -= 1
        self._total_num_tasks_in_flight -= 1
        if not state.num_tasks_in_flight:
            self._num_active_actors -= 1

        if actor in self._alive_actors_to_in_flight_tasks_heap:
            rank = _ActorRank(state.num_tasks_in_flight)
            self._alive_actors_to_in_flight_tasks_heap[actor] = rank
            node_heap = self._alive_node_to_actor_heap.get(state.actor_location)
            if node_heap is not None and actor in node_heap:
                node_heap[actor] = rank

    # === End of overriding methods of AutoscalingActorPool ===

    def _get_actor_logical_id(self, actor: ActorHandle) -> LogicalActorId:
        return self._actor_to_logical_id[actor]

    def _can_apply_request(self, req: ActorPoolScalingRequest) -> bool:
        """Returns whether Actor Pool is able to execute scaling request"""

        if req.delta < 0:
            # To prevent bouncing back and forth, we disallow scale down for
            # a "cool-off" period after the most recent scaling up, with an intention
            # to allow application to actually utilize newly provisioned resources
            # before making decisions on subsequent actions.
            #
            # Note that this action is unidirectional and doesn't apply to
            # scaling up, ie if actor pool just scaled down, it'd still be able
            # to scale back up immediately.
            if (
                not req.force
                and self._last_upscaled_at is not None
                and (time.time() <= self._last_upscaled_at + self._debounce_period_s)
            ):
                # NOTE: To avoid spamming logs unnecessarily, debounce log is produced once
                #       per upscaling event
                if self._last_upscaled_at != self._last_downscaling_debounce_warning_ts:
                    logger.debug(
                        f"Ignoring scaling down request (request={req}; reason=debounced from scaling up at {self._last_upscaled_at})"
                    )
                    self._last_downscaling_debounce_warning_ts = self._last_upscaled_at

                return False

        return True

    def _create_actor(
        self,
    ) -> Tuple[ActorHandle, ObjectRef, ExecutionResources]:
        logical_actor_id = str(uuid.uuid4())
        labels = {self.get_logical_id_label_key(): logical_actor_id}
        actor, ready_ref, resource_usage = self._create_actor_fn(
            labels, logical_actor_id
        )
        self._actor_to_logical_id[actor] = logical_actor_id
        return actor, ready_ref, resource_usage

    def _update_running_actor_state(self, actor: ActorHandle):
        """Update running actor state. This is called for every actor
        in `refresh_actor_state`.

        Args:
            actor: The running actor that needs state update.
        """
        actor_state = actor._get_local_state()

        # 1) Check if actor is restarting
        running_actor_state = self._running_actors[actor]
        died: bool = False
        if actor_state in (None, _ACTOR_STATE_DEAD):
            # actor._get_local_state can return None if the state is Unknown
            # If actor_state is None or dead, there is nothing to do.
            died = True
        elif actor_state != _ACTOR_STATE_ALIVE:
            # The actors can be either ALIVE or RESTARTING here because they will
            # be restarted indefinitely until execution finishes.
            assert actor_state == _ACTOR_STATE_RESTARTING, actor_state
            if not running_actor_state.is_restarting:
                self._num_restarting_actors += 1
                self._pending_or_restarting_usage = (
                    self._pending_or_restarting_usage.add(
                        self._actor_resource_usage[actor]
                    )
                )
                running_actor_state.is_restarting = True
        else:
            if running_actor_state.is_restarting:
                self._num_restarting_actors -= 1
                self._pending_or_restarting_usage = (
                    self._pending_or_restarting_usage.subtract(
                        self._actor_resource_usage[actor]
                    )
                )
                running_actor_state.is_restarting = False

        self._update_rank(actor=actor, state=running_actor_state, died=died)

    def _update_rank(self, actor: ActorHandle, state: _ActorState, died: bool):
        """Update the scheduling rank for an actor after a state refresh.

        Alive actors are added/updated in both structures; restarting
        actors are removed from the heap (and omitted from the node map
        since ``refresh_actor_state`` clears it before calling this).
        """
        if not (state.is_restarting or died):
            node_id = state.actor_location
            rank = _ActorRank(state.num_tasks_in_flight)
            self._alive_node_to_actor_heap[node_id][actor] = rank
            if actor not in self._alive_actors_to_in_flight_tasks_heap:
                assert state.num_tasks_in_flight <= self.max_tasks_in_flight_per_actor()
                self._alive_actors_to_in_flight_tasks_heap[actor] = rank
        else:
            if actor in self._alive_actors_to_in_flight_tasks_heap:
                del self._alive_actors_to_in_flight_tasks_heap[actor]
            node_id = state.actor_location
            node_heap = self._alive_node_to_actor_heap.get(node_id)
            if node_heap is not None and actor in node_heap:
                del node_heap[actor]

    def _add_pending_actor(
        self,
        actor: ActorHandle,
        ready_ref: ObjectRef,
        resource_usage: ExecutionResources,
    ):
        """Adds a pending actor to the pool.

        This actor won't be pickable until it is marked as running via a
        pending_to_running() call.

        Args:
            actor: The not-yet-ready actor to add as pending to the pool.
            ready_ref: The ready future for the actor.
            resource_usage: The actual resource usage for this actor.
        """
        self._pending_actors[ready_ref] = actor
        self._actor_resource_usage[actor] = resource_usage
        self._total_usage = self._total_usage.add(resource_usage)
        self._pending_or_restarting_usage = self._pending_or_restarting_usage.add(
            resource_usage
        )

    def _get_logical_ids(self) -> List[LogicalActorId]:
        """Get the logical IDs for pending and running actors in the actor pool.

        We can't use Ray Core actor IDs because we need to identify actors by labels,
        but labels must be set before creation, and actor IDs aren't available until
        after.
        """
        return list(self._actor_to_logical_id.values())

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
            usage = self._actor_resource_usage.pop(actor)
            self._total_usage = self._total_usage.subtract(usage)
            self._pending_or_restarting_usage = (
                self._pending_or_restarting_usage.subtract(usage)
            )
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

    def _release_pending_actors(self, force: bool):
        # Release pending actors from the set of pending ones
        pending = dict(self._pending_actors)
        self._pending_actors.clear()
        for actor in pending.values():
            usage = self._actor_resource_usage.pop(actor)
            self._total_usage = self._total_usage.subtract(usage)
            self._pending_or_restarting_usage = (
                self._pending_or_restarting_usage.subtract(usage)
            )
            self._actor_to_logical_id.pop(actor, None)

        if force:
            for actor in pending.values():
                # NOTE: Actors can't be brought back after being ``ray.kill``-ed,
                #       hence we're only doing that if this is a forced release
                ray.kill(actor)

    def _release_running_actors(self, force: bool):
        running = list(self._running_actors.keys())

        for actor in running:
            self._release_running_actor(actor)

        # NOTE: Actors can't be brought back after being ``ray.kill``-ed,
        #       hence we're only doing that if this is a forced release
        if force:
            for actor in running:
                ray.kill(actor)

    def _release_running_actor(self, actor: ActorHandle):
        """Remove the given actor from the pool by dropping all pool references."""
        # NOTE: By default, we remove references to the actor and let ref counting
        # garbage collect the actor, instead of using ray.kill.
        #
        # Otherwise, actor cannot be reconstructed for the purposes of produced
        # object's lineage reconstruction.
        if actor not in self._running_actors:
            return

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

        if actor in self._alive_actors_to_in_flight_tasks_heap:
            del self._alive_actors_to_in_flight_tasks_heap[actor]

        node_id = actor_state.actor_location
        node_heap = self._alive_node_to_actor_heap.get(node_id)
        if node_heap is not None and actor in node_heap:
            del node_heap[actor]

        del self._running_actors[actor]
        del self._actor_to_logical_id[actor]

        usage = self._actor_resource_usage.pop(actor)
        self._total_usage = self._total_usage.subtract(usage)
        if actor_state.is_restarting:
            self._pending_or_restarting_usage = (
                self._pending_or_restarting_usage.subtract(usage)
            )

    def _find_actor_with_locality(self, bundle: RefBundle) -> Optional[ActorHandle]:
        """Find the least-busy alive actor on the preferred node with the most data.

        Preferred nodes are visited in descending order of bytes on-node.
        For each node, the per-node heap gives the least-busy actor in O(1).

        Args:
            bundle: The bundle to find an actor for.

        Returns:
            The best locality-aware actor, or None if none are available.
        """
        preferred_locs = bundle.get_preferred_object_locations()
        if not preferred_locs:
            return None

        max_tasks = self.max_tasks_in_flight_per_actor()

        for node_id, _total_bytes in sorted(
            preferred_locs.items(), key=lambda item: (-item[1], item[0])
        ):
            node_heap = self._alive_node_to_actor_heap.get(node_id)
            if not node_heap:
                continue
            actor, rank = node_heap.peekitem()
            if rank < max_tasks:
                return actor

        return None

    def current_logical_usage(self) -> ExecutionResources:
        return self._total_usage

    def pending_logical_usage(self) -> ExecutionResources:
        return self._pending_or_restarting_usage
