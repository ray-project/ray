from __future__ import annotations

import copy
import functools
import logging
import math
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import replace
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    Final,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from typing_extensions import override

from ray._private.ray_constants import (
    DEFAULT_OBJECT_STORE_MEMORY_PROPORTION,
    DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION,
)
from ray.data._internal.util import GiB

if TYPE_CHECKING:
    import pyarrow as pa

    from ray.data._internal.execution.block_ref_counter import BlockRefCounter

import ray
from ray import ObjectRef
from ray._raylet import ObjectRefGenerator
from ray.data._internal.compute import (
    ActorPoolStrategy,
    ComputeStrategy,
    TaskPoolStrategy,
)
from ray.data._internal.execution.bundle_queue import (
    BaseBundleQueue,
    EstimateSize,
    FIFOBundleQueue,
    RebundleQueue,
    ReorderingBundleQueue,
)
from ray.data._internal.execution.interfaces import (
    BlockSlice,
    ExecutionOptions,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
    TaskExecDriverStats,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.interfaces.ref_bundle import (
    _iter_sliced_blocks,
)
from ray.data._internal.execution.lineage_tracker import (
    ObjectReuseStatus,
    ParentBlockOutput,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
    OneToOneOperator,
)
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    CustomOpStatsReporter,
    MapTransformer,
    TransformClock,
)
from ray.data._internal.execution.util import (
    memory_string,
    merge_label_selector,
    yield_block_with_stats,
)
from ray.data._internal.stats import StatsDict
from ray.data._internal.util import MemoryProfiler, iterate_with_retry
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    BlockStats,
    TaskExecWorkerStats,
    to_stats,
)
from ray.data.context import DataContext

logger = logging.getLogger(__name__)

SAFE_DEFAULT_LOGICAL_MEMORY_PER_CPU: Final[int] = int(
    4
    * GiB
    * (
        1
        - DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION
        - DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
    )
)
"""A safe default logical memory to request for map tasks and actors per logical CPU.

Ray Data aims to guarantee that if you set each UDF's logical memory to at least
the heap memory that UDF needs, the system won't oversubscribe tasks and actors.

The problem is that if you set logical memory for some UDFs but not others, the
unspecified ones fall back to 0 and the system oversubscribes anyway.

To avoid that, we can default to ~2.57 GiB per CPU core. Here's where that magic number
comes from: We want to pick the largest logical memory (so it's safe) that won't
decrease concurrency (so we don't regress performance). Hyperscaler nodes usually
have 4 GiB per CPU core, and Ray Core sets logical memory to physical memory minus
30% for the object store and 10% for system-reserved memory. So, in the typical
case, you end up with 4 GiB * 60% = ~2.57 GiB GiB of logical memory per core, and
that's the highest you can go before you start decreasing concurrency.

We use this heuristic over more sophisticated alternatives because a constant
default is easy to reason about.
"""


def get_safe_default_logical_memory(ray_remote_args: Dict[str, Any]) -> int:
    """Return a safe default logical memory (in bytes) for the given remote args."""
    num_cpus = ray_remote_args.get("num_cpus")
    if not num_cpus:
        # If the map tasks or actors don't require logical CPUs, just assume it
        # requires one logical CPU for the purpose of computing a default value.
        default_memory = math.ceil(SAFE_DEFAULT_LOGICAL_MEMORY_PER_CPU)
    else:
        default_memory = math.ceil(SAFE_DEFAULT_LOGICAL_MEMORY_PER_CPU * num_cpus)

    assert isinstance(default_memory, int), default_memory
    assert default_memory > 0, default_memory
    return default_memory


@ray.remote(num_cpus=0)
def _get_arrow_schema_from_block(block: Block) -> "pa.Schema":
    """Extract PyArrow schema from a block by converting a 1-row sample.

    This runs on a worker to avoid fetching block data to the driver.
    Uses num_cpus=0 since it's a lightweight metadata operation.

    Slices to 1 row before converting to Arrow to minimize conversion overhead
    for large Pandas blocks. This ensures schema is consistent with actual
    block conversion logic (e.g., pa.Table.from_pandas).
    """
    accessor = BlockAccessor.for_block(block)
    sample_block = accessor.slice(0, 1)
    sample_accessor = BlockAccessor.for_block(sample_block)
    return sample_accessor.to_arrow().schema


def _get_schema_from_bundle(
    bundle: RefBundle,
    label_selector: Optional[Dict[str, str]] = None,
) -> Optional["pa.Schema"]:
    """Extract PyArrow schema from a RefBundle.

    For Arrow schemas, returns directly. For Pandas blocks, runs a lightweight
    remote task to convert a 1-row sample to Arrow and extract the schema.
    This ensures schema consistency with actual block conversion logic without
    fetching block data to the driver.
    """
    import pyarrow as pa

    from ray.data._internal.pandas_block import PandasBlockSchema
    from ray.data.dataset import Schema

    if bundle.schema is None:
        return None

    schema = bundle.schema

    # Unwrap Schema wrapper if present
    if isinstance(schema, Schema):
        schema = schema.base_schema

    # Already a PyArrow schema - use directly
    if isinstance(schema, pa.Schema):
        return schema

    # PandasBlockSchema - use remote task to convert via actual block conversion
    # This runs on a worker to avoid fetching block data to the driver
    if isinstance(schema, PandasBlockSchema):
        if not bundle.blocks:
            return None
        block_ref = bundle.blocks[0].ref
        task = _get_arrow_schema_from_block
        if label_selector:
            task = task.options(label_selector=label_selector)
        schema_ref = task.remote(block_ref)
        return ray.get(schema_ref)

    return None


class MapOperator(InternalQueueOperatorMixin, OneToOneOperator, ABC):
    """A streaming operator that maps input bundles 1:1 to output bundles.

    This operator implements the distributed map operation, supporting both task
    and actor compute strategies.
    """

    MAP_UDF_WARN_SIZE_THRESHOLD = 100 * 1024**2
    """
    Warn if the size of the map UDF exceeds this threshold.
    """

    def __init__(
        self,
        map_transformer: MapTransformer,
        input_op: PhysicalOperator,
        data_context: DataContext,
        name: str,
        target_max_block_size_override: Optional[int],
        min_rows_per_bundle: Optional[int],
        ref_bundler: Optional[RebundleQueue],
        supports_fusion: bool,
        map_task_kwargs: Optional[Dict[str, Any]],
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]],
        ray_remote_args: Optional[Dict[str, Any]],
        on_start: Optional[Callable[[Optional["pa.Schema"]], None]] = None,
        default_logical_memory_enabled: bool = False,
    ):
        # NOTE: This constructor should not be called directly; use MapOperator.create()
        # instead.
        # NOTE: This constructor must be called by subclasses.
        if map_task_kwargs is None:
            map_task_kwargs = {}

        if ray_remote_args is None:
            ray_remote_args = {}

        ray_remote_args = _canonicalize_ray_remote_args(ray_remote_args)

        # Configure a default logical memory to improve memory safety when the user has
        # specified memory for some UDFs but not others.
        if default_logical_memory_enabled:
            default_memory = get_safe_default_logical_memory(ray_remote_args)
            ray_remote_args.setdefault("memory", default_memory)
            logger.debug(
                f"Operator {name!r} set default logical memory to {default_memory}"
            )

        self._map_transformer = map_transformer
        self._supports_fusion = supports_fusion
        self._map_task_kwargs = map_task_kwargs
        self._ray_remote_args = ray_remote_args
        self._ray_remote_args_fn = ray_remote_args_fn
        self._remote_args_for_metrics = copy.deepcopy(self._ray_remote_args)

        # Bundles block references up to the min_rows_per_bundle target.
        self._block_ref_bundler = ref_bundler
        if self._block_ref_bundler is None:
            self._block_ref_bundler = RebundleQueue(EstimateSize(min_rows_per_bundle))

        # Queue for task outputs, either ordered or unordered (this is set by start()).
        self._output_queue: Optional[BaseBundleQueue] = None
        # Output metadata, added to on get_next().
        self._output_blocks_stats: List[BlockStats] = []
        # All active `DataOpTask`s.
        self._data_tasks: Dict[int, DataOpTask] = {}
        self._next_data_task_idx = 0
        # All active `MetadataOpTask`s.
        self._metadata_tasks: Dict[int, MetadataOpTask] = {}
        self._next_metadata_task_idx = 0
        # Input bundles of this op's seed tasks, retained so object-loss recovery can
        # re-inject them. `register_task_failed` hands back seed *ids*, and the tracker
        # holds no `RefBundle`s by design, so something has to turn an id back into a
        # resubmittable bundle. Costs no memory: `InputDataBuffer` indexes rather than
        # pops, keeping these refs alive for the whole run anyway. Empty unless
        # recovery is enabled and this op reads straight from an `InputDataBuffer`
        # (see `_anchors_seed_input`).
        self._seed_task_inputs: Dict[str, RefBundle] = {}
        # block hex -> queue of (seed task id, plan id) for a seed input queued for
        # re-injection. A seed's input comes from the source, not from a task, so no
        # producer recorded it and `_lineage_for_submission` cannot look it up.
        # `_recover_lost_object` stamps the identity here; submission pops it.
        #
        # A queue, not a single value: a second plan tracing back to the same seed
        # re-injects the same bundle behind the one already queued.
        self._pending_seed_ids: Dict[str, Deque[Tuple[str, str]]] = {}
        # block hex -> (child task id, plan id) for an input set owed to a
        # reconstruction child. The producer's plan bucket records this, but
        # `register_task_complete` discharges it in the producer's done callback,
        # before the consumer's turn to submit. So the producer stamps it here at
        # release; submission pops it. A single value: a child is released once per
        # plan, and the join rule keeps plans from sharing a child.
        self._pending_child_ids: Dict[str, Tuple[str, str]] = {}
        # plan id -> {(producing data task id, output index): held bundle}, for
        # re-produced outputs withheld from `_output_queue` until every parent of the
        # consumer has delivered its share. A reconstruction child must run against
        # exactly the input set its first attempt consumed, and those inputs arrive one
        # completing parent at a time -- so they are collected here and released as one
        # bundle by whichever parent completes last (see
        # `_release_reconstruction_children`).
        #
        # Collecting them *here*, on the producer, rather than downstream is what keeps
        # this race-free: every parent of a child is a task of this same operator (a
        # `MapOperator` has exactly one input dependency, and only `MapOperator`
        # registers outputs), so the whole set is in hand synchronously, in the very
        # callback that discovers the child is ready.
        #
        # Keyed by plan rather than by child: a block is dispatched to exactly one
        # consumer, so which child is owed a slot only needs resolving at release.
        # Empty unless recovery is enabled and a plan is in flight.
        self._reconstruction_outputs: Dict[str, Dict[Tuple[str, int], RefBundle]] = {}
        # Keep track of all finished streaming generators.
        super().__init__(name, input_op, data_context, target_max_block_size_override)

        # If set, then all output blocks will be split into
        # this many sub-blocks. This is to avoid having
        # too-large blocks, which may reduce parallelism for
        # the subsequent operator.
        self._additional_split_factor = None
        # Callback functions that generate additional task kwargs
        # for the map task.
        self._map_task_kwargs_fns: List[Callable[[], Dict[str, Any]]] = []
        # Callback for when first input bundle is ready (before task submission).
        # Receives schema from the first bundle for deferred initialization
        # (e.g., schema evolution for Iceberg writes via on_write_start).
        self._on_start: Optional[Callable[[Optional["pa.Schema"]], None]] = on_start
        self._on_start_called = False

    @functools.cached_property
    def _map_transformer_ref(self) -> ObjectRef[MapTransformer]:
        """Lazily serialize _map_transformer to object store on first access.

        Deferred until first task submission so that on_start callbacks
        (e.g., on_write_start for Iceberg) can modify the transformer state
        before serialization.
        """
        # _map_transformer_ref is lazily initialized on first access.
        # This ensures on_start callback (if registered) can modify the transformer
        # before serialization (e.g., for Iceberg schema evolution).
        ref = ray.put(self._map_transformer)
        self._warn_large_udf(ref)
        return ref

    @functools.cached_property
    def _data_context_ref(self) -> ObjectRef[DataContext]:
        return ray.put(self.data_context)

    def add_map_task_kwargs_fn(self, map_task_kwargs_fn: Callable[[], Dict[str, Any]]):
        """Add a callback function that generates additional kwargs for the map tasks.
        In the map tasks, the kwargs can be accessible via `TaskContext.kwargs`.
        """
        self._map_task_kwargs_fns.append(map_task_kwargs_fn)

    def _notify_first_input(self, bundled_input: RefBundle) -> None:
        """Invoke on_start callback with schema if registered and not yet invoked.

        Used for deferred initialization that needs schema from the first bundle
        (e.g., schema evolution for Iceberg writes via on_write_start).
        """
        if not self._on_start_called and self._on_start is not None:
            schema = _get_schema_from_bundle(
                bundled_input,
                label_selector=self.data_context.execution_options.label_selector,
            )
            self._on_start(schema)
            self._on_start_called = True
            # Note: _map_transformer_ref is lazily initialized, so no need to
            # re-serialize here - it will be created with the updated state
            # when first accessed in _add_bundled_input.

    def get_map_task_kwargs(self) -> Dict[str, Any]:
        """Get the kwargs for the map task.
        Subclasses should pass the returned kwargs to the map tasks.
        In the map tasks, the kwargs can be accessible via `TaskContext.kwargs`.
        """
        kwargs = self._map_task_kwargs.copy()
        for fn in self._map_task_kwargs_fns:
            kwargs.update(fn())
        return kwargs

    def get_additional_split_factor(self) -> int:
        if self._additional_split_factor is None:
            return 1
        return self._additional_split_factor

    def set_additional_split_factor(self, k: int):
        self._additional_split_factor = k

    @property
    def name(self) -> str:
        name = super().name
        if self._additional_split_factor is not None:
            name += f"->SplitBlocks({self._additional_split_factor})"
        return name

    @classmethod
    def create(
        cls,
        map_transformer: MapTransformer,
        input_op: PhysicalOperator,
        data_context: DataContext,
        target_max_block_size_override: Optional[int] = None,
        name: str = "Map",
        # TODO(ekl): slim down ComputeStrategy to only specify the compute
        # config and not contain implementation code.
        compute_strategy: Optional[ComputeStrategy] = None,
        min_rows_per_bundle: Optional[int] = None,
        ref_bundler: Optional[RebundleQueue] = None,
        supports_fusion: bool = True,
        map_task_kwargs: Optional[Dict[str, Any]] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        per_block_limit: Optional[int] = None,
        on_start: Optional[Callable[[Optional["pa.Schema"]], None]] = None,
        isolate_workers: bool = False,
    ) -> "MapOperator":
        """Create a MapOperator.

        This factory creates the MapOperator pool implementation that corresponds to the
        compute argument:
            - If None or TaskPoolStrategy -> TaskPoolMapOperator
            - If ActorPoolStrategy -> ActorPoolMapOperator

        Args:
            map_transformer: The :class:`MapTransformer` to apply to each ref
                bundle input.
            input_op: Operator generating input data for this op.
            data_context: The :class:`DataContext` to use for this operator.
            target_max_block_size_override: Override for target max-block-size.
            name: The name of this operator.
            compute_strategy: Customize the compute strategy for this op.
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
            ray_remote_args: Customize the :func:`ray.remote` args for this op's tasks.
            per_block_limit: Maximum number of rows to process per block, for early termination.
            on_start: Optional callback invoked with the schema from the first input
                bundle before any tasks are submitted. Used for deferred initialization
                that requires schema from actual data (e.g., schema evolution for
                Iceberg writes).
            isolate_workers: If ``True``, ensure that other operators' tasks don't get
                scheduled on the same worker processes as this operator's. This flag
                is useful to prevent side-effects from affecting other operators, like
                large PyArrow memory allocations.

        Returns:
            A ``MapOperator`` instance whose concrete subclass depends on the
            requested compute strategy.
        """
        if (ref_bundler is not None and min_rows_per_bundle is not None) or (
            min_rows_per_bundle is not None and ref_bundler is not None
        ):
            raise ValueError(
                "min_rows_per_bundle and ref_bundler cannot be used together"
            )

        if compute_strategy is None:
            compute_strategy = TaskPoolStrategy()

        # Apply per-block limit to the map transformer if set
        if per_block_limit is not None:
            map_transformer = _wrap_transformer_with_limit(
                map_transformer, per_block_limit
            )
        if isinstance(compute_strategy, TaskPoolStrategy):
            from ray.data._internal.execution.operators import (
                get_task_pool_map_operator_cls,
            )

            TaskPoolMapOperator = get_task_pool_map_operator_cls()
            return TaskPoolMapOperator(
                map_transformer,
                input_op,
                data_context,
                name=name,
                target_max_block_size_override=target_max_block_size_override,
                min_rows_per_bundle=min_rows_per_bundle,
                ref_bundler=ref_bundler,
                max_concurrency=compute_strategy.size,
                supports_fusion=supports_fusion,
                map_task_kwargs=map_task_kwargs,
                ray_remote_args_fn=ray_remote_args_fn,
                ray_remote_args=ray_remote_args,
                on_start=on_start,
                isolate_workers=isolate_workers,
                default_logical_memory_enabled=data_context.default_map_logical_memory_enabled,
            )
        elif isinstance(compute_strategy, ActorPoolStrategy):
            from ray.data._internal.execution.operators import (
                get_actor_pool_map_operator_cls,
            )

            if isolate_workers:
                logger.debug(
                    "`isolate_workers` is set but has no effect with "
                    "`ActorPoolStrategy` because actors are already isolated."
                )

            ActorPoolMapOperator = get_actor_pool_map_operator_cls()
            return ActorPoolMapOperator(
                map_transformer,
                input_op,
                data_context,
                target_max_block_size_override=target_max_block_size_override,
                compute_strategy=compute_strategy,
                name=name,
                min_rows_per_bundle=min_rows_per_bundle,
                ref_bundler=ref_bundler,
                supports_fusion=supports_fusion,
                map_task_kwargs=map_task_kwargs,
                ray_remote_args_fn=ray_remote_args_fn,
                ray_remote_args=ray_remote_args,
                on_start=on_start,
                default_logical_memory_enabled=data_context.default_map_logical_memory_enabled,
            )
        else:
            raise ValueError(f"Unsupported execution strategy {compute_strategy}")

    def start(
        self,
        options: "ExecutionOptions",
        block_ref_counter: "BlockRefCounter",
    ):
        super().start(options, block_ref_counter)
        # Create output queue with desired ordering semantics.
        if options.preserve_order:
            self._output_queue = ReorderingBundleQueue()
        else:
            self._output_queue = FIFOBundleQueue()

        map_transformer = self._map_transformer
        # Apply additional block split if needed.
        if self.get_additional_split_factor() > 1:
            split_factor = self.get_additional_split_factor()
            split_transformer = MapTransformer(
                [
                    BlockMapTransformFn(
                        lambda blocks, ctx: _split_blocks(blocks, split_factor),
                        # NOTE: Disable block-shaping to avoid it overriding
                        #       splitting
                        disable_block_shaping=True,
                    )
                ]
            )
            map_transformer = map_transformer.fuse(split_transformer)

        # Store the potentially modified map_transformer for later use
        self._map_transformer = map_transformer

    def _warn_large_udf(self, udf: ObjectRef[MapTransformer]):
        """Print a warning if the UDF is too large."""
        udf_size = ray.experimental.get_local_object_locations([udf])[udf][
            "object_size"
        ]
        if udf_size > self.MAP_UDF_WARN_SIZE_THRESHOLD:
            logger.warning(
                f"The UDF of operator {self.name} is too large "
                f"(size = {memory_string(udf_size)}). "
                "Check if the UDF has accidentally captured large objects. "
                "Load the large objects in the __init__ method "
                "or pass them as ObjectRefs instead."
            )

    def _add_input_inner(self, refs: RefBundle, input_index: int):
        assert input_index == 0, input_index

        # A reconstruction child's input set was assembled complete by its producer
        # (`_release_reconstruction_children`), and it must run against *exactly* that
        # set. `RebundleQueue` would merge it with whatever else is pending -- running
        # two children as one task and stranding one of them -- or `slice()` it to hit
        # the row target, re-emitting part of a block. So bypass the bundler.
        #
        # This costs no backpressure: `OpState.dispatch_next_task` is the only caller of
        # `add_input` and only runs for operators `get_eligible_operators` has already
        # cleared, so the strict contract holds here exactly as it does below. The stamp
        # is read rather than popped; `_lineage_for_submission` still pops it at
        # submission to name the task.
        if self._pending_child_ids and any(
            entry.ref.hex() in self._pending_child_ids for entry in refs.blocks
        ):
            self._try_schedule_task(refs, strict=True)
            return

        # Add RefBundle to the bundler.
        self._block_ref_bundler.add(refs)
        self._metrics.on_input_queued(refs, input_index=0)

        if self._block_ref_bundler.has_next():
            # The ref bundler combines one or more `RefBundle`s into a new
            # `RefBundle`. To update metrics appropriately, we need to deque
            # original input bundles.
            (
                bundled_input,
                input_refs,
            ) = self._block_ref_bundler.get_next_with_original()
            for bundle in input_refs:
                self._metrics.on_input_dequeued(bundle, input_index=0)

            # If the bundler has a full bundle, add it to the operator's task submission
            # queue
            #
            # NOTE: This is a strict path, hence operator is *required* to launch
            #       at least 1 task
            self._try_schedule_task(bundled_input, strict=True)

    def _get_dynamic_ray_remote_args(
        self, input_bundle: Optional[RefBundle] = None
    ) -> Dict[str, Any]:
        ray_remote_args = copy.deepcopy(self._ray_remote_args)

        # max_calls isn't supported in `.options()`, so we remove it when generating dynamic ray_remote_args
        ray_remote_args.pop("max_calls", None)

        # Override parameters from user provided remote args function.
        if self._ray_remote_args_fn:
            new_remote_args = self._ray_remote_args_fn()
            for k, v in new_remote_args.items():
                ray_remote_args[k] = v
        # For tasks with small args, we will use SPREAD by default to optimize for
        # compute load-balancing. For tasks with large args, we will use DEFAULT to
        # allow the Ray locality scheduler a chance to optimize task placement.
        if "scheduling_strategy" not in ray_remote_args:
            ctx = self.data_context
            if input_bundle and input_bundle.size_bytes() > ctx.large_args_threshold:
                ray_remote_args[
                    "scheduling_strategy"
                ] = ctx.scheduling_strategy_large_args
                # Takes precedence over small args case. This is to let users know
                # when the large args case is being triggered.
                self._remote_args_for_metrics = copy.deepcopy(ray_remote_args)
            else:
                ray_remote_args["scheduling_strategy"] = ctx.scheduling_strategy
                # Only save to metrics if we haven't already done so.
                if "scheduling_strategy" not in self._remote_args_for_metrics:
                    self._remote_args_for_metrics = copy.deepcopy(ray_remote_args)
        ray_remote_args = merge_label_selector(
            ray_remote_args, self.data_context.execution_options.label_selector
        )
        return ray_remote_args

    @abstractmethod
    def _try_schedule_task(self, refs: RefBundle, strict: bool):
        """Method to try schedule task handling provided bundle

        Args:
            refs: The fully-bundled ref bundle that should be added as input.
            strict: Controls whether operator has to strictly follow the input handling
                    protocol and guarantee that at least 1 task is launched.
        """
        pass

    def _anchors_seed_input(self) -> bool:
        """Whether this op anchors a seed input for object-loss recovery.

        An op anchors when it consumes directly from an ``InputDataBuffer`` -- a
        source with no upstream lineage, whose output bundle is therefore the
        durable, resubmittable seed input. This covers, uniformly:
          - ``Read`` (V1 / ``range``, input is a ``ReadTask``) and ``ReadFiles``
            (V2, input is a ``FileManifest``): the input is tiny metadata and
            re-running the task reproduces the data.
          - ``from_blocks`` / ``from_items`` / ``from_pandas`` / ``from_arrow``
            and cached blocks: the input *is* the data, captured durably as-is.
        In every case recovery re-injects the captured input into this op.
        """
        from ray.data._internal.execution.operators.input_data_buffer import (
            InputDataBuffer,
        )

        return any(isinstance(dep, InputDataBuffer) for dep in self.input_dependencies)

    def _data_task_id_for(self, task_index: int) -> str:
        """The lineage id of this operator's ``task_index``-th fresh task.

        The one place the format is known; ``owns_data_task`` is its inverse.
        Everything else, the tracker included, treats the id as an opaque string.
        """
        return f"{self.id}:{task_index}"

    @override
    def owns_data_task(self, data_task_id: str) -> bool:
        return data_task_id.rsplit(":", 1)[0] == self.id

    @override
    def retained_seed_input(self, seed_task_id: str) -> Optional[RefBundle]:
        return self._seed_task_inputs.get(seed_task_id)

    def stamp_seed_reinjection(
        self, seed_task_id: str, plan_id: str, seed_input: RefBundle
    ) -> None:
        """Carry a re-injected seed's identity across to its resubmission.

        A seed's input comes from the source rather than from a task, so no producer
        recorded it and `_lineage_for_submission` cannot look it up. Without this the
        re-injected bundle is minted a fresh id and the plan never resolves.

        Every block of the bundle is stamped, so a bundler merge cannot hide the one
        that gets looked at.
        """
        for block_ref in seed_input.block_refs:
            self._pending_seed_ids.setdefault(block_ref.hex(), deque()).append(
                (seed_task_id, plan_id)
            )

    def _lineage_for_submission(
        self, task_index: int, inputs: RefBundle
    ) -> Tuple[Optional[str], Optional[str], List[ParentBlockOutput]]:
        """Work out what this attempt should register with the lineage graph.

        Returns ``(data_task_id, plan_id, dependencies)``, where ``plan_id`` is the
        plan this attempt serves -- ``None`` for a fresh attempt -- or
        ``(None, None, [])`` when object-loss recovery is off for this DAG.

        A fresh attempt is named ``f"{self.id}:{task_index}"``. A *reconstruction*
        must re-use the original logical id instead, or the plan never resolves and
        the graph grows a duplicate node.
        """
        if self._lineage_tracker is None:
            return None, None, []

        # A re-injected seed is the one identity that cannot be looked up: its input
        # came from the source rather than from a task, so no producer ever recorded
        # it. `_recover_lost_object` carries the id across instead. Check first --
        # the lookup below would find nothing for these blocks.
        #
        # Every block of the seed input is stamped (so a bundler merge cannot hide the
        # one we look at), so drain this bundle's entry from *all* of them rather than
        # stopping at the first. A leftover entry would otherwise be picked up by a
        # later re-injection of the same bundle and pair it with the wrong plan.
        seed = None
        if self._pending_seed_ids:
            for block_ref in inputs.block_refs:
                queued = self._pending_seed_ids.get(block_ref.hex())
                if not queued:
                    continue
                claimed = queued.popleft()
                if not queued:
                    del self._pending_seed_ids[block_ref.hex()]
                if seed is None:
                    seed = claimed
        if seed is not None:
            seed_id, plan_id = seed
            return seed_id, plan_id, []

        # One lookup, two uses. Passing every ref rather than just ``block_refs[0]``
        # matters: `RebundleQueue` parks zero-row bundles and prepends them on the
        # next merge, so the block of interest is not necessarily first.
        dependencies = self._lineage_tracker.resolve_dependencies(
            block_ref.hex() for block_ref in inputs.block_refs
        )

        # A re-produced block carries the identity of the child it is owed to. Pop
        # every match rather than stopping at the first: a bundle can merge several
        # of them, and a stamp left behind would misidentify some later task.
        reconstruction = None
        if self._pending_child_ids:
            for block_ref in inputs.block_refs:
                child = self._pending_child_ids.pop(block_ref.hex(), None)
                if child is not None and reconstruction is None:
                    reconstruction = child

        if reconstruction is not None:
            data_task_id, plan_id = reconstruction
            return data_task_id, plan_id, dependencies

        return self._data_task_id_for(task_index), None, dependencies

    def _release_reconstruction_children(
        self, data_task_id: str, plan_id: str, task_index: int
    ) -> None:
        """Release the reconstruction children whose whole input set is now in hand.

        Called from the completing parent's task-done callback. Every parent of a child
        is a task of this operator, and each one's re-produced outputs were withheld
        into ``_reconstruction_outputs`` as they were generated, so the parent that
        completes last holds the child's full input set and can hand it downstream as a
        single bundle.

        Readiness is decided here rather than in the tracker: the withheld blocks are
        this operator's, so "is the child's whole input set in hand" is a local
        question. A child is released exactly once because the release pops its slots
        out of ``held`` -- a parent completing later finds them gone and skips it.

        The assembled bundle is stamped with the child's identity, because a
        reconstruction child must be submitted under its *original* id: minting a fresh
        one abandons the plan and grows a second node for the same logical task, after
        which the parent lists two consumers for one output and pruning is computed
        against a doubled map. Stamping before the bundle enters ``_output_queue`` means
        the mark is always in place by the time the consumer's ``add_input`` runs.
        """
        held = self._reconstruction_outputs.get(plan_id, {})
        for child_task_id, requirements in self._lineage_tracker.get_pending_children(
            data_task_id, plan_id
        ).items():
            # Requirement order is the child's original input order: the map is built
            # from `parent_tasks` / `child_task_block_dependencies` insertion order,
            # which is the order the first attempt's dependencies were registered in.
            slots = [
                (parent_task_id, output_index)
                for parent_task_id, output_indices in requirements.items()
                for output_index in output_indices
            ]
            missing = [slot for slot in slots if slot not in held]
            if missing:
                # Some parent of this child has not re-produced its share yet. Wait:
                # every parent serving the plan runs this on completion, and a parent's
                # outputs are withheld before its own done-callback, so whichever
                # completes last holds the whole set. Submitting now would run the child
                # against part of its input and silently emit a subset of its rows.
                logger.debug(
                    "[lineage-recovery] Child %s of plan %s is not ready: %d of its "
                    "input blocks are still pending (missing %s, held %s).",
                    child_task_id,
                    plan_id,
                    len(missing),
                    missing,
                    sorted(held),
                )
                continue

            inputs = RefBundle.merge_ref_bundles([held.pop(slot) for slot in slots])
            self._stamp_reconstruction_child(child_task_id, plan_id, inputs)
            # Queued under the completing task's index, just before this callback
            # finalizes that key, so the bundle travels the ordinary path downstream
            # (backpressure, metrics and `add_output` all unchanged).
            self._output_queue.add(inputs, key=task_index)
            self._metrics.on_output_queued(inputs)

        if not held:
            self._reconstruction_outputs.pop(plan_id, None)

    def _stamp_reconstruction_child(
        self, child_task_id: str, plan_id: str, inputs: RefBundle
    ) -> None:
        """Mark an assembled input set with the child task id it belongs to.

        Addressed to the consumer that owns the child rather than broadcast to every
        output dependency. No adjacent consumer owning it means some non-map operator
        sits between them: there is nobody to carry the identity to, and the bundle is
        submitted as ordinary work. Warned about rather than raised: the rows still
        flow.
        """
        for downstream_op in self.output_dependencies:
            if downstream_op.owns_data_task(child_task_id):
                # Only a MapOperator mints data task ids, so the owner has the dict.
                assert isinstance(downstream_op, MapOperator), type(downstream_op)
                for block_ref in inputs.block_refs:
                    downstream_op._pending_child_ids[block_ref.hex()] = (
                        child_task_id,
                        plan_id,
                    )
                return

        logger.warning(
            "[lineage-recovery] No adjacent consumer for reconstruction child %s "
            "(plan %s); its re-execution will be registered as a fresh task.",
            child_task_id,
            plan_id,
        )

    def _submit_data_task(
        self,
        gen: ObjectRefGenerator,
        inputs: RefBundle,
        task_done_callback: Optional[Callable[[], None]] = None,
    ):
        """Submit a new data-handling task."""
        # TODO(hchen):
        # 1. Move this to the base PhyscialOperator class.
        # 2. This method should only take a block-processing function as input,
        #    instead of a streaming generator. The logic of submitting ray tasks
        #    can also be capsulated in the base class.
        task_index = self._next_data_task_idx
        self._next_data_task_idx += 1

        # Resolve this attempt's lineage identity up front so the callbacks below
        # can close over it. (None, None, []) when recovery is disabled.
        data_task_id, plan_id, dependencies = self._lineage_for_submission(
            task_index, inputs
        )
        if data_task_id is not None:
            self._lineage_tracker.register_task_submission(
                data_task_id, dependencies, plan_id
            )
            if self._anchors_seed_input():
                # A seed consumes straight from an `InputDataBuffer`, so its input is
                # durable and resubmittable. `register_task_failed` hands back seed
                # *ids* and the tracker stores no `RefBundle`s, so keep it here.
                self._seed_task_inputs[data_task_id] = inputs

        # This task's next output_index. A per-task closure local, so it is scoped
        # exactly right and resets naturally on a re-execution.
        num_outputs_emitted = 0

        def _output_ready_callback(
            task_index,
            output: RefBundle,
        ):
            nonlocal num_outputs_emitted
            # Since output is streamed, it should only contain one block.
            assert len(output) == 1
            output_index = num_outputs_emitted
            num_outputs_emitted += 1
            self._metrics.on_task_output_generated(task_index, output)

            if data_task_id is not None:
                # Attribute the block to (this task, output_index) so whichever
                # downstream task consumes it can name its own dependencies.
                self._lineage_tracker.register_output(
                    data_task_id, output.block_refs[0].hex(), output_index
                )
                if plan_id is not None:
                    status = self._lineage_tracker.get_object_reuse_status(
                        data_task_id, output_index, plan_id
                    )
                    # REUSED: withhold until the child's whole input set is
                    # re-produced, then release it as one bundle
                    # (`_release_reconstruction_children`). Emitting now would run the
                    # child against part of its input.
                    if status is ObjectReuseStatus.OBJECT_REUSED:
                        self._reconstruction_outputs.setdefault(plan_id, {})[
                            (data_task_id, output_index)
                        ] = output
                        return
                    # PRUNED (a copy of the rows outlives the loss) or UNRELATED (the
                    # plan already finished with this task): no consumer is waiting,
                    # so drop it rather than re-emit rows the consumer already has.
                    # NEW falls through to the output queue.
                    if status is not ObjectReuseStatus.OBJECT_NEW:
                        return

            # Notify output queue that the task has produced an new output.
            self._output_queue.add(output, key=task_index)
            self._metrics.on_output_queued(output)

        def _task_done_callback(
            task_index: int,
            exception: Optional[Exception],
            task_exec_stats: Optional[TaskExecWorkerStats],
            task_exec_driver_stats: Optional[TaskExecDriverStats],
        ):
            # NOTE: `TaskExecStats` could be null in case there's no blocks
            #       emitted (current limitation, since it's emitted along with
            #       `BlockMetadata`)
            assert exception or (
                task_exec_driver_stats
            ), "Driver's task execution stats must be provided on task's successful completion"

            self._metrics.on_task_finished(
                task_index, exception, task_exec_stats, task_exec_driver_stats
            )

            # Estimate number of tasks and rows from inputs received and tasks
            # submitted so far
            (
                _,
                self._estimated_num_output_bundles,
                self._estimated_output_num_rows,
            ) = estimate_total_num_of_blocks(
                self._next_data_task_idx, self.upstream_op_num_outputs(), self._metrics
            )

            if data_task_id is not None and exception is None:
                # Hand any child whose whole input set this completion completes
                # downstream before reporting the completion itself.
                if plan_id is not None:
                    self._release_reconstruction_children(
                        data_task_id, plan_id, task_index
                    )
                self._lineage_tracker.register_task_complete(data_task_id, plan_id)

            self._data_tasks.pop(task_index)
            # Notify output queue that this task is complete.
            self._output_queue.finalize(key=task_index)
            if task_done_callback:
                task_done_callback()

        data_task = DataOpTask(
            task_index,
            gen,
            self._block_ref_counter,
            self.id,
            output_ready_callback=lambda output: _output_ready_callback(
                task_index, output
            ),
            task_done_callback=functools.partial(_task_done_callback, task_index),
            operator_name=self.name,
            data_task_id=data_task_id,
            plan_id=plan_id,
        )
        self._metrics.on_task_submitted(
            task_index, inputs, task_id=data_task.get_task_id()
        )
        self._data_tasks[task_index] = data_task

    def _submit_metadata_task(
        self, result_ref: ObjectRef, task_done_callback: Callable[[], None]
    ):
        """Submit a new metadata-handling task."""
        # TODO(hchen): Move this to the base PhyscialOperator class.
        task_index = self._next_metadata_task_idx
        self._next_metadata_task_idx += 1

        def _task_done_callback():
            self._metadata_tasks.pop(task_index)
            task_done_callback()

        self._metadata_tasks[task_index] = MetadataOpTask(
            task_index, result_ref, _task_done_callback
        )

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._metadata_tasks.values()) + list(self._data_tasks.values())

    def all_inputs_done(self):
        self._block_ref_bundler.finalize()

        # Handle any bundles still in the bundler
        while self._block_ref_bundler.has_next():
            # The ref bundler combines one or more `RefBundle`s into a new
            # `RefBundle`. To update metrics appropriately, we need to deque
            # original input bundles.
            (
                bundled_input,
                input_refs,
            ) = self._block_ref_bundler.get_next_with_original()
            for bundle in input_refs:
                self._metrics.on_input_dequeued(bundle, input_index=0)

            # NOTE: When `all_inputs_done` is invoked we can't guarantee that the
            #       task will be launched since all actors might be busy.
            self._try_schedule_task(bundled_input, strict=False)

        assert (
            self._block_ref_bundler.estimate_size_bytes() == 0
        ), f"Bundler in {self} must be empty (got {self._block_ref_bundler.num_blocks()} blocks)"

        super().all_inputs_done()

    def has_next(self) -> bool:
        assert self._started
        return self._output_queue.has_next()

    def _held_reconstruction_bundles(self) -> Iterator[RefBundle]:
        """The re-produced outputs withheld for reconstruction children not yet ready."""
        for held in self._reconstruction_outputs.values():
            yield from held.values()

    @override
    def internal_output_queue_num_blocks(self) -> int:
        # Withheld reconstruction outputs are outputs this operator still owes a
        # consumer, so they have to count here. `has_completed` is what gates the
        # downstream operator's `all_inputs_done`, and reporting this operator complete
        # while it holds a child's inputs would leave that child unsubmitted and drop
        # its rows silently.
        return super().internal_output_queue_num_blocks() + sum(
            len(bundle.blocks) for bundle in self._held_reconstruction_bundles()
        )

    @override
    def internal_output_queue_num_bytes(self) -> int:
        return super().internal_output_queue_num_bytes() + sum(
            bundle.size_bytes() for bundle in self._held_reconstruction_bundles()
        )

    @override
    def clear_internal_output_queue(self) -> None:
        super().clear_internal_output_queue()
        self._reconstruction_outputs.clear()

    def _get_next_inner(self) -> RefBundle:
        assert self._started
        bundle = self._output_queue.get_next()
        self._metrics.on_output_dequeued(bundle)
        self._output_blocks_stats.extend(to_stats(bundle.metadata))
        return bundle

    @abstractmethod
    def progress_str(self) -> str:
        raise NotImplementedError

    def _extra_metrics(self) -> Dict[str, Any]:
        return {"ray_remote_args": dict(sorted(self._remote_args_for_metrics.items()))}

    def get_stats(self) -> StatsDict:
        return {self._name: self._output_blocks_stats}

    def get_map_transformer(self) -> MapTransformer:
        return self._map_transformer

    def _do_shutdown(self, force: bool = False):
        # Invoke base-class sequence
        super()._do_shutdown(force)
        # Release refs
        self._data_tasks.clear()
        self._metadata_tasks.clear()

    @abstractmethod
    def current_logical_usage(self) -> ExecutionResources:
        raise NotImplementedError

    @abstractmethod
    def pending_logical_usage(self) -> ExecutionResources:
        raise NotImplementedError

    @abstractmethod
    def incremental_resource_usage(self) -> ExecutionResources:
        raise NotImplementedError

    def supports_fusion(self) -> bool:
        return self._supports_fusion

    def num_active_tasks(self) -> int:
        # Override `num_active_tasks` to only include data tasks and exclude
        # metadata tasks, which are used by the actor-pool map operator to
        # check if a newly created actor is ready.
        # The reasons are because:
        # 1. `PhysicalOperator.has_completed` checks `num_active_tasks`. The operator
        #   should be considered completed if there are still pending actors.
        # 2. The number of active tasks in the progress bar will be more accurate
        #   to reflect the actual data processing tasks.
        return len(self._data_tasks)


def _map_task(
    map_transformer: MapTransformer,
    data_context: DataContext,
    ctx: TaskContext,
    *blocks: Block,
    slices: Optional[List[BlockSlice]] = None,
    **kwargs: Dict[str, Any],
) -> Iterator[Union[Block, "BlockMetadataWithSchema"]]:
    """Remote function for a single operator task.

    Args:
        map_transformer: The :class:`MapTransformer` to apply, taking
            ``Iterator[Block]`` as input and yielding ``Iterator[Block]``.
        data_context: The :class:`DataContext` to install for this task.
        ctx: The :class:`TaskContext` for the task, used to look up
            per-task settings and propagate context to the UDF.
        *blocks: The concrete block values from the task ref bundle.
        slices: List of block slices for this task to process.
        **kwargs: Additional keyword arguments stored on ``ctx.kwargs`` and
            forwarded to the map transformer.

    Yields:
        Union[Block, BlockMetadataWithSchema]: A generator of blocks, followed by the
        list of BlockMetadata for the blocks as the last generator return.
    """
    task_start_s = time.perf_counter()

    blk_exec_stats_builder = BlockExecStats.builder()

    logger.debug(
        "Executing map task of operator %s with task index %d",
        ctx.op_name,
        ctx.task_idx,
    )

    ctx.kwargs.update(kwargs)

    with DataContext.current(data_context), TaskContext.current(ctx):
        map_transformer.override_target_max_block_size(
            ctx.target_max_block_size_override
        )

        retry_on = data_context.retried_map_errors

        # NOTE: We avoid the cost of deduping schemas in the task because
        # each yielded block should have the same schema, since each one
        # is a slice of the UDF's single output block, and we know that
        # slicing preserves the schema (so all yielded blocks will have
        # the same schema)
        yielded_schema: bool = False

        # Defines a reporter a transform calls to report its ``CustomOpStats``
        # which are recorded during the task execution on the worker.
        # This is owned by _map_task so it belongs to actual, possibly-fused, running task
        # rather than a transformer instance reused across tasks.
        op_stats_reporter = CustomOpStatsReporter()
        # Likewise owned by _map_task: an actor pool shares one transformer
        # across every task the actor runs, and with
        # `max_concurrent_calls_per_actor > 1` several run at once.
        clock = TransformClock()

        def transform_iter_factory():
            # Clear any per-task custom stats before each attempt (the reporter
            # is reused across retries of this task), so a prior attempt's stats
            # can't leak into this one. A producing transform repopulates it
            # before the first block is yielded.
            op_stats_reporter.clear()
            clock.drain()
            blocks_iter = (
                _iter_sliced_blocks(blocks, slices) if slices else iter(blocks)
            )
            return map_transformer.apply_transform(
                blocks_iter,
                ctx,
                op_stats_reporter.report,
                clock=clock,
            )

        if retry_on:
            block_iter = iterate_with_retry(
                transform_iter_factory,
                description="apply UDF transform",
                match=None if retry_on is True else retry_on,
                max_attempts=data_context.max_map_retries + 1,
                unwrap_cause=True,
            )
        else:
            block_iter = transform_iter_factory()

        with MemoryProfiler(data_context.memory_usage_poll_interval_s) as profiler:
            for block in block_iter:
                block_meta = BlockAccessor.for_block(block).get_metadata()
                block_schema = BlockAccessor.for_block(block).schema()

                # Finish processing before yielding the block!
                blk_exec_stats_builder.finish()

                def build_metadata(block_ser_time_s):
                    phase_times = clock.drain()
                    exec_stats = blk_exec_stats_builder.build(
                        block_ser_time_s=block_ser_time_s,
                        block_transform_time_s=phase_times.total_s,
                        input_prep_time_s=phase_times.input_prep_s,
                        function_body_time_s=phase_times.function_body_s,
                        output_build_time_s=phase_times.output_build_s,
                        task_idx=ctx.task_idx,
                    )
                    # NOTE: This tracks task duration up to this point, though we're
                    # primarily interested in task total duration.
                    # TODO figure out a better way to track task total duration
                    task_dur_s = time.perf_counter() - task_start_s
                    return BlockMetadataWithSchema.from_metadata(
                        replace(
                            block_meta,
                            exec_stats=exec_stats,
                            task_exec_stats=TaskExecWorkerStats(
                                task_wall_time_s=task_dur_s,
                                max_uss_bytes=profiler.estimate_max_uss(),
                                # Reported by producing transforms through the
                                # per-task reporter; empty if the op reports nothing.
                                custom_op_stats=op_stats_reporter.get_stats(),
                            ),
                        ),
                        schema=block_schema if not yielded_schema else None,
                    )

                yield from yield_block_with_stats(block, build_metadata)

                # Reset trackers
                yielded_schema = True
                blk_exec_stats_builder = BlockExecStats.builder()


def _canonicalize_ray_remote_args(ray_remote_args: Dict[str, Any]) -> Dict[str, Any]:
    """Enforce rules on ray remote args for map tasks.

    Namely, args must explicitly specify either CPU or GPU, not both. Disallowing
    mixed resources avoids potential starvation and deadlock issues during scheduling,
    and should not be a serious limitation for users.
    """
    ray_remote_args = ray_remote_args.copy()

    # TODO: Might be better to log this warning at composition-time rather than at
    # execution. Validating inputs early is a good practice.
    if ray_remote_args.get("num_cpus") and ray_remote_args.get("num_gpus"):
        logger.warning(
            "Specifying both num_cpus and num_gpus for map tasks is experimental, "
            "and may result in scheduling or stability issues. "
            "Please report any issues to the Ray team: "
            "https://github.com/ray-project/ray/issues/new/choose"
        )

    if "num_cpus" not in ray_remote_args and "num_gpus" not in ray_remote_args:
        ray_remote_args["num_cpus"] = 1

    return ray_remote_args


def _splitrange(n, k):
    """Calculates array lens of np.array_split().

    This is the equivalent of
    `[len(x) for x in np.array_split(range(n), k)]`.
    """
    base = n // k
    output = [base] * k
    rem = n - sum(output)
    for i in range(len(output)):
        if rem > 0:
            output[i] += 1
            rem -= 1
    assert rem == 0, (rem, output, n, k)
    assert sum(output) == n, (output, n, k)
    return output


def _split_blocks(blocks: Iterable[Block], split_factor: float) -> Iterable[Block]:
    for block in blocks:
        block = BlockAccessor.for_block(block)
        offset = 0
        split_sizes = _splitrange(block.num_rows(), split_factor)
        for size in split_sizes:
            if size <= 0:
                continue
            yield block.slice(offset, offset + size, copy=False)
            offset += size


def _wrap_transformer_with_limit(
    map_transformer: MapTransformer, per_block_limit: int
) -> MapTransformer:
    """Wrap a MapTransformer with per-block limit functionality."""

    # Create a new limit transform function that goes at the end
    limit_transform_fn = _create_per_block_limit_transform_fn(per_block_limit)

    # Add the limit transform as the last step
    # Appending at the end so that the cap applies to the final output
    # blocks after all prior transforms.
    existing_transform_fns = map_transformer.get_transform_fns()
    new_transform_fns = existing_transform_fns + [limit_transform_fn]

    # Create new transformer with the limit added
    # TODO: Modify `add_transform_fns` to do this operation internally instead of modifying in place.
    new_transformer = MapTransformer(
        new_transform_fns,
        init_fn=map_transformer._init_fn,
        output_block_size_option_override=map_transformer._output_block_size_option_override,
    )

    return new_transformer


def _per_block_limit_fn(
    input: Iterable[Block], ctx: TaskContext, per_block_limit: int
) -> Iterable[Block]:
    """Apply per-block limit to the input blocks."""
    from ray.data.block import BlockAccessor

    # This is used to track the number of rows processed within this task.
    processed_rows = 0

    for block in input:
        if processed_rows >= per_block_limit:
            # We've hit the limit, stop processing
            break

        block_accessor = BlockAccessor.for_block(block)
        block_rows = block_accessor.num_rows()

        if processed_rows + block_rows <= per_block_limit:
            # Entire block fits within limit
            processed_rows += block_rows
            yield block
        else:
            # Need to truncate this block
            remaining_rows = per_block_limit - processed_rows
            truncated_block = block_accessor.slice(0, remaining_rows, copy=False)
            processed_rows += remaining_rows
            yield truncated_block


def _create_per_block_limit_transform_fn(per_block_limit: int) -> BlockMapTransformFn:
    """Create a transform function that applies per-block row limits."""
    limit_fn = functools.partial(_per_block_limit_fn, per_block_limit=per_block_limit)
    return BlockMapTransformFn(limit_fn)
