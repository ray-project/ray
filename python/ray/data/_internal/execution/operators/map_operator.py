import copy
import functools
import itertools
import logging
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

if TYPE_CHECKING:
    import pyarrow as pa

import ray
from ray import ObjectRef
from ray._raylet import ObjectRefGenerator
from ray.data._internal.compute import (
    ActorPoolStrategy,
    ComputeStrategy,
    TaskPoolStrategy,
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
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.interfaces.ref_bundle import (
    _iter_sliced_blocks,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
    OneToOneOperator,
)
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.util import memory_string
from ray.data._internal.stats import StatsDict
from ray.data._internal.util import MemoryProfiler
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    BlockStats,
    _take_first_non_empty_schema,
    to_stats,
)
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)


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


def _get_schema_from_bundle(bundle: RefBundle) -> Optional["pa.Schema"]:
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
        block_ref, _ = bundle.blocks[0]
        schema_ref = _get_arrow_schema_from_block.remote(block_ref)
        return ray.get(schema_ref)

    return None


class BaseRefBundler(ABC):
    """Interface for the rebundling behavior of the MapOperator."""

    @abstractmethod
    def num_blocks(self) -> int:
        """Return the total number of blocks buffered inside the bundler."""
        pass

    @abstractmethod
    def add_bundle(self, bundle: RefBundle):
        """Add a new input bundle to the bundler."""
        pass

    @abstractmethod
    def has_bundle(self) -> bool:
        """Return whether the bundler currently holds a full bundle ready to emit."""
        pass

    @abstractmethod
    def size_bytes(self) -> int:
        """Estimate the total size in bytes of buffered bundles."""
        pass

    @abstractmethod
    def get_next_bundle(
        self,
    ) -> Tuple[List[RefBundle], RefBundle]:
        """Pop and return the next bundled input ready for task submission."""
        pass

    @abstractmethod
    def done_adding_bundles(self):
        """Signal that no additional bundles will be added to the bundler so the bundler can be finalized."""
        pass


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
        ref_bundler: Optional[BaseRefBundler],
        supports_fusion: bool,
        map_task_kwargs: Optional[Dict[str, Any]],
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]],
        ray_remote_args: Optional[Dict[str, Any]],
        on_start: Optional[Callable[[Optional["pa.Schema"]], None]] = None,
    ):
        # NOTE: This constructor should not be called directly; use MapOperator.create()
        # instead.
        # NOTE: This constructor must be called by subclasses.
        if map_task_kwargs is None:
            map_task_kwargs = {}

        self._map_transformer = map_transformer
        self._supports_fusion = supports_fusion
        self._map_task_kwargs = map_task_kwargs
        self._ray_remote_args = _canonicalize_ray_remote_args(ray_remote_args or {})
        self._ray_remote_args_fn = ray_remote_args_fn
        self._ray_remote_args_factory_actor_locality = None
        self._remote_args_for_metrics = copy.deepcopy(self._ray_remote_args)

        # Bundles block references up to the min_rows_per_bundle target.
        self._block_ref_bundler = ref_bundler or BlockRefBundler(min_rows_per_bundle)

        # Queue for task outputs, either ordered or unordered (this is set by start()).
        self._output_queue: Optional[_OutputQueue] = None
        # Output metadata, added to on get_next().
        self._output_blocks_stats: List[BlockStats] = []
        # All active `DataOpTask`s.
        self._data_tasks: Dict[int, DataOpTask] = {}
        self._next_data_task_idx = 0
        # All active `MetadataOpTask`s.
        self._metadata_tasks: Dict[int, MetadataOpTask] = {}
        self._next_metadata_task_idx = 0
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
        # _map_transformer_ref is lazily initialized on first access.
        # This ensures on_start callback (if registered) can modify the transformer
        # before serialization (e.g., for Iceberg schema evolution).
        self.__map_transformer_ref = None

    @property
    def _map_transformer_ref(self):
        """Lazily serialize _map_transformer to object store on first access.

        Deferred until first task submission so that on_start callbacks
        (e.g., on_write_start for Iceberg) can modify the transformer state
        before serialization.
        """
        if self.__map_transformer_ref is None:
            self.__map_transformer_ref = ray.put(self._map_transformer)
            self._warn_large_udf()
        return self.__map_transformer_ref

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
            schema = _get_schema_from_bundle(bundled_input)
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

    def internal_input_queue_num_blocks(self) -> int:
        return self._block_ref_bundler.num_blocks()

    def internal_input_queue_num_bytes(self) -> int:
        return self._block_ref_bundler.size_bytes()

    def internal_output_queue_num_blocks(self) -> int:
        return self._output_queue.num_blocks()

    def internal_output_queue_num_bytes(self) -> int:
        return self._output_queue.size_bytes()

    def clear_internal_input_queue(self) -> None:
        """Clear internal input queue (block ref bundler)."""
        self._block_ref_bundler.done_adding_bundles()
        while self._block_ref_bundler.has_bundle():
            (input_bundles, _) = self._block_ref_bundler.get_next_bundle()
            for input_bundle in input_bundles:
                self._metrics.on_input_dequeued(input_bundle)

    def clear_internal_output_queue(self) -> None:
        """Clear internal output queue."""
        while self._output_queue.has_next():
            bundle = self._output_queue.get_next()
            self._metrics.on_output_dequeued(bundle)

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
        ref_bundler: Optional[BaseRefBundler] = None,
        supports_fusion: bool = True,
        map_task_kwargs: Optional[Dict[str, Any]] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        per_block_limit: Optional[int] = None,
        on_start: Optional[Callable[[Optional["pa.Schema"]], None]] = None,
    ) -> "MapOperator":
        """Create a MapOperator.

        This factory creates the MapOperator pool implementation that corresponds to the
        compute argument:
            - If None or TaskPoolStrategy -> TaskPoolMapOperator
            - If ActorPoolStrategy -> ActorPoolMapOperator

        Args:
            transform_fn: The function to apply to each ref bundle input.
            input_op: Operator generating input data for this op.
            init_fn: The callable class to instantiate if using ActorPoolMapOperator.
            name: The name of this operator.
            compute_strategy: Customize the compute strategy for this op.
            target_max_block_size_override: Override for target max-block-size.
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
            from ray.data._internal.execution.operators.task_pool_map_operator import (
                TaskPoolMapOperator,
            )

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
            )
        elif isinstance(compute_strategy, ActorPoolStrategy):
            from ray.data._internal.execution.operators.actor_pool_map_operator import (
                ActorPoolMapOperator,
            )

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
            )
        else:
            raise ValueError(f"Unsupported execution strategy {compute_strategy}")

    def start(self, options: "ExecutionOptions"):
        super().start(options)
        # Create output queue with desired ordering semantics.
        if options.preserve_order:
            self._output_queue = _OrderedOutputQueue()
        else:
            self._output_queue = _UnorderedOutputQueue()

        if options.locality_with_output:
            if isinstance(options.locality_with_output, list):
                locs = options.locality_with_output
            else:
                locs = [ray.get_runtime_context().get_node_id()]

            class RoundRobinAssign:
                def __init__(self, locs):
                    self.locs = locs
                    self.i = 0

                def __call__(self, args):
                    args = copy.deepcopy(args)
                    args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                        self.locs[self.i],
                        soft=True,
                        _spill_on_unavailable=True,
                    )
                    self.i += 1
                    self.i %= len(self.locs)
                    return args

            self._ray_remote_args_factory_actor_locality = RoundRobinAssign(locs)

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

    def _warn_large_udf(self):
        """Print a warning if the UDF is too large."""
        udf_size = ray.experimental.get_local_object_locations(
            [self.__map_transformer_ref]
        )[self.__map_transformer_ref]["object_size"]
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

        # Add RefBundle to the bundler.
        self._block_ref_bundler.add_bundle(refs)
        self._metrics.on_input_queued(refs)

        if self._block_ref_bundler.has_bundle():
            # The ref bundler combines one or more RefBundles into a new larger
            # RefBundle. Rather than dequeuing the new RefBundle, which was never
            # enqueued in the first place, we dequeue the original RefBundles.
            (input_refs, bundled_input) = self._block_ref_bundler.get_next_bundle()
            for bundle in input_refs:
                self._metrics.on_input_dequeued(bundle)

            # If the bundler has a full bundle, add it to the operator's task submission
            # queue
            self._add_bundled_input(bundled_input)

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
        # This should take precedence over previously set scheduling strategy, as it
        # implements actor-based locality overrides.
        if self._ray_remote_args_factory_actor_locality:
            return self._ray_remote_args_factory_actor_locality(ray_remote_args)
        return ray_remote_args

    @abstractmethod
    def _add_bundled_input(self, refs: RefBundle):
        """Add a pre-bundled upstream output to this operator.

        Unlike the add_input() arg, this RefBundle has already been further bundled by
        _block_ref_bundler up to the target size, meaning that this bundle is ready for
        task submission.

        This must be implemented by subclasses.

        Args:
            refs: The fully-bundled ref bundle that should be added as input.
        """
        raise NotImplementedError

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
        self._metrics.on_task_submitted(task_index, inputs)

        def _output_ready_callback(
            task_index,
            output: RefBundle,
        ):
            # Since output is streamed, it should only contain one block.
            assert len(output) == 1
            self._metrics.on_task_output_generated(task_index, output)

            # Notify output queue that the task has produced an new output.
            self._output_queue.notify_task_output_ready(task_index, output)
            self._metrics.on_output_queued(output)

        def _task_done_callback(task_index: int, exception: Optional[Exception]):
            self._metrics.on_task_finished(task_index, exception)

            # Estimate number of tasks and rows from inputs received and tasks
            # submitted so far
            (
                _,
                self._estimated_num_output_bundles,
                self._estimated_output_num_rows,
            ) = estimate_total_num_of_blocks(
                self._next_data_task_idx, self.upstream_op_num_outputs(), self._metrics
            )

            self._data_tasks.pop(task_index)
            # Notify output queue that this task is complete.
            self._output_queue.notify_task_completed(task_index)
            if task_done_callback:
                task_done_callback()

        self._data_tasks[task_index] = DataOpTask(
            task_index,
            gen,
            lambda output: _output_ready_callback(task_index, output),
            functools.partial(_task_done_callback, task_index),
        )

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
        self._block_ref_bundler.done_adding_bundles()
        if self._block_ref_bundler.has_bundle():
            # Handle any leftover bundles in the bundler.
            (
                _,
                bundled_input,
            ) = self._block_ref_bundler.get_next_bundle()

            self._add_bundled_input(bundled_input)

        super().all_inputs_done()

    def has_next(self) -> bool:
        assert self._started
        return self._output_queue.has_next()

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
    def current_processor_usage(self) -> ExecutionResources:
        raise NotImplementedError

    @abstractmethod
    def pending_processor_usage(self) -> ExecutionResources:
        raise NotImplementedError

    @abstractmethod
    def incremental_resource_usage(self) -> ExecutionResources:
        raise NotImplementedError

    def implements_accurate_memory_accounting(self) -> bool:
        return True

    def supports_fusion(self) -> bool:
        return self._supports_fusion

    def num_active_tasks(self) -> int:
        # Override `num_active_tasks` to only include data tasks and exclude
        # metadata tasks, which are used by the actor-pool map operator to
        # check if a newly created actor is ready.
        # The reasons are because:
        # 1. `PhysicalOperator.completed` checks `num_active_tasks`. The operator
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
        fn: The callable that takes Iterator[Block] as input and returns
            Iterator[Block] as output.
        blocks: The concrete block values from the task ref bundle.
        slices: List of block slices for this task to process.

    Returns:
        A generator of blocks, followed by the list of BlockMetadata for the blocks
        as the last generator return.
    """
    logger.debug(
        "Executing map task of operator %s with task index %d",
        ctx.op_name,
        ctx.task_idx,
    )
    DataContext._set_current(data_context)
    ctx.kwargs.update(kwargs)
    TaskContext.set_current(ctx)
    stats = BlockExecStats.builder()
    map_transformer.override_target_max_block_size(ctx.target_max_block_size_override)
    block_iter: Iterable[Block]
    if slices:
        block_iter = _iter_sliced_blocks(blocks, slices)
    else:
        block_iter = iter(blocks)

    with MemoryProfiler(data_context.memory_usage_poll_interval_s) as profiler:
        for b_out in map_transformer.apply_transform(block_iter, ctx):
            # TODO(Clark): Add input file propagation from input blocks.
            m_out = BlockAccessor.for_block(b_out).get_metadata()
            s_out = BlockAccessor.for_block(b_out).schema()
            m_out.exec_stats = stats.build()
            m_out.exec_stats.udf_time_s = map_transformer.udf_time()
            m_out.exec_stats.task_idx = ctx.task_idx
            m_out.exec_stats.max_uss_bytes = profiler.estimate_max_uss()
            meta_with_schema = BlockMetadataWithSchema(metadata=m_out, schema=s_out)
            yield b_out
            yield meta_with_schema
            stats = BlockExecStats.builder()
            profiler.reset()

    TaskContext.reset_current()


class BlockRefBundler(BaseRefBundler):
    """Rebundles RefBundles to get them close to a particular number of rows."""

    def __init__(self, min_rows_per_bundle: Optional[int]):
        """Creates a BlockRefBundler.

        Args:
            min_rows_per_bundle: The target number of rows per bundle. Note that we
                bundle up to this target, but only exceed it if not doing so would
                result in an empty bundle.
        """
        assert (
            min_rows_per_bundle is None or min_rows_per_bundle >= 0
        ), "Min rows per bundle has to be non-negative"

        self._min_rows_per_bundle = min_rows_per_bundle
        self._bundle_buffer: List[RefBundle] = []
        self._bundle_buffer_size = 0
        self._bundle_buffer_size_bytes = 0
        self._finalized = False

    def num_blocks(self):
        return sum(len(b.block_refs) for b in self._bundle_buffer)

    def add_bundle(self, bundle: RefBundle):
        """Add a bundle to the bundler."""
        self._bundle_buffer.append(bundle)
        self._bundle_buffer_size += self._get_bundle_size(bundle)
        self._bundle_buffer_size_bytes += bundle.size_bytes()

    def has_bundle(self) -> bool:
        """Returns whether the bundler has a bundle."""
        return self._bundle_buffer and (
            self._min_rows_per_bundle is None
            or self._bundle_buffer_size >= self._min_rows_per_bundle
            or (self._finalized and self._bundle_buffer_size >= 0)
        )

    def size_bytes(self) -> int:
        return self._bundle_buffer_size_bytes

    def get_next_bundle(
        self,
    ) -> Tuple[List[RefBundle], RefBundle]:
        """Gets the next bundle.

        Returns:
            A two-tuple. The first element is a list of bundles that were combined into
            the output bundle. The second element is the output bundle.
        """
        assert self.has_bundle()

        if self._min_rows_per_bundle is None:
            # Short-circuit if no bundle row target was defined.
            assert len(self._bundle_buffer) == 1
            bundle = self._bundle_buffer[0]
            self._bundle_buffer = []
            self._bundle_buffer_size = 0
            self._bundle_buffer_size_bytes = 0
            return [bundle], bundle

        remainder = []
        output_buffer = []
        output_buffer_size = 0

        for idx, bundle in enumerate(self._bundle_buffer):
            bundle_size = self._get_bundle_size(bundle)

            # Add bundle to the output buffer so long as either
            #   - Output buffer size is still 0
            #   - Output buffer doesn't exceeds the `_min_rows_per_bundle` threshold
            if (
                output_buffer_size < self._min_rows_per_bundle
                or output_buffer_size == 0
            ):
                output_buffer.append(bundle)
                output_buffer_size += bundle_size
            else:
                remainder = self._bundle_buffer[idx:]

        self._bundle_buffer = remainder
        self._bundle_buffer_size = sum(
            self._get_bundle_size(bundle) for bundle in remainder
        )
        self._bundle_buffer_size_bytes = sum(
            bundle.size_bytes() for bundle in remainder
        )

        return list(output_buffer), _merge_ref_bundles(*output_buffer)

    def done_adding_bundles(self):
        """Indicate that no more RefBundles will be added to this bundler."""
        self._finalized = True

    @staticmethod
    def _get_bundle_size(bundle: RefBundle):
        return bundle.num_rows() if bundle.num_rows() is not None else float("inf")


def _merge_ref_bundles(*bundles: RefBundle) -> RefBundle:
    """Merge N ref bundles into a single bundle of multiple blocks."""
    # Check that at least one bundle is non-null.
    bundles = [bundle for bundle in bundles if bundle is not None]
    assert len(bundles) > 0
    blocks = list(
        itertools.chain(block for bundle in bundles for block in bundle.blocks)
    )
    owns_blocks = all(bundle.owns_blocks for bundle in bundles)
    schema = _take_first_non_empty_schema(bundle.schema for bundle in bundles)
    return RefBundle(blocks, owns_blocks=owns_blocks, schema=schema)


class _OutputQueue(ABC):
    """Interface for swapping between different output order modes."""

    @abstractmethod
    def notify_task_output_ready(self, task_index: int, output: RefBundle):
        """Called when a task's output is ready."""
        pass

    def notify_task_completed(self, task_index: int):
        """Called when a previously pending task completes."""
        pass

    @abstractmethod
    def has_next(self) -> bool:
        pass

    @abstractmethod
    def get_next(self) -> RefBundle:
        pass

    @abstractmethod
    def num_blocks(self) -> int:
        pass

    @abstractmethod
    def size_bytes(self) -> int:
        pass


class _OrderedOutputQueue(_OutputQueue):
    """An queue that returns finished tasks in submission order."""

    def __init__(self):
        self._task_outputs: Dict[int, Deque[RefBundle]] = defaultdict(lambda: deque())
        self._current_output_index: int = 0
        self._completed_tasks: Set[int] = set()
        self._size_bytes: int = 0
        self._num_blocks: int = 0

    def notify_task_output_ready(self, task_index: int, output: RefBundle):
        self._task_outputs[task_index].append(output)
        self._size_bytes += output.size_bytes()
        self._num_blocks += len(output.blocks)

    def _move_to_next_task(self):
        """Move the outut index to the next task.

        This method should only be called when the current task is complete and all
        outputs have been taken.
        """
        assert len(self._task_outputs[self._current_output_index]) == 0
        assert self._current_output_index in self._completed_tasks
        del self._task_outputs[self._current_output_index]
        self._completed_tasks.remove(self._current_output_index)
        self._current_output_index += 1

    def notify_task_completed(self, task_index: int):
        assert task_index >= self._current_output_index
        self._completed_tasks.add(task_index)
        if task_index == self._current_output_index:
            if len(self._task_outputs[task_index]) == 0:
                self._move_to_next_task()

    def has_next(self) -> bool:
        return len(self._task_outputs[self._current_output_index]) > 0

    def get_next(self) -> RefBundle:
        next_bundle = self._task_outputs[self._current_output_index].popleft()
        self._size_bytes -= next_bundle.size_bytes()
        self._num_blocks -= len(next_bundle.blocks)
        if len(self._task_outputs[self._current_output_index]) == 0:
            if self._current_output_index in self._completed_tasks:
                self._move_to_next_task()
        return next_bundle

    def num_blocks(self) -> int:
        return self._num_blocks

    def size_bytes(self) -> int:
        return self._size_bytes


class _UnorderedOutputQueue(_OutputQueue):
    """An queue that does not guarantee output order of finished tasks."""

    def __init__(self):
        self._queue: Deque[RefBundle] = deque()
        self._num_blocks: int = 0
        self._size_bytes: int = 0

    def notify_task_output_ready(self, _: int, output: RefBundle):
        self._queue.append(output)
        self._num_blocks += len(output.blocks)
        self._size_bytes += output.size_bytes()

    def has_next(self) -> bool:
        return len(self._queue) > 0

    def get_next(self) -> RefBundle:
        next_bundle = self._queue.popleft()
        self._num_blocks -= len(next_bundle.blocks)
        self._size_bytes -= next_bundle.size_bytes()
        return next_bundle

    def num_blocks(self) -> int:
        return self._num_blocks

    def size_bytes(self) -> int:
        return self._size_bytes


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
