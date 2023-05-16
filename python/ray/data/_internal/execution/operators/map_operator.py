from abc import ABC, abstractmethod
import copy
from dataclasses import dataclass
import itertools
from typing import Callable, List, Iterator, Any, Dict, Optional, Union

import ray
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockMetadata,
    BlockExecStats,
)
from ray.data._internal.compute import (
    ComputeStrategy,
    TaskPoolStrategy,
    ActorPoolStrategy,
)
from ray.data._internal.execution.interfaces import (
    RefBundle,
    ExecutionOptions,
    ExecutionResources,
    PhysicalOperator,
    TaskContext,
    MapTransformFn,
)
from ray.data._internal.memory_tracing import trace_allocation
from ray.data._internal.stats import StatsDict
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.types import ObjectRef
from ray._raylet import ObjectRefGenerator


class MapOperator(PhysicalOperator, ABC):
    """A streaming operator that maps input bundles 1:1 to output bundles.

    This operator implements the distributed map operation, supporting both task
    and actor compute strategies.
    """

    def __init__(
        self,
        transform_fn: MapTransformFn,
        input_op: PhysicalOperator,
        name: str,
        min_rows_per_bundle: Optional[int],
        ray_remote_args: Optional[Dict[str, Any]],
    ):
        # NOTE: This constructor should not be called directly; use MapOperator.create()
        # instead.
        # NOTE: This constructor must be called by subclasses.

        self._transform_fn = transform_fn
        self._ray_remote_args = _canonicalize_ray_remote_args(ray_remote_args or {})
        self._ray_remote_args_factory = None

        # Bundles block references up to the min_rows_per_bundle target.
        self._block_ref_bundler = _BlockRefBundler(min_rows_per_bundle)
        # Object store allocation stats.
        self._metrics = _ObjectStoreMetrics(alloc=0, freed=0, cur=0, peak=0)

        # Queue for task outputs, either ordered or unordered (this is set by start()).
        self._output_queue: _OutputQueue = None
        # Output metadata, added to on get_next().
        self._output_metadata: List[BlockMetadata] = []

        super().__init__(name, [input_op])

    @classmethod
    def create(
        cls,
        transform_fn: MapTransformFn,
        input_op: PhysicalOperator,
        init_fn: Optional[Callable[[], None]] = None,
        name: str = "Map",
        # TODO(ekl): slim down ComputeStrategy to only specify the compute
        # config and not contain implementation code.
        compute_strategy: Optional[ComputeStrategy] = None,
        min_rows_per_bundle: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
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
            min_rows_per_bundle: The number of rows to gather per batch passed to the
                transform_fn, or None to use the block size. Setting the batch size is
                important for the performance of GPU-accelerated transform functions.
                The actual rows passed may be less if the dataset is small.
            ray_remote_args: Customize the ray remote args for this op's tasks.
        """
        if compute_strategy is None:
            compute_strategy = TaskPoolStrategy()

        if isinstance(compute_strategy, TaskPoolStrategy):
            from ray.data._internal.execution.operators.task_pool_map_operator import (
                TaskPoolMapOperator,
            )

            return TaskPoolMapOperator(
                transform_fn,
                input_op,
                name=name,
                min_rows_per_bundle=min_rows_per_bundle,
                ray_remote_args=ray_remote_args,
            )
        elif isinstance(compute_strategy, ActorPoolStrategy):
            from ray.data._internal.execution.operators.actor_pool_map_operator import (
                ActorPoolMapOperator,
                AutoscalingConfig,
                AutoscalingPolicy,
            )

            autoscaling_config = AutoscalingConfig.from_compute_strategy(
                compute_strategy
            )
            autoscaling_policy = AutoscalingPolicy(autoscaling_config)

            if init_fn is None:

                def init_fn():
                    pass

            return ActorPoolMapOperator(
                transform_fn,
                init_fn,
                input_op,
                autoscaling_policy=autoscaling_policy,
                name=name,
                min_rows_per_bundle=min_rows_per_bundle,
                ray_remote_args=ray_remote_args,
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

            self._ray_remote_args_factory = RoundRobinAssign(locs)

        # Put the function def in the object store to avoid repeated serialization
        # in case it's large (i.e., closure captures large objects).
        self._transform_fn_ref = ray.put(self._transform_fn)

    def add_input(self, refs: RefBundle, input_index: int):
        assert input_index == 0, input_index
        # Add ref bundle allocation to operator's object store metrics.
        self._metrics.cur += refs.size_bytes()
        if self._metrics.cur > self._metrics.peak:
            self._metrics.peak = self._metrics.cur
        # Add RefBundle to the bundler.
        self._block_ref_bundler.add_bundle(refs)
        if self._block_ref_bundler.has_bundle():
            # If the bundler has a full bundle, add it to the operator's task submission
            # queue.
            bundle = self._block_ref_bundler.get_next_bundle()
            self._add_bundled_input(bundle)

    def _get_runtime_ray_remote_args(self) -> Dict[str, Any]:
        if self._ray_remote_args_factory:
            return self._ray_remote_args_factory(self._ray_remote_args)
        return self._ray_remote_args

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

    def _handle_task_submitted(self, task: "_TaskState"):
        """Handle a newly submitted task, notifying the output queue and updating
        object store metrics.

        This should be called by subclasses right after a task is submitted.

        Args:
            task: The task state for the newly submitted task.
        """
        # Notify output queue that this task is pending.
        self._output_queue.notify_pending_task(task)

    @abstractmethod
    def notify_work_completed(
        self, ref: Union[ObjectRef[ObjectRefGenerator], ray.ObjectRef]
    ):
        """Indicates that a task is done executing OR that a worker is done starting.

        This must be implemented by subclasses.

        Args:
            ref: The output ref for the task that's done or the worker that has
                been started.
        """
        raise NotImplementedError

    def _handle_task_done(self, task: "_TaskState"):
        """Handle a newly completed task, notifying the output queue, freeing task
        inputs, and updating object store metrics.

        This should be called by subclasses right after a task completes.

        Args:
            task: The task state for the newly completed task.
        """
        # Notify output queue that this task is complete.
        self._output_queue.notify_task_completed(task)
        task.inputs.destroy_if_owned()
        # Update object store metrics.
        allocated = task.output.size_bytes()
        self._metrics.alloc += allocated
        self._metrics.cur += allocated
        freed = task.inputs.size_bytes()
        self._metrics.freed += freed
        self._metrics.cur -= freed
        if self._metrics.cur > self._metrics.peak:
            self._metrics.peak = self._metrics.cur

    def inputs_done(self):
        self._block_ref_bundler.done_adding_bundles()
        if self._block_ref_bundler.has_bundle():
            # Handle any leftover bundles in the bundler.
            bundle = self._block_ref_bundler.get_next_bundle()
            self._add_bundled_input(bundle)
        super().inputs_done()

    def has_next(self) -> bool:
        assert self._started
        return self._output_queue.has_next()

    def get_next(self) -> RefBundle:
        assert self._started
        bundle = self._output_queue.get_next()
        self._metrics.cur -= bundle.size_bytes()
        for _, meta in bundle.blocks:
            self._output_metadata.append(meta)
        return bundle

    @abstractmethod
    def get_work_refs(
        self,
    ) -> List[Union[ObjectRef[ObjectRefGenerator], ray.ObjectRef]]:
        raise NotImplementedError

    @abstractmethod
    def num_active_work_refs(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def progress_str(self) -> str:
        raise NotImplementedError

    def get_metrics(self) -> Dict[str, int]:
        return self._metrics.to_metrics_dict()

    def get_stats(self) -> StatsDict:
        return {self._name: self._output_metadata}

    def get_transformation_fn(self) -> MapTransformFn:
        return self._transform_fn

    @abstractmethod
    def shutdown(self):
        # NOTE: This must be implemented by subclasses, and those overriding methods
        # must call this method.
        super().shutdown()

    @abstractmethod
    def current_resource_usage(self) -> ExecutionResources:
        raise NotImplementedError

    @abstractmethod
    def base_resource_usage(self) -> ExecutionResources:
        raise NotImplementedError

    @abstractmethod
    def incremental_resource_usage(self) -> ExecutionResources:
        raise NotImplementedError

    @staticmethod
    def _map_ref_to_ref_bundle(ref: ObjectRef[ObjectRefGenerator]) -> RefBundle:
        """Utility for converting a generator ref to a RefBundle.

        This function blocks on the completion of the underlying generator task via
        ray.get().
        """
        all_refs = list(ray.get(ref))
        del ref
        block_refs = all_refs[:-1]
        block_metas = ray.get(all_refs[-1])
        assert len(block_metas) == len(block_refs), (block_refs, block_metas)
        for ref in block_refs:
            trace_allocation(ref, "map_operator_work_completed")
        return RefBundle(list(zip(block_refs, block_metas)), owns_blocks=True)


@dataclass
class _TaskState:
    """Tracks the driver-side state for an MapOperator task.

    Attributes:
        inputs: The input ref bundle.
        output: The output ref bundle that is set when the task completes.
    """

    inputs: RefBundle
    output: Optional[RefBundle] = None


@dataclass
class _ObjectStoreMetrics:
    """Metrics for object store memory allocations."""

    alloc: int
    freed: int
    cur: int
    peak: int

    def to_metrics_dict(self) -> Dict[str, int]:
        return {
            "obj_store_mem_alloc": self.alloc,
            "obj_store_mem_freed": self.freed,
            "obj_store_mem_peak": self.peak,
        }


def _map_task(
    fn: MapTransformFn,
    ctx: TaskContext,
    *blocks: Block,
) -> Iterator[Union[Block, List[BlockMetadata]]]:
    """Remote function for a single operator task.

    Args:
        fn: The callable that takes Iterator[Block] as input and returns
            Iterator[Block] as output.
        blocks: The concrete block values from the task ref bundle.

    Returns:
        A generator of blocks, followed by the list of BlockMetadata for the blocks
        as the last generator return.
    """
    output_metadata = []
    stats = BlockExecStats.builder()
    for b_out in fn(iter(blocks), ctx):
        # TODO(Clark): Add input file propagation from input blocks.
        m_out = BlockAccessor.for_block(b_out).get_metadata([], None)
        m_out.exec_stats = stats.build()
        output_metadata.append(m_out)
        yield b_out
        stats = BlockExecStats.builder()
    yield output_metadata


class _BlockRefBundler:
    """Rebundles RefBundles to get them close to a particular number of rows."""

    def __init__(self, min_rows_per_bundle: Optional[int]):
        """Creates a BlockRefBundler.

        Args:
            min_rows_per_bundle: The target number of rows per bundle. Note that we
                bundle up to this target, but only exceed it if not doing so would
                result in an empty bundle.
        """
        self._min_rows_per_bundle = min_rows_per_bundle
        self._bundle_buffer: List[RefBundle] = []
        self._bundle_buffer_size = 0
        self._finalized = False

    def add_bundle(self, bundle: RefBundle):
        """Add a bundle to the bundler."""
        self._bundle_buffer.append(bundle)
        self._bundle_buffer_size += self._get_bundle_size(bundle)

    def has_bundle(self) -> bool:
        """Returns whether the bundler has a bundle."""
        return self._bundle_buffer and (
            self._min_rows_per_bundle is None
            or self._bundle_buffer_size >= self._min_rows_per_bundle
            or (self._finalized and self._bundle_buffer_size > 0)
        )

    def get_next_bundle(self) -> RefBundle:
        """Gets the next bundle."""
        assert self.has_bundle()
        if self._min_rows_per_bundle is None:
            # Short-circuit if no bundle row target was defined.
            assert len(self._bundle_buffer) == 1
            bundle = self._bundle_buffer[0]
            self._bundle_buffer = []
            self._bundle_buffer_size = 0
            return bundle
        leftover = []
        output_buffer = []
        output_buffer_size = 0
        buffer_filled = False
        for bundle in self._bundle_buffer:
            bundle_size = self._get_bundle_size(bundle)
            if buffer_filled:
                # Buffer has been filled, save it in the leftovers.
                leftover.append(bundle)
            elif (
                output_buffer_size + bundle_size <= self._min_rows_per_bundle
                or output_buffer_size == 0
            ):
                # Bundle fits in buffer, or bundle doesn't fit but the buffer still
                # needs a non-empty bundle.
                output_buffer.append(bundle)
                output_buffer_size += bundle_size
            else:
                # Bundle doesn't fit in a buffer that already has at least one non-empty
                # bundle, so we add it to the leftovers.
                leftover.append(bundle)
                # Add all remaining bundles to the leftovers.
                buffer_filled = True
        self._bundle_buffer = leftover
        self._bundle_buffer_size = sum(
            self._get_bundle_size(bundle) for bundle in leftover
        )
        return _merge_ref_bundles(*output_buffer)

    def done_adding_bundles(self):
        """Indicate that no more RefBundles will be added to this bundler."""
        self._finalized = True

    @staticmethod
    def _get_bundle_size(bundle: RefBundle):
        return bundle.num_rows() if bundle.num_rows() is not None else float("inf")


def _merge_ref_bundles(*bundles: RefBundle) -> RefBundle:
    """Merge N ref bundles into a single bundle of multiple blocks."""
    # Check that at least one bundle is non-null.
    assert any(bundle is not None for bundle in bundles)
    blocks = list(
        itertools.chain(
            block for bundle in bundles if bundle is not None for block in bundle.blocks
        )
    )
    owns_blocks = all(bundle.owns_blocks for bundle in bundles if bundle is not None)
    return RefBundle(blocks, owns_blocks)


class _OutputQueue:
    """Interface for swapping between different output order modes."""

    def notify_pending_task(self, task: _TaskState):
        """Called when a new task becomes pending."""
        pass

    def notify_task_completed(self, task: _TaskState):
        """Called when a previously pending task completes."""
        pass

    def has_next(self) -> bool:
        raise NotImplementedError

    def get_next(self) -> RefBundle:
        raise NotImplementedError


class _OrderedOutputQueue(_OutputQueue):
    """An queue that returns finished tasks in submission order."""

    def __init__(self):
        self._tasks_by_output_order: Dict[int, _TaskState] = {}
        self._next_task_index: int = 0
        self._next_output_index: int = 0

    def notify_pending_task(self, task: _TaskState):
        self._tasks_by_output_order[self._next_task_index] = task
        self._next_task_index += 1

    def has_next(self) -> bool:
        i = self._next_output_index
        return (
            i in self._tasks_by_output_order
            and self._tasks_by_output_order[i].output is not None
        )

    def get_next(self) -> RefBundle:
        # Get the output RefBundle for the current task.
        out_bundle = self._tasks_by_output_order[self._next_output_index].output
        # Pop out the next single-block bundle.
        next_bundle = RefBundle(
            [out_bundle.blocks.pop(0)], owns_blocks=out_bundle.owns_blocks
        )
        if not out_bundle.blocks:
            # If this task's RefBundle is exhausted, move to the next one.
            del self._tasks_by_output_order[self._next_output_index]
            self._next_output_index += 1
        return next_bundle


class _UnorderedOutputQueue(_OutputQueue):
    """An queue that does not guarantee output order of finished tasks."""

    def __init__(self):
        self._completed_tasks: List[_TaskState] = []

    def notify_task_completed(self, task: _TaskState):
        self._completed_tasks.append(task)

    def has_next(self) -> bool:
        return len(self._completed_tasks) > 0

    def get_next(self) -> RefBundle:
        # Get the output RefBundle for the oldest completed task.
        out_bundle = self._completed_tasks[0].output
        # Pop out the next single-block bundle.
        next_bundle = RefBundle(
            [out_bundle.blocks.pop(0)], owns_blocks=out_bundle.owns_blocks
        )
        if not out_bundle.blocks:
            # If this task's RefBundle is exhausted, move to the next one.
            del self._completed_tasks[0]
        return next_bundle


def _canonicalize_ray_remote_args(ray_remote_args: Dict[str, Any]) -> Dict[str, Any]:
    """Enforce rules on ray remote args for map tasks.

    Namely, args must explicitly specify either CPU or GPU, not both. Disallowing
    mixed resources avoids potential starvation and deadlock issues during scheduling,
    and should not be a serious limitation for users.
    """
    ray_remote_args = ray_remote_args.copy()
    if "num_cpus" not in ray_remote_args and "num_gpus" not in ray_remote_args:
        ray_remote_args["num_cpus"] = 1
    if ray_remote_args.get("num_gpus", 0) > 0:
        if ray_remote_args.get("num_cpus", 0) != 0:
            raise ValueError(
                "It is not allowed to specify both num_cpus and num_gpus for map tasks."
            )
    elif ray_remote_args.get("num_cpus", 0) > 0:
        if ray_remote_args.get("num_gpus", 0) != 0:
            raise ValueError(
                "It is not allowed to specify both num_cpus and num_gpus for map tasks."
            )
    return ray_remote_args
