from dataclasses import dataclass, field
import os
from typing import Dict, List, Optional, Iterable, Iterator, Tuple, Callable, Union

import ray
from ray.util.annotations import DeveloperAPI
from ray.data._internal.execution.util import memory_string
from ray.data._internal.logical.interfaces import Operator
from ray.data._internal.memory_tracing import trace_deallocation
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatastreamStats, StatsDict
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef

# Node id string returned by `ray.get_runtime_context().get_node_id()`.
NodeIdStr = str


@dataclass
class RefBundle:
    """A group of data block references and their metadata.

    Operators take in and produce streams of RefBundles.

    Most commonly a RefBundle consists of a single block object reference.
    In some cases, e.g., due to block splitting, or for a reduce task, there may
    be more than one block.

    Block bundles have ownership semantics, i.e., shared ownership (similar to C++
    shared_ptr, multiple operators share the same block bundle), or unique ownership
    (similar to C++ unique_ptr, only one operator owns the block bundle). This
    allows operators to know whether they can destroy blocks when they don't need
    them. Destroying blocks eagerly is more efficient than waiting for Python GC /
    Ray reference counting to kick in.
    """

    # The size_bytes must be known in the metadata, num_rows is optional.
    blocks: List[Tuple[ObjectRef[Block], BlockMetadata]]

    # Whether we own the blocks (can safely destroy them).
    owns_blocks: bool

    # This attribute is used by the split() operator to assign bundles to logical
    # output splits. It is otherwise None.
    output_split_idx: Optional[int] = None

    # Cached location, used for get_cached_location().
    _cached_location: Optional[NodeIdStr] = None

    def __post_init__(self):
        for b in self.blocks:
            assert isinstance(b, tuple), b
            assert len(b) == 2, b
            assert isinstance(b[0], ray.ObjectRef), b
            assert isinstance(b[1], BlockMetadata), b
            if b[1].size_bytes is None:
                raise ValueError(
                    "The size in bytes of the block must be known: {}".format(b)
                )

    def num_rows(self) -> Optional[int]:
        """Number of rows present in this bundle, if known."""
        total = 0
        for b in self.blocks:
            if b[1].num_rows is None:
                return None
            else:
                total += b[1].num_rows
        return total

    def size_bytes(self) -> int:
        """Size of the blocks of this bundle in bytes."""
        return sum(b[1].size_bytes for b in self.blocks)

    def destroy_if_owned(self) -> int:
        """Clears the object store memory for these blocks if owned.

        Returns:
            The number of bytes freed.
        """
        should_free = self.owns_blocks and DataContext.get_current().eager_free
        for b in self.blocks:
            trace_deallocation(b[0], "RefBundle.destroy_if_owned", free=should_free)
        return self.size_bytes() if should_free else 0

    def get_cached_location(self) -> Optional[NodeIdStr]:
        """Return a location for this bundle's data, if possible.

        Caches the resolved location so multiple calls to this are efficient.
        """
        if self._cached_location is None:
            # Only consider the first block in the bundle for now. TODO(ekl) consider
            # taking into account other blocks.
            ref = self.blocks[0][0]
            # This call is pretty fast for owned objects (~5k/s), so we don't need to
            # batch it for now.
            locs = ray.experimental.get_object_locations([ref])
            nodes = locs[ref]["node_ids"]
            if nodes:
                self._cached_location = nodes[0]
            else:
                self._cached_location = ""
        if self._cached_location:
            return self._cached_location
        else:
            return None  # Return None if cached location is "".

    def __eq__(self, other) -> bool:
        return self is other

    def __hash__(self) -> int:
        return id(self)


@dataclass
class ExecutionResources:
    """Specifies resources usage or resource limits for execution.

    The value `None` represents unknown resource usage or an unspecified limit.
    """

    # CPU usage in cores (Ray logical CPU slots).
    cpu: Optional[float] = None

    # GPU usage in devices (Ray logical GPU slots).
    gpu: Optional[float] = None

    # Object store memory usage in bytes.
    object_store_memory: Optional[int] = None

    def object_store_memory_str(self) -> str:
        """Returns a human-readable string for the object store memory field."""
        if self.object_store_memory is None:
            return "None"
        else:
            return memory_string(self.object_store_memory)

    def add(self, other: "ExecutionResources") -> "ExecutionResources":
        """Adds execution resources.

        Returns:
            A new ExecutionResource object with summed resources.
        """
        total = ExecutionResources()
        if self.cpu is not None or other.cpu is not None:
            total.cpu = (self.cpu or 0.0) + (other.cpu or 0.0)
        if self.gpu is not None or other.gpu is not None:
            total.gpu = (self.gpu or 0.0) + (other.gpu or 0.0)
        if (
            self.object_store_memory is not None
            or other.object_store_memory is not None
        ):
            total.object_store_memory = (self.object_store_memory or 0.0) + (
                other.object_store_memory or 0.0
            )
        return total

    def satisfies_limit(self, limit: "ExecutionResources") -> bool:
        """Return if this resource struct meets the specified limits.

        Note that None for a field means no limit.
        """

        if self.cpu is not None and limit.cpu is not None and self.cpu > limit.cpu:
            return False
        if self.gpu is not None and limit.gpu is not None and self.gpu > limit.gpu:
            return False
        if (
            self.object_store_memory is not None
            and limit.object_store_memory is not None
            and self.object_store_memory > limit.object_store_memory
        ):
            return False
        return True

    def scale(self, f: float) -> "ExecutionResources":
        """Return copy with all set values scaled by `f`."""
        return ExecutionResources(
            cpu=self.cpu * f if self.cpu is not None else None,
            gpu=self.gpu * f if self.gpu is not None else None,
            object_store_memory=self.object_store_memory * f
            if self.object_store_memory is not None
            else None,
        )


@DeveloperAPI
@dataclass
class ExecutionOptions:
    """Common options for execution.

    Some options may not be supported on all executors (e.g., resource limits).

    Attributes:
        resource_limits: Set a soft limit on the resource usage during execution.
            This is not supported in bulk execution mode. Autodetected by default.
        locality_with_output: Set this to prefer running tasks on the same node as the
            output node (node driving the execution). It can also be set to a list of
            node ids to spread the outputs across those nodes. Off by default.
        preserve_order: Set this to preserve the ordering between blocks processed by
            operators under the streaming executor. The bulk executor always preserves
            order. Off by default.
        actor_locality_enabled: Whether to enable locality-aware task dispatch to
            actors (on by default). This applies to both ActorPoolStrategy map and
            streaming_split operations.
        verbose_progress: Whether to report progress individually per operator. By
            default, only AllToAll operators and global progress is reported. This
            option is useful for performance debugging. Off by default.
    """

    resource_limits: ExecutionResources = field(default_factory=ExecutionResources)

    locality_with_output: Union[bool, List[NodeIdStr]] = False

    preserve_order: bool = False

    actor_locality_enabled: bool = True

    verbose_progress: bool = bool(int(os.environ.get("RAY_DATA_VERBOSE_PROGRESS", "0")))


@dataclass
class TaskContext:
    """This describes the information of a task running block transform."""

    # The index of task. Each task has a unique task index within the same
    # operator.
    task_idx: int

    # The dictionary of sub progress bar to update. The key is name of sub progress
    # bar. Note this is only used on driver side.
    # TODO(chengsu): clean it up from TaskContext with new optimizer framework.
    sub_progress_bar_dict: Optional[Dict[str, ProgressBar]] = None


# Block transform function applied by task and actor pools in MapOperator.
MapTransformFn = Callable[[Iterable[Block], TaskContext], Iterable[Block]]

# Block transform function applied in AllToAllOperator.
AllToAllTransformFn = Callable[
    [List[RefBundle], TaskContext], Tuple[List[RefBundle], StatsDict]
]


class PhysicalOperator(Operator):
    """Abstract class for physical operators.

    An operator transforms one or more input streams of RefBundles into a single
    output stream of RefBundles.

    Physical operators are stateful and non-serializable; they live on the driver side
    of the Datastream only.

    Here's a simple example of implementing a basic "Map" operator:

        class MapOperator(PhysicalOperator):
            def __init__(self):
                self.active_tasks = []

            def add_input(self, refs, _):
                self.active_tasks.append(map_task.remote(refs))

            def has_next(self):
                ready, _ = ray.wait(self.active_tasks, timeout=0)
                return len(ready) > 0

            def get_next(self):
                ready, remaining = ray.wait(self.active_tasks, num_returns=1)
                self.active_tasks = remaining
                return ready[0]

    Note that the above operator fully supports both bulk and streaming execution,
    since `add_input` and `get_next` can be called in any order. In bulk execution,
    all inputs would be added up-front, but in streaming execution the calls could
    be interleaved.
    """

    def __init__(self, name: str, input_dependencies: List["PhysicalOperator"]):
        super().__init__(name, input_dependencies)
        for x in input_dependencies:
            assert isinstance(x, PhysicalOperator), x
        self._inputs_complete = not input_dependencies
        self._dependents_complete = False
        self._started = False

    def __reduce__(self):
        raise ValueError("Operator is not serializable.")

    def completed(self) -> bool:
        """Return True when this operator is completed.

        An operator is completed if any of the following conditions are met:
        - All upstream operators are completed and all outputs are taken.
        - All downstream operators are completed.
        """
        return (
            self._inputs_complete
            and len(self.get_work_refs()) == 0
            and not self.has_next()
        ) or self._dependents_complete

    def get_stats(self) -> StatsDict:
        """Return recorded execution stats for use with DatastreamStats."""
        raise NotImplementedError

    def get_metrics(self) -> Dict[str, int]:
        """Returns dict of metrics reported from this operator.

        These should be instant values that can be queried at any time, e.g.,
        obj_store_mem_allocated, obj_store_mem_freed.
        """
        return {}

    def get_transformation_fn(self) -> Callable:
        """Returns the underlying transformation function for this operator.

        This is used by the physical plan optimizer for e.g. operator fusion.
        """
        raise NotImplementedError

    def progress_str(self) -> str:
        """Return any extra status to be displayed in the operator progress bar.

        For example, `<N> actors` to show current number of actors in an actor pool.
        """
        return ""

    def num_outputs_total(self) -> Optional[int]:
        """Returns the total number of output bundles of this operator, if known.

        This is useful for reporting progress.
        """
        if len(self.input_dependencies) == 1:
            return self.input_dependencies[0].num_outputs_total()
        return None

    def start(self, options: ExecutionOptions) -> None:
        """Called by the executor when execution starts for an operator.

        Args:
            options: The global options used for the overall execution.
        """
        self._started = True

    def should_add_input(self) -> bool:
        """Return whether it is desirable to add input to this operator right now.

        Operators can customize the implementation of this method to apply additional
        backpressure (e.g., waiting for internal actors to be created).
        """
        return True

    def need_more_inputs(self) -> bool:
        """Return true if the operator still needs more inputs.

        Once this return false, it should never return true again.
        """
        return True

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        """Called when an upstream result is available.

        Inputs may be added in any order, and calls to `add_input` may be interleaved
        with calls to `get_next` / `has_next` to implement streaming execution.

        Args:
            refs: The ref bundle that should be added as input.
            input_index: The index identifying the input dependency producing the
                input. For most operators, this is always `0` since there is only
                one upstream input operator.
        """
        raise NotImplementedError

    def inputs_done(self) -> None:
        """Called when all upstream operators have completed().

        After this is called, the executor guarantees that no more inputs will be added
        via `add_input` for any input index.
        """
        self._inputs_complete = True

    def all_dependents_complete(self) -> None:
        """Called when all downstream operators have completed().

        After this is called, the operator is marked as completed.
        """
        self._dependents_complete = True

    def has_next(self) -> bool:
        """Returns when a downstream output is available.

        When this returns true, it is safe to call `get_next()`.
        """
        raise NotImplementedError

    def get_next(self) -> RefBundle:
        """Get the next downstream output.

        It is only allowed to call this if `has_next()` has returned True.
        """
        raise NotImplementedError

    def get_work_refs(self) -> List[ray.ObjectRef]:
        """Get a list of object references the executor should wait on.

        When a reference becomes ready, the executor must call
        `notify_work_completed(ref)` to tell this operator of the state change.
        """
        return []

    def throttling_disabled(self) -> bool:
        """Whether to disable resource throttling for this operator.

        This should return True for operators that only manipulate bundle metadata
        (e.g., the OutputSplitter operator). This hints to the execution engine that
        these operators should not be throttled based on resource usage.
        """
        return False

    def num_active_work_refs(self) -> int:
        """Return the number of active work refs.

        Subclasses can override this as a performance optimization.
        """
        return len(self.get_work_refs())

    def internal_queue_size(self) -> int:
        """If the operator has an internal input queue, return its size.

        This is used to report tasks pending submission to actor pools.
        """
        return 0

    def notify_work_completed(self, work_ref: ray.ObjectRef) -> None:
        """Executor calls this when the given work is completed and local.

        This must be called as soon as the operator is aware that `work_ref` is
        ready.
        """
        raise NotImplementedError

    def shutdown(self) -> None:
        """Abort execution and release all resources used by this operator.

        This release any Ray resources acquired by this operator such as active
        tasks, actors, and objects.
        """
        if not self._started:
            raise ValueError("Operator must be started before being shutdown.")

    def current_resource_usage(self) -> ExecutionResources:
        """Returns the current estimated resource usage of this operator.

        This method is called by the executor to decide how to allocate resources
        between different operators.
        """
        return ExecutionResources()

    def base_resource_usage(self) -> ExecutionResources:
        """Returns the minimum amount of resources required for execution.

        For example, an operator that creates an actor pool requiring 8 GPUs could
        return ExecutionResources(gpu=8) as its base usage.
        """
        return ExecutionResources()

    def incremental_resource_usage(self) -> ExecutionResources:
        """Returns the incremental resources required for processing another input.

        For example, an operator that launches a task per input could return
        ExecutionResources(cpu=1) as its incremental usage.
        """
        return ExecutionResources()

    def notify_resource_usage(
        self, input_queue_size: int, under_resource_limits: bool
    ) -> None:
        """Called periodically by the executor.

        Args:
            input_queue_size: The number of inputs queued outside this operator.
            under_resource_limits: Whether this operator is under resource limits.
        """
        pass


class OutputIterator(Iterator[RefBundle]):
    """Iterator used to access the output of an Executor execution.

    This is a blocking iterator. Datastreams guarantees that all its iterators are
    thread-safe (i.e., multiple threads can block on them at the same time).
    """

    def __init__(self, base: Iterable[RefBundle]):
        self._it = iter(base)

    def get_next(self, output_split_idx: Optional[int] = None) -> RefBundle:
        """Can be used to pull outputs by a specified output index.

        This is used to support the streaming_split() API, where the output of a
        streaming execution is to be consumed by multiple processes.

        Args:
            output_split_idx: The output split index to get results for. This arg is
                only allowed for iterators created by `Datastream.streaming_split()`.

        Raises:
            StopIteration if there are no more outputs to return.
        """
        if output_split_idx is not None:
            raise NotImplementedError()
        return next(self._it)

    def __next__(self) -> RefBundle:
        return self.get_next()


class Executor:
    """Abstract class for executors, which implement physical operator execution.

    Subclasses:
        BulkExecutor
        StreamingExecutor
    """

    def __init__(self, options: ExecutionOptions):
        """Create the executor."""
        self._options = options

    def execute(
        self, dag: PhysicalOperator, initial_stats: Optional[DatastreamStats] = None
    ) -> OutputIterator:
        """Start execution.

        Args:
            dag: The operator graph to execute.
            initial_stats: The DatastreamStats to prepend to the stats returned by the
                executor. These stats represent actions done to compute inputs.
        """
        raise NotImplementedError

    def shutdown(self):
        """Shutdown an executor, which may still be running.

        This should interrupt execution and clean up any used resources.
        """
        pass

    def get_stats(self) -> DatastreamStats:
        """Return stats for the execution so far.

        This is generally called after `execute` has completed, but may be called
        while iterating over `execute` results for streaming execution.
        """
        raise NotImplementedError
