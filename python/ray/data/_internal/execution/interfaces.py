from dataclasses import dataclass
from typing import Dict, List, Optional, Iterable, Tuple

import ray
from ray.data._internal.logical.interfaces import Operator
from ray.data._internal.memory_tracing import trace_deallocation
from ray.data._internal.stats import DatasetStats, StatsDict
from ray.data.block import Block, BlockMetadata
from ray.data.context import DatasetContext
from ray.types import ObjectRef


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
        should_free = self.owns_blocks and DatasetContext.get_current().eager_free
        for b in self.blocks:
            trace_deallocation(b[0], "RefBundle.destroy_if_owned", free=should_free)
        return self.size_bytes() if should_free else 0


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
        elif self.object_store_memory >= 1024 * 1024 * 1024:
            return f"{round(self.object_store_memory / (1024 * 1024 * 1024), 2)} GiB"
        else:
            return f"{round(self.object_store_memory / (1024 * 1024), 2)} MiB"

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


@dataclass
class ExecutionOptions:
    """Common options for execution.

    Some options may not be supported on all executors (e.g., resource limits).
    """

    # Set a soft limit on the resource usage during execution. This is not supported
    # in bulk execution mode.
    resource_limits: ExecutionResources = ExecutionResources()

    # Set this to prefer running tasks on the same node as the output
    # node (node driving the execution).
    locality_with_output: bool = False

    # Always preserve ordering of blocks, even if using operators that
    # don't require it.
    preserve_order: bool = True


class PhysicalOperator(Operator):
    """Abstract class for physical operators.

    An operator transforms one or more input streams of RefBundles into a single
    output stream of RefBundles.

    Physical operators are stateful and non-serializable; they live on the driver side
    of the Dataset only.

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
        self._started = False

    def __reduce__(self):
        raise ValueError("Operator is not serializable.")

    def completed(self) -> bool:
        """Return True when this operator is done and all outputs are taken."""
        return (
            self._inputs_complete
            and len(self.get_work_refs()) == 0
            and not self.has_next()
        )

    def get_stats(self) -> StatsDict:
        """Return recorded execution stats for use with DatasetStats."""
        raise NotImplementedError

    def get_metrics(self) -> Dict[str, int]:
        """Returns dict of metrics reported from this operator.

        These should be instant values that can be queried at any time, e.g.,
        obj_store_mem_allocated, obj_store_mem_freed.
        """
        return {}

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

    def num_active_work_refs(self) -> int:
        """Return the number of active work refs.

        Subclasses can override this as a performance optimization.
        """
        return len(self.get_work_refs())

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
        self, dag: PhysicalOperator, initial_stats: Optional[DatasetStats] = None
    ) -> Iterable[RefBundle]:
        """Start execution.

        Args:
            dag: The operator graph to execute.
            initial_stats: The DatasetStats to prepend to the stats returned by the
                executor. These stats represent actions done to compute inputs.
        """
        raise NotImplementedError

    def get_stats(self) -> DatasetStats:
        """Return stats for the execution so far.

        This is generally called after `execute` has completed, but may be called
        while iterating over `execute` results for streaming execution.
        """
        raise NotImplementedError
