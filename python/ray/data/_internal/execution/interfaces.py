from dataclasses import dataclass
from typing import Dict, List, Optional, Iterable, Tuple

import ray
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
class ExecutionOptions:
    """Common options for execution.

    Some options may not be supported on all executors (e.g., parallelism limit).
    """

    # Max number of in flight tasks. This is a soft limit, and is not supported in
    # bulk execution mode.
    parallelism_limit: Optional[int] = None

    # Example: set to 1GB and executor will try to limit object store
    # memory usage to 1GB. This is a soft limit, and is not supported in
    # bulk execution mode.
    memory_limit_bytes: Optional[int] = None

    # Set this to prefer running tasks on the same node as the output
    # node (node driving the execution).
    locality_with_output: bool = False

    # Always preserve ordering of blocks, even if using operators that
    # don't require it.
    preserve_order: bool = True


class PhysicalOperator:
    """Abstract class for physical operators.

    An operator transforms one or more input streams of RefBundles into a single
    output stream of RefBundles.

    Operators are stateful and non-serializable; they live on the driver side of the
    Dataset execution only.

    Here's a simple example of implementing a basic "Map" operator:

        class Map(PhysicalOperator):
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
        self._name = name
        self._input_dependencies = input_dependencies
        for x in input_dependencies:
            assert isinstance(x, PhysicalOperator), x
        self._inputs_complete = not input_dependencies

    @property
    def name(self) -> str:
        return self._name

    @property
    def input_dependencies(self) -> List["PhysicalOperator"]:
        """List of operators that provide inputs for this operator."""
        assert hasattr(
            self, "_input_dependencies"
        ), "PhysicalOperator.__init__() was not called."
        return self._input_dependencies

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

    def __reduce__(self):
        raise ValueError("PhysicalOperator is not serializable.")

    def __str__(self) -> str:
        if self.input_dependencies:
            out_str = ", ".join([str(x) for x in self.input_dependencies])
            out_str += " -> "
        else:
            out_str = ""
        out_str += f"{self.__class__.__name__}[{self._name}]"
        return out_str

    def num_outputs_total(self) -> Optional[int]:
        """Returns the total number of output bundles of this operator, if known.

        This is useful for reporting progress.
        """
        if len(self.input_dependencies) == 1:
            return self.input_dependencies[0].num_outputs_total()
        return None

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
        """Called when all upstream operator have completed().

        After this is called, the executor guarantees more inputs will be added via
        `add_input` for any input index.
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
        pass


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
