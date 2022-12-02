from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Iterator, Tuple, Callable

import ray
from ray.data._internal.stats import DatasetStats
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef


@dataclass
class RefBundle:
    """A group of data block references and their metadata.

    Operators take in and produce streams of RefBundles.

    Most commonly an RefBundle consists of a single block object reference.
    In some cases, e.g., due to block splitting, or for a SortReduce task, there may
    be more than one block.

    Block bundles have ownership semantics, i.e., shared_ptr vs unique_ptr. This
    allows operators to know whether they can destroy blocks when they don't need
    them. Destroying blocks eagerly is more efficient than waiting for Python GC /
    Ray reference counting to kick in.
    """

    # The num_rows / size_bytes must be known in the metadata.
    blocks: List[Tuple[ObjectRef[Block], BlockMetadata]]

    # Serializable extra data passed from upstream operator. This can be
    # used to implement per-block behavior, for example, the last task
    # for a Limit() operation truncates the block at a certain row.
    input_metadata: Dict[str, Any] = field(default_factory=lambda: {})

    # Whether we own the blocks (can safely destroy them).
    owns_blocks: bool = False

    def __post_init__(self):
        for b in self.blocks:
            assert isinstance(b, tuple), b
            assert len(b) == 2, b
            assert isinstance(b[0], ray.ObjectRef), b
            assert isinstance(b[1], BlockMetadata), b
            assert b[1].num_rows is not None, b
            assert b[1].size_bytes is not None, b

    def num_rows(self) -> int:
        """Number of rows present in this bundle."""
        return sum(b[1].num_rows for b in self.blocks)

    def size_bytes(self) -> int:
        """Size of the blocks of this bundle in bytes."""
        return sum(b[1].size_bytes for b in self.blocks)

    def destroy(self) -> None:
        """Clears the object store memory for these blocks."""
        assert self.owns_blocks, "Should not destroy unowned blocks."
        raise NotImplementedError


@dataclass
class ExecutionOptions:
    """Common options that should be supported by all Executor implementations."""

    # Max number of in flight tasks.
    parallelism_limit: Optional[int] = None

    # Example: set to 1GB and executor will try to limit object store
    # memory usage to 1GB.
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
    output stream of RefBundles. There are three types of operators that Executors
    must be aware of in operator DAGs.

    Subclasses:
        OneToOneOperator: handles one-to-one operations (e.g., map, filter)
        ExchangeOperator: handles other types of operations (e.g., shuffle, union)
    """

    def __init__(self, name: str, input_dependencies: List["PhysicalOperator"]):
        self._name = name
        self._input_dependencies = input_dependencies
        for x in input_dependencies:
            assert isinstance(x, PhysicalOperator), x

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

    def __reduce__(self):
        raise ValueError("PhysicalOperator is not serializable.")

    def num_outputs_total(self) -> Optional[int]:
        """Returns the total number of output bundles of this operator, if known.

        This is useful for reporting progress.
        """
        if len(self.input_dependencies) == 1:
            return self.input_dependencies[0].num_outputs_total()
        return None

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        """Called when an upstream result is available."""
        raise NotImplementedError

    def inputs_done(self, input_index: int) -> None:
        """Called when an upstream operator finishes."""
        raise NotImplementedError

    def has_next(self) -> bool:
        """Returns when a downstream output is available."""
        raise NotImplementedError

    def get_next(self) -> RefBundle:
        """Get the next downstream output."""
        raise NotImplementedError

    def get_tasks(self) -> List[ray.ObjectRef]:
        """Get a list of object references the executor should wait on."""
        return []

    def notify_task_completed(self, task: ray.ObjectRef) -> None:
        """Executor calls this when the given task is completed and local."""
        raise NotImplementedError

    def release_unused_resources(self) -> None:
        """Release any currently unused operator resources."""
        pass


class Executor:
    """Abstract class for executors, which implement physical operator execution.

    Subclasses:
        BulkExecutor
        PipelinedExecutor
    """

    def __init__(self, options: ExecutionOptions):
        """Create the executor."""
        self._options = options

    def execute(self, dag: PhysicalOperator) -> Iterator[RefBundle]:
        """Start execution."""
        raise NotImplementedError

    def get_stats() -> DatasetStats:
        """Return stats for the execution so far."""
        raise NotImplementedError


class ExchangeOperator(PhysicalOperator):
    """A streaming operator for more complex parallel transformations.

    Subclasses have full control over how to buffer and transform input blocks, which
    enables them to implement metadata-only stream transformations (e.g., union),
    as well as all-to-all transformations (e.g., shuffle, zip).

    Subclasses:
        AllToAllOperator
    """

    pass


class AllToAllOperator(ExchangeOperator):
    """An ExchangeOperator that doesn't execute until all inputs are available.

    Used to implement all:all transformations such as sort / shuffle.

    Subclasses:
         SortMap
    """

    def __init__(self, preprocessor: Optional[Callable] = None):
        self._preprocessor = preprocessor
        self._buffer = []
        self._outbox = None

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0, "AllToAll only supports one input."
        self._buffer.append(refs)

    def inputs_done(self, input_index: int) -> None:
        # Note: blocking synchronous execution for now.
        self._outbox = self.execute_all(self._buffer)

    def has_next(self) -> bool:
        return bool(self._outbox)

    def get_next(self) -> RefBundle:
        return self._outbox.pop(0)

    def execute_all(self, inputs: List[RefBundle]) -> List[RefBundle]:
        """Execute distributedly from a driver process.

        This is a synchronous call that blocks until the computation is completed.

        Args:
             inputs: List of ref bundles.
        """
        raise NotImplementedError
