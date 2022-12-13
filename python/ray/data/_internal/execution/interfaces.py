from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Iterator, Tuple

import ray
from ray.data._internal.stats import DatasetStats, StatsDict
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

    # Whether we own the blocks (can safely destroy them).
    owns_blocks: bool

    # Serializable extra data passed from upstream operator. This can be
    # used to implement per-block behavior, for example, the last task
    # for a Limit() operation truncates the block at a certain row.
    input_metadata: Dict[str, Any] = field(default_factory=lambda: {})

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

    def destroy_if_owned(self) -> None:
        """Clears the object store memory for these blocks if owned."""
        if self.owns_blocks:
            ray._private.internal_api.free(
                [b[0] for b in self.blocks], local_only=False
            )


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
    output stream of RefBundles.

    Operators are stateful and non-serializable; they live on the driver side of the
    Dataset execution only.
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

    def get_stats(self) -> StatsDict:
        """Return recorded execution stats for use with DatasetStats."""
        raise NotImplementedError

    def __reduce__(self):
        raise ValueError("PhysicalOperator is not serializable.")

    def __str__(self):
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
        """Called when an upstream result is available."""
        raise NotImplementedError

    def inputs_done(self, input_index: int) -> None:
        """Called when an upstream operator finishes."""
        pass

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

    def get_stats(self) -> DatasetStats:
        """Return stats for the execution so far."""
        raise NotImplementedError
