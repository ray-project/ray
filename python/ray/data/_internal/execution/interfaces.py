from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Iterator, Tuple

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

    def destroy(self) -> None:
        """Clears the object store memory for these blocks."""
        assert self.owns_blocks, "Should not destroy unowned blocks."
        raise NotImplementedError


@dataclass
class ExecutionOptions:
    """Common options that should be supported by all Executor implementations."""

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
        OneToOneOperator
        AllToAllOperator
        BufferOperator
    """

    def __init__(self, input_dependencies: List["PhysicalOperator"]):
        self._input_dependencies = input_dependencies
        for x in input_dependencies:
            assert isinstance(x, PhysicalOperator), x

    @property
    def input_dependencies(self) -> List["PhysicalOperator"]:
        """List of operators that provide inputs for this operator."""
        assert hasattr(
            self, "_input_dependencies"
        ), "PhysicalOperator.__init__() was not called."
        return self._input_dependencies


class Executor:
    """Abstract class for executors, wihch implement physical operator execution.

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


class OneToOneOperator(PhysicalOperator):
    """Abstract class for operators that run on a single process.

    Used to implement 1:1 transformations. The executor will need to
    wrap the operator in Ray tasks or actors for actual execution,
    e.g., using TaskPoolStrategy or ActorPoolStrategy.

    Subclasses:
        Read
        Map
        Write
        SortReduce
        WholeStage
    """

    def execute_one(
        self, block_bundle: Iterator[Block], input_metadata: Dict[str, Any]
    ) -> Iterator[Block]:
        """Execute locally on a worker process.

        Args:
            block_bundle: Iterator over input blocks of a RefBundle. Typically, this
                will yield only a single block, unless the transformation has multiple
                inputs, e.g., in the SortReduce or ZipBlocks cases. It is an iterator
                instead of a list for memory efficiency.
            input_metadata: Extra metadata provided from the upstream operator.
        """
        raise NotImplementedError


class AllToAllOperator(PhysicalOperator):
    """Abstract class for operators defining their entire distributed execution.

    Used to implement all:all transformations.

    This also defines a barrier between operators in the DAG. Operators
    before and after an AllToAllOperator will not run concurrently.

    Subclasses:
         SortMap
    """

    def __init__(self, preprocessor: Optional[OneToOneOperator] = None):
        self._preprocessor = preprocessor

    def execute_all(self, inputs: List[RefBundle]) -> List[RefBundle]:
        """Execute distributedly from a driver process.

        This is a synchronous call that blocks until the computation is completed.

        Args:
             inputs: List of ref bundles.
        """
        raise NotImplementedError


class BufferOperator(PhysicalOperator):
    """A streaming operator that buffers blocks for downstream operators.

    For example, this can take two operators and combine their blocks
    pairwise for zip, group adjacent blocks for repartition, etc.

    Buffers do not read or transform any block data; they only operate
    on block metadata.

    Examples:
        Zip = ZipBuffer + DoZip
        Union = UnionBuffer
        Repartition(False) = SplitBuf + Splitter + CombineBuf + Combiner
        Cache = CacheBuffer
        Limit = LimitBuffer + MaybeTruncate
        RandomizeBlockOrder = RandomizeBlockOrderBuffer
    """

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
