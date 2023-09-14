import itertools
from abc import abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar, Union

from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.context import DataContext

# Allowed input/output data types for a MapTransformFn.
Row = Dict[str, Any]
MapTransformFnData = Union[Block, Row, DataBatch]

# Function signature of a MapTransformFn.
IN = TypeVar("IN")
OUT = TypeVar("OUT")
MapTransformCallable = Callable[[Iterable[IN], TaskContext], Iterable[OUT]]


class MapTransformFnDataType(Enum):
    """An enum that represents the input/output data type of a MapTransformFn."""

    Block = 0
    Row = 1
    Batch = 2


class MapTransformFn:
    """Represents a single transform function in a MapTransformer."""

    def __init__(
        self,
        input_type: MapTransformFnDataType,
        output_type: MapTransformFnDataType,
    ):
        """
        Args:
            callable: the underlying Python callable object.
            input_type: the type of the input data.
            output_type: the type of the output data.
        """
        self._callable = callable
        self._input_type = input_type
        self._output_type = output_type

    @abstractmethod
    def __call__(
        self, input: Iterable[MapTransformFnData], ctx: TaskContext
    ) -> Iterable[MapTransformFnData]:
        ...

    @property
    def input_type(self) -> MapTransformFnDataType:
        return self._input_type

    @property
    def output_type(self) -> MapTransformFnDataType:
        return self._output_type


class MapTransformer:
    """Encapsulates the data transformation logic of a physical MapOperator.

    A MapTransformer may consist of one or more steps, each of which is represented
    as a MapTransformFn. The first MapTransformFn must take blocks as input, and
    the last MapTransformFn must output blocks. The intermediate data types can
    be blocks, rows, or batches.
    """

    def __init__(
        self,
        transform_fns: List[MapTransformFn],
        init_fn: Optional[Callable[[], None]] = None,
    ):
        """
        Args:
        transform_fns: A list of `MapTransformFn`s that will be executed sequentially
            to transform data.
        init_fn: A function that will be called before transforming data.
            Used for the actor-based map operator.
        """
        self.set_transform_fns(transform_fns)
        self._init_fn = init_fn if init_fn is not None else lambda: None

    def set_transform_fns(self, transform_fns: List[MapTransformFn]) -> None:
        """Set the transform functions."""
        assert len(transform_fns) > 0
        assert (
            transform_fns[0].input_type == MapTransformFnDataType.Block
        ), "The first transform function must take blocks as input."
        assert (
            transform_fns[-1].output_type == MapTransformFnDataType.Block
        ), "The last transform function must output blocks."

        for i in range(len(transform_fns) - 1):
            assert transform_fns[i].output_type == transform_fns[i + 1].input_type, (
                "The output type of the previous transform function must match "
                "the input type of the next transform function."
            )
        self._transform_fns = transform_fns

    def get_transform_fns(self) -> List[MapTransformFn]:
        """Get the transform functions."""
        return self._transform_fns

    def init(self) -> None:
        """Initialize the transformer.

        Should be called before applying the transform.
        """
        self._init_fn()

    def apply_transform(
        self, input_blocks: Iterable[Block], ctx: TaskContext
    ) -> Iterable[Block]:
        """Apply the transform functions to the input blocks."""
        iter = input_blocks
        # Apply the transform functions sequentially to the input iterable.
        for transform_fn in self._transform_fns:
            iter = transform_fn(iter, ctx)
        return iter

    def fuse(self, other: "MapTransformer") -> "MapTransformer":
        """Fuse two `MapTransformer`s together."""

        # Define them as standalone variables to avoid fused_init_fn capturing the
        # entire `MapTransformer` object.
        self_init_fn = self._init_fn
        other_init_fn = other._init_fn

        def fused_init_fn():
            self_init_fn()
            other_init_fn()

        fused_transform_fns = self._transform_fns + other._transform_fns
        return MapTransformer(fused_transform_fns, init_fn=fused_init_fn)


def create_map_transformer_from_block_fn(
    block_fn: MapTransformCallable[Block, Block],
    init_fn: Optional[Callable[[], None]] = None,
):
    """Create a MapTransformer from a single block-based transform function.

    This method should only be used for testing and legacy compatibility.
    """
    return MapTransformer(
        [
            BlockMapTransformFn(block_fn),
        ],
        init_fn,
    )


# Below are subclasses of MapTransformFn.


class RowMapTransformFn(MapTransformFn):
    """A rows-to-rows MapTransformFn."""

    def __init__(self, row_fn: MapTransformCallable[Row, Row]):
        self._row_fn = row_fn
        super().__init__(
            MapTransformFnDataType.Row,
            MapTransformFnDataType.Row,
        )

    def __call__(self, input: Iterable[Row], ctx: TaskContext) -> Iterable[Row]:
        yield from self._row_fn(input, ctx)

    def __repr__(self) -> str:
        return f"RowMapTransformFn({self._row_fn})"


class BatchMapTransformFn(MapTransformFn):
    """A batch-to-batch MapTransformFn."""

    def __init__(self, batch_fn: MapTransformCallable[DataBatch, DataBatch]):
        self._batch_fn = batch_fn
        super().__init__(
            MapTransformFnDataType.Batch,
            MapTransformFnDataType.Batch,
        )

    def __call__(
        self, input: Iterable[DataBatch], ctx: TaskContext
    ) -> Iterable[DataBatch]:
        yield from self._batch_fn(input, ctx)

    def __repr__(self) -> str:
        return f"BatchMapTransformFn({self._batch_fn})"


class BlockMapTransformFn(MapTransformFn):
    """A block-to-block MapTransformFn."""

    def __init__(self, block_fn: MapTransformCallable[Block, Block]):
        self._block_fn = block_fn
        super().__init__(
            MapTransformFnDataType.Block,
            MapTransformFnDataType.Block,
        )

    def __call__(self, input: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        yield from self._block_fn(input, ctx)

    def __repr__(self) -> str:
        return f"BlockMapTransformFn({self._block_fn})"


class BlocksToRowsMapTransformFn(MapTransformFn):
    """A MapTransformFn that converts input blocks to rows."""

    def __init__(self):
        super().__init__(
            MapTransformFnDataType.Block,
            MapTransformFnDataType.Row,
        )

    def __call__(self, blocks: Iterable[Block], _: TaskContext) -> Iterable[Row]:
        for block in blocks:
            block = BlockAccessor.for_block(block)
            for row in block.iter_rows(public_row_format=True):
                yield row

    @classmethod
    def instance(cls) -> "BlocksToRowsMapTransformFn":
        """Returns the singleton instance."""
        if getattr(cls, "_instance", None) is None:
            cls._instance = cls()
        return cls._instance

    def __repr__(self) -> str:
        return "BlocksToRowsMapTransformFn()"


class BlocksToBatchesMapTransformFn(MapTransformFn):
    """A MapTransformFn that converts input blocks to batches."""

    def __init__(
        self,
        batch_size: Optional[int] = None,
        batch_format: str = "default",
        zero_copy_batch: bool = False,
    ):
        self._batch_size = batch_size
        self._batch_format = batch_format
        self._ensure_copy = not zero_copy_batch and batch_size is not None
        super().__init__(
            MapTransformFnDataType.Block,
            MapTransformFnDataType.Batch,
        )

    def __call__(
        self,
        blocks: Iterable[Block],
        _: TaskContext,
    ) -> Iterable[DataBatch]:
        """Converts input blocks to batches."""
        block_iter = iter(blocks)
        first = next(block_iter, None)
        if first is None:
            return []
        blocks = itertools.chain([first], block_iter)
        empty_block = BlockAccessor.for_block(first).builder().build()
        # Don't hold the first block in memory, so we reset the reference.
        first = None

        # Ensure that zero-copy batch views are copied so mutating UDFs don't error.
        formatted_batch_iter = batch_blocks(
            blocks=blocks,
            stats=None,
            batch_size=self._batch_size,
            batch_format=self._batch_format,
            ensure_copy=self._ensure_copy,
        )

        first = next(formatted_batch_iter, None)
        if first is None:
            # If the input blocks are all empty, then yield an empty block with same
            # format as the input blocks.
            return [empty_block]
        else:
            return itertools.chain([first], formatted_batch_iter)

    @property
    def batch_size(self) -> Optional[int]:
        return self._batch_size

    @property
    def batch_format(self) -> str:
        return self._batch_format

    @property
    def zero_copy_batch(self) -> bool:
        return not self._ensure_copy

    def __repr__(self) -> str:
        return (
            f"BlocksToBatchesMapTransformFn("
            f"batch_size={self._batch_size}, "
            f"batch_format={self._batch_format}, "
            f"zero_copy_batch={self.zero_copy_batch}"
            f")"
        )


class BuildOutputBlocksMapTransformFn(MapTransformFn):
    """A MapTransformFn that converts UDF-returned data to output blocks."""

    def __init__(self, input_type: MapTransformFnDataType):
        """
        Args:
            input_type: the type of input data.
        """
        self._input_type = input_type
        super().__init__(
            input_type,
            MapTransformFnDataType.Block,
        )

    def __call__(
        self,
        iter: Iterable[MapTransformFnData],
        _: TaskContext,
    ) -> Iterable[Block]:
        """Convert UDF-returned data to output blocks.

        Args:
            iter: the iterable of UDF-returned data, whose type
                must match self._input_type.
        """
        output_buffer = BlockOutputBuffer(
            DataContext.get_current().target_max_block_size
        )
        if self._input_type == MapTransformFnDataType.Block:
            add_fn = output_buffer.add_block
        elif self._input_type == MapTransformFnDataType.Batch:
            add_fn = output_buffer.add_batch
        else:
            assert self._input_type == MapTransformFnDataType.Row
            add_fn = output_buffer.add
        for data in iter:
            add_fn(data)
            while output_buffer.has_next():
                yield output_buffer.next()
        output_buffer.finalize()
        while output_buffer.has_next():
            yield output_buffer.next()

    @classmethod
    def for_rows(cls) -> "BuildOutputBlocksMapTransformFn":
        """Return a BuildOutputBlocksMapTransformFn for row input."""
        if getattr(cls, "_instance_for_rows", None) is None:
            cls._instance_for_rows = cls(MapTransformFnDataType.Row)
        return cls._instance_for_rows

    @classmethod
    def for_batches(cls) -> "BuildOutputBlocksMapTransformFn":
        """Return a BuildOutputBlocksMapTransformFn for batch input."""
        if getattr(cls, "_instance_for_batches", None) is None:
            cls._instance_for_batches = cls(MapTransformFnDataType.Batch)
        return cls._instance_for_batches

    @classmethod
    def for_blocks(cls) -> "BuildOutputBlocksMapTransformFn":
        """Return a BuildOutputBlocksMapTransformFn for block input."""
        if getattr(cls, "_instance_for_blocks", None) is None:
            cls._instance_for_blocks = cls(MapTransformFnDataType.Block)
        return cls._instance_for_blocks

    def __repr__(self) -> str:
        return f"BuildOutputBlocksMapTransformFn(input_type={self._input_type})"
