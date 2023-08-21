import itertools
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Self, TypeVar, Union

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
        callable: MapTransformCallable[MapTransformFnData, MapTransformFnData],
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

    def __call__(
        self, input: Iterable[MapTransformFnData], ctx: TaskContext
    ) -> Iterable[MapTransformFnData]:
        return self._callable(input, ctx)

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
        self._init_fn = init_fn if init_fn is not None else lambda: None

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
            MapTransformFn(
                block_fn,
                MapTransformFnDataType.Block,
                MapTransformFnDataType.Block,
            )
        ],
        init_fn,
    )


# Below are util `MapTransformFn`s for converting input/output data.


class InputBlocksToRowsMapTransformFn(MapTransformFn):
    def __init__(self):
        super().__init__(
            self._input_blocks_to_rows,
            MapTransformFnDataType.Block,
            MapTransformFnDataType.Row,
        )

    def _input_blocks_to_rows(
        self, blocks: Iterable[Block], _: TaskContext
    ) -> Iterable[Row]:
        """Converts input blocks to rows."""
        for block in blocks:
            block = BlockAccessor.for_block(block)
            for row in block.iter_rows(public_row_format=True):
                yield row

    @classmethod
    def instance(cls) -> "InputBlocksToRowsMapTransformFn":
        """Returns a singleton instance of InputBlocksToRowsMapTransformFn."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance


class InputBlocksToBatchesMapTransformFn(MapTransformFn):
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
            self._input_blocks_to_batches,
            MapTransformFnDataType.Block,
            MapTransformFnDataType.Batch,
        )

    def _input_blocks_to_batches(
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


class BuildOutputBlocksMapTransformFn(MapTransformFn):
    """A MapTransformFn that converts UDF-returned data to output blocks."""

    def __init__(self, input_type: MapTransformFnDataType):
        """
        Args:
            input_type: the type of input data.
        """
        self._input_type = input_type
        super().__init__(
            self._to_output_blocks,
            input_type,
            MapTransformFnDataType.Block,
        )

    def _to_output_blocks(
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
            None, DataContext.get_current().target_max_block_size
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
    def for_rows(cls) -> Self:
        """Return a BuildOutputBlocksMapTransformFn for row input."""
        if cls._rows_instance is None:
            cls._rows_instance = cls(MapTransformFnDataType.Row)
        return cls._rows_instance

    @classmethod
    def for_batches(cls) -> Self:
        """Return a BuildOutputBlocksMapTransformFn for batch input."""
        if cls._batches_instance is None:
            cls._batches_instance = cls(MapTransformFnDataType.Batch)
        return cls._batches_instance

    @classmethod
    def for_blocks(cls) -> Self:
        """Return a BuildOutputBlocksMapTransformFn for block input."""
        if cls._blocks_instance is None:
            cls._blocks_instance = cls(MapTransformFnDataType.Block)
        return cls._blocks_instance
