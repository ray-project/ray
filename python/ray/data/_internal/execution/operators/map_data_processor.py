import functools
import itertools
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.context import DataContext

Row = Dict[str, Any]
MapTransformFnData = Union[Block, Row, DataBatch]
MapTransformCallable = Callable[
    [Iterable[MapTransformFnData], TaskContext], Iterable[MapTransformFnData]
]
BlockTransformCallable = Callable[[Iterable[Block], TaskContext], Iterable[Block]]
RowTransformCallable = Callable[[Iterable[Row], TaskContext], Iterable[Row]]
BatchTransformCallable = Callable[
    [Iterable[DataBatch], TaskContext], Iterable[DataBatch]
]
InitFn = Callable[[], None]


class MapTransformFnDataType(Enum):
    """An enum that represents the input/output data type of a MapTransformFn."""

    Block = 0
    Row = 1
    Batch = 2


class MapTransformFn:
    """Represents a single transform function in a MapDataProcessor."""

    def __init__(
        self,
        callable: MapTransformCallable,
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


class MapDataProcessor:
    """Encapsulates the logic of processing data for a map PhysicalOperator.

    MapDataProcessor may consist of one or more steps, each of which is represented
    as a MapTransformFn. The first transform function must take blocks as input, and
    the last transform function must output blocks.
    """

    def __init__(
        self,
        transform_fns: List[MapTransformFn],
        init_fn: Optional[InitFn] = None,
    ):
        """
        Args:
        transform_fns: A list of `MapTransformFn`s that will be executed sequentially
            to process data.
        init_fn: A function that will be called before processing data.
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
        self._init_fn()

    def process(
        self, input_blocks: Iterable[Block], ctx: TaskContext
    ) -> Iterable[Block]:
        iter = input_blocks
        # Apply the transform functions sequentially to the input iterable.
        for transform_fn in self._transform_fns:
            iter = transform_fn(iter, ctx)
        return iter

    def fuse(self, other: "MapDataProcessor") -> "MapDataProcessor":
        """Fuse two MapDataProcessors together."""

        def fused_init_fn():
            self._init_fn()
            other._init_fn()

        fused_transform_fns = self._transform_fns + other._transform_fns
        return MapDataProcessor(fused_transform_fns, init_fn=fused_init_fn)


# Below are util functions for converting input/output data.


def _input_blocks_to_rows_fn(blocks: Iterable[Block], _: TaskContext) -> Iterable[Row]:
    """Converts input blocks to rows."""
    for block in blocks:
        block = BlockAccessor.for_block(block)
        for row in block.iter_rows(public_row_format=True):
            yield row


_input_blocks_to_rows = (
    MapTransformFn(
        _input_blocks_to_rows_fn,
        MapTransformFnDataType.Block,
        MapTransformFnDataType.Row,
    ),
)


def _input_blocks_to_batches(
    blocks: Iterable[Block],
    _: TaskContext,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    zero_copy_batch: bool = False,
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
    ensure_copy = not zero_copy_batch and batch_size is not None
    formatted_batch_iter = batch_blocks(
        blocks=blocks,
        stats=None,
        batch_size=batch_size,
        batch_format=batch_format,
        ensure_copy=ensure_copy,
    )

    first = next(formatted_batch_iter, None)
    if first is None:
        # If the input blocks are all empty, then yield an empty block with same
        # format as the input blocks.
        return [empty_block]
    else:
        return itertools.chain([first], formatted_batch_iter)


def _to_output_blocks(
    iter, _: TaskContext, iter_type: MapTransformFnDataType
) -> Iterable[Block]:
    """Convert UDF-returned data to output blocks."""
    output_buffer = BlockOutputBuffer(
        None, DataContext.get_current().target_max_block_size
    )
    for data in iter:
        if iter_type == MapTransformFnDataType.Block:
            output_buffer.add_block(data)
        elif iter_type == MapTransformFnDataType.Batch:
            output_buffer.add_batch(data)
        else:
            assert iter_type == MapTransformFnDataType.Row
            output_buffer.add(data)
        while output_buffer.has_next():
            yield output_buffer.next()
    output_buffer.finalize()
    while output_buffer.has_next():
        yield output_buffer.next()


_rows_to_output_blocks = MapTransformFn(
    functools.partial(_to_output_blocks, iter_type=MapTransformFnDataType.Row),
    MapTransformFnDataType.Row,
    MapTransformFnDataType.Block,
)

_batches_to_output_blocks = MapTransformFn(
    functools.partial(_to_output_blocks, iter_type=MapTransformFnDataType.Batch),
    MapTransformFnDataType.Batch,
    MapTransformFnDataType.Block,
)

_blocks_to_output_blocks = MapTransformFn(
    functools.partial(_to_output_blocks, iter_type=MapTransformFnDataType.Block),
    MapTransformFnDataType.Block,
    MapTransformFnDataType.Block,
)


# End of util functions.


def create_map_data_processor_for_row_based_map_op(
    row_fn: RowTransformCallable,
    init_fn: Optional[InitFn] = None,
) -> MapDataProcessor:
    """Create a MapDataProcessor for a row-based map operator
    (e.g. map, flat_map, filter)."""
    transform_fns = [
        # Convert input blocks to rows.
        _input_blocks_to_rows,
        # Apply the UDF.
        MapTransformFn(row_fn, MapTransformFnDataType.Row, MapTransformFnDataType.Row),
        # Convert output rows to blocks.
        _rows_to_output_blocks,
    ]
    return MapDataProcessor(transform_fns, init_fn=init_fn)


def create_map_data_processor_for_map_batches_op(
    batch_fn: BatchTransformCallable,
    batch_size: Optional[int] = None,
    batch_format: str = "default",
    zero_copy_batch: bool = False,
    init_fn: Optional[InitFn] = None,
) -> MapDataProcessor:
    """Create a MapDataProcessor for a map_batches operator."""
    input_blocks_to_batches = (
        MapTransformFn(
            functools.partial(
                _input_blocks_to_batches,
                batch_size=batch_size,
                batch_format=batch_format,
                zero_copy_batch=zero_copy_batch,
            ),
            MapTransformFnDataType.Block,
            MapTransformFnDataType.Batch,
        ),
    )
    transform_fns = [
        # Convert input blocks to batches.
        input_blocks_to_batches,
        # Apply the UDF.
        MapTransformFn(
            batch_fn, MapTransformFnDataType.Batch, MapTransformFnDataType.Batch
        ),
        # Convert output batches to blocks.
        _batches_to_output_blocks,
    ]
    return MapDataProcessor(transform_fns, init_fn)


def create_map_data_processor_for_read_op(
    read_fn: BlockTransformCallable,
    init_fn: Optional[InitFn] = None,
) -> MapDataProcessor:
    """Create a MapDataProcessor for a read operator."""
    # TODO(hchen): Currently, we apply the BlockOuputBuffer within the read tasks.
    # We should remove that and use `_blocks_to_output_blocks` here.
    transform_fns = [
        MapTransformFn(
            read_fn, MapTransformFnDataType.Block, MapTransformFnDataType.Block
        ),
    ]
    return MapDataProcessor(transform_fns, init_fn=init_fn)


def create_map_data_processor_for_write_op(
    write_fn: BlockTransformCallable,
    init_fn: Optional[InitFn] = None,
) -> MapDataProcessor:
    """Create a MapDataProcessor for a write operator."""
    transform_fns = [
        MapTransformFn(
            write_fn, MapTransformFnDataType.Block, MapTransformFnDataType.Block
        ),
        _blocks_to_output_blocks,
    ]
    return MapDataProcessor(transform_fns, init_fn=init_fn)


def create_map_data_processor_from_block_fn(
    block_fn: BlockTransformCallable,
    init_fn: Optional[InitFn] = None,
):
    """Create a MapDataProcessor from a single block-based transform function.

    This method should only used for testing and legacy compatibility.
    """
    return MapDataProcessor(
        [
            MapTransformFn(
                block_fn, MapTransformFnDataType.Block, MapTransformFnDataType.Block
            )
        ],
        init_fn,
    )
