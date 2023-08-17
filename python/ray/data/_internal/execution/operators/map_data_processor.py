import functools
import itertools
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, TypeVar, Union

from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.context import DataContext

RowType = Dict[str, Any]
TransformDataType = Union[Block, RowType, DataBatch]
TransformCallable = Callable[
    [Iterable[TransformDataType], TaskContext], Iterable[TransformDataType]
]


class MapTransformDataType(Enum):
    Block = 0
    Row = 1
    Batch = 2


class MapTransformFn:
    def __init__(
        self,
        fn: TransformCallable,
        input_type: MapTransformDataType,
        output_type: MapTransformDataType,
    ):
        self._fn = fn
        self._input_type = input_type
        self._output_type = output_type

    def __call__(
        self, input: Iterable[TransformDataType], ctx: TaskContext
    ) -> Iterable[TransformDataType]:
        return self._fn(input, ctx)

    @property
    def input_type(self) -> MapTransformDataType:
        return self._input_type

    @property
    def output_type(self) -> MapTransformDataType:
        return self._output_type


class MapDataProcessor:
    def __init__(
        self,
        transform_fns: List[MapTransformFn],
        init_fn: Optional[Callable[[], None]] = None,
    ):
        self._transform_fns = transform_fns
        self._init_fn = init_fn if init_fn is not None else lambda: None

    def init(self) -> None:
        self._init_fn()

    def process(self, input_blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        iter = input_blocks
        cur_type = MapTransformDataType.Block
        for transform_fn in self._transform_fns:
            assert transform_fn.input_type == cur_type
            iter = transform_fn(iter, ctx)
            cur_type = transform_fn.output_type
        assert cur_type == MapTransformDataType.Block
        return iter

    def fuse(self, other: "MapDataProcessor") -> "MapDataProcessor":
        assert self._transform_fns[-1].output_type == other._transform_fns[0].input_type

        def fused_init_fn():
            self._init_fn()
            other._init_fn()

        fused_transform_fns = self._transform_fns + other._transform_fns
        return MapDataProcessor(fused_transform_fns, init_fn=fused_init_fn)


# Util functions that convert input blocks to UDF data.


def _input_blocks_to_rows(blocks: Iterable[Block], _: TaskContext) -> Iterable[RowType]:
    for block in blocks:
        block = BlockAccessor.for_block(block)
        for row in block.iter_rows(public_row_format=True):
            yield row


def _input_blocks_to_batches(
    blocks: Iterable[Block], _: TaskContext, batch_size, batch_format, zero_copy_batch
) -> Iterable[DataBatch]:
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


# Util functions that convert UDF data to output blocks.


def _to_output_blocks(iter, _: TaskContext, iter_type: MapTransformDataType) -> Iterable[Block]:
    output_buffer = BlockOutputBuffer(
        None, DataContext.get_current().target_max_block_size
    )
    for data in iter:
        if iter_type == MapTransformDataType.Block:
            output_buffer.add_block(data)
        elif iter_type == MapTransformDataType.Batch:
            output_buffer.add_batch(data)
        else:
            assert iter_type == MapTransformDataType.Row
            output_buffer.add(data)
        while output_buffer.has_next():
            yield output_buffer.next()
    output_buffer.finalize()
    while output_buffer.has_next():
        yield output_buffer.next()


_rows_to_output_blocks = functools.partial(
    _to_output_blocks, iter_type=MapTransformDataType.Row
)
_batches_to_output_blocks = functools.partial(
    _to_output_blocks, iter_type=MapTransformDataType.Batch
)
_blocks_to_output_blocks = functools.partial(
    _to_output_blocks, iter_type=MapTransformDataType.Block
)


def create_map_data_processor_for_map_op(op_transform_fn, init_fn) -> MapDataProcessor:
    transform_fns = [
        MapTransformFn(
            _input_blocks_to_rows, MapTransformDataType.Block, MapTransformDataType.Row
        ),
        MapTransformFn(
            op_transform_fn, MapTransformDataType.Row, MapTransformDataType.Row
        ),
        MapTransformFn(
            _rows_to_output_blocks, MapTransformDataType.Row, MapTransformDataType.Block
        ),
    ]
    return MapDataProcessor(transform_fns, init_fn=init_fn)


def create_map_data_processor_for_map_batches_op(
    op_transform_fn, batch_size, batch_format, zero_copy_batch, init_fn
) -> MapDataProcessor:
    input_blocks_to_batches = functools.partial(
        _input_blocks_to_batches,
        batch_size=batch_size,
        batch_format=batch_format,
        zero_copy_batch=zero_copy_batch,
    )
    transform_fns = [
        MapTransformFn(
            input_blocks_to_batches,
            MapTransformDataType.Block,
            MapTransformDataType.Batch,
        ),
        MapTransformFn(
            op_transform_fn, MapTransformDataType.Batch, MapTransformDataType.Batch
        ),
        MapTransformFn(
            _batches_to_output_blocks,
            MapTransformDataType.Batch,
            MapTransformDataType.Block,
        ),
    ]
    return MapDataProcessor(transform_fns, init_fn)


def create_map_data_processor_for_read_op(read_fn) -> MapDataProcessor:
    transform_fns = [
        MapTransformFn(
            read_fn, MapTransformDataType.Block, MapTransformDataType.Block
        ),
        MapTransformFn(
            _blocks_to_output_blocks,
            MapTransformDataType.Block,
            MapTransformDataType.Block,
        ),
    ]
    return MapDataProcessor(transform_fns)


def create_map_data_processor_for_write_op(write_fn) -> MapDataProcessor:
    transform_fns = [
        MapTransformFn(
            write_fn, MapTransformDataType.Block, MapTransformDataType.Block
        ),
        MapTransformFn(
            _blocks_to_output_blocks,
            MapTransformDataType.Block,
            MapTransformDataType.Block,
        ),
    ]
    return MapDataProcessor(transform_fns)
