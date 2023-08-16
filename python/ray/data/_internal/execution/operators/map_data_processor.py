import collections
import functools
import itertools
from abc import abstractmethod
from enum import Enum
from types import GeneratorType
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, TypeVar, Union

import numpy as np
import pandas as pd
import pyarrow as pa

from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.numpy_support import is_valid_udf_return
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.context import DataContext
from ray.data.datasource.datasource import ReadTask

# RowBasedMapTransformFn = Callable[[RowType], Iterable[RowType]]
# BatchBasedMapTransformFn = Callable[[DataBatch], Iterable[DataBatch]]


RowType = Dict[str, Any]
TransformDataType = Union[Block, RowType, DataBatch]
TransformCallable = Callable[[Iterable[TransformDataType]], Iterable[TransformDataType]]


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

    def __call__(self, input: Iterable[TransformDataType]) -> Iterable[TransformDataType]:
        return self._fn(input)

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

    def process(self, input_blocks: Iterable[Block]) -> Iterable[Block]:
        iter = input_blocks
        cur_type = MapTransformDataType.Block
        for transform_fn in self._transform_fns:
            assert transform_fn.input_type == cur_type
            iter = transform_fn(iter)
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

def _input_blocks_to_rows(blocks: Iterable[Block]) -> Iterable[RowType]:
    for block in blocks:
        block = BlockAccessor.for_block(block)
        for row in block.iter_rows(public_row_format=True):
            yield row


def _input_blocks_to_batches(blocks: Iterable[Block], batch_size, batch_format, zero_copy_batch) -> Iterable[DataBatch]:
    try:
        block_iter = iter(blocks)
        first_block = next(block_iter)
        blocks = itertools.chain([first_block], block_iter)
        empty_block = BlockAccessor.for_block(first_block).builder().build()
        # Don't hold the first block in memory, so we reset the reference.
        first_block = None
    except StopIteration:
        first_block = None

    # Ensure that zero-copy batch views are copied so mutating UDFs don't error.
    ensure_copy = not zero_copy_batch and batch_size is not None
    formatted_batch_iter = batch_blocks(
        blocks=blocks,
        stats=None,
        batch_size=batch_size,
        batch_format=batch_format,
        ensure_copy=ensure_copy,
    )
    return formatted_batch_iter


# Util functions that convert UDF data to output blocks.

def _to_output_blocks(iter, iter_type: MapTransformDataType) -> Iterable[Block]:
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


_rows_to_output_blocks = functools.partial(_to_output_blocks, iter_type=MapTransformDataType.Row)
_batches_to_output_blocks = functools.partial(_to_output_blocks, iter_type=MapTransformDataType.Batch)
_blocks_to_output_blocks = functools.partial(_to_output_blocks, iter_type=MapTransformDataType.Block)


def create_map_data_processor_for_map_op(op_transform_fn) -> MapDataProcessor:
    transform_fns = [
        MapTransformFn(_input_blocks_to_rows, MapTransformDataType.Block, MapTransformDataType.Row),
        MapTransformFn(op_transform_fn, MapTransformDataType.Row, MapTransformDataType.Row),
        MapTransformFn(_rows_to_output_blocks, MapTransformDataType.Row, MapTransformDataType.Block),
    ]
    return MapDataProcessor(transform_fns)


def create_map_data_processor_for_map_batches_op(op_transorm_fn) -> MapDataProcessor:
    input_blocks_to_batches = functools.partial(_input_blocks_to_batches, batch_size=1, batch_format="row", zero_copy_batch=False)
    transform_fns = [
        MapTransformFn(input_blocks_to_batches, MapTransformDataType.Block, MapTransformDataType.Batch),
        MapTransformFn(op_transorm_fn, MapTransformDataType.Batch, MapTransformDataType.Batch),
        MapTransformFn(_batches_to_output_blocks, MapTransformDataType.Batch, MapTransformDataType.Block),
    ]
    return MapDataProcessor(transform_fns)


def create_map_data_processor_for_read_op(op_transform_fn) -> MapDataProcessor:
    transform_fns = [
        MapTransformFn(op_transform_fn, MapTransformDataType.Block, MapTransformDataType.Block),
        MapTransformFn(_blocks_to_output_blocks, MapTransformDataType.Block, MapTransformDataType.Block),
    ]
    return MapDataProcessor(transform_fns)



class ReadOpMapDataProcessor(MapDataProcessor):
    def process(self, input_blocks: Iterable[Block]) -> Iterable[Block]:
        for read_task in input_blocks:
            assert isinstance(read_task, ReadTask)
            for batch in read_task():
                self._output_buffer.add_block(batch)
                yield from self._yield_available_blocks()
        self._output_buffer.finalize()
        yield from self._yield_available_blocks()


class BatchBasedMapDataProcessor(MapDataProcessor):
    def __init__(
        self,
        transform_fn,
        batch_size,
        batch_format,
        zero_copy_batch,
        init_fn: Optional[Callable[[], None]] = None,
    ):
        super().__init__(init_fn)
        self._transform_fn = transform_fn
        self._batch_size = batch_size
        self._batch_format = batch_format
        self._ensure_copy = not zero_copy_batch and batch_size is not None
                    )

    def process(self, input_blocks: Iterable[Block]) -> Iterable[Block]:
        blocks = input_blocks
        try:
            block_iter = iter(blocks)
            first_block = next(block_iter)
            blocks = itertools.chain([first_block], block_iter)
            empty_block = BlockAccessor.for_block(first_block).builder().build()
            # Don't hold the first block in memory, so we reset the reference.
            first_block = None
        except StopIteration:
            first_block = None

        # Ensure that zero-copy batch views are copied so mutating UDFs don't error.
        formatted_batch_iter = batch_blocks(
            blocks=blocks,
            stats=None,
            batch_size=self._batch_size,
            batch_format=self._batch_format,
            ensure_copy=self._ensure_copy,
        )

        has_batches = False
        for batch in formatted_batch_iter:
            has_batches = True
            # Apply UDF.
            try:
                batch = self._transform_fn(batch)

                if not isinstance(batch, GeneratorType):
                    batch = [batch]

                for b in batch:
                    self._validate_batch(b)
                    # Add output batch to output buffer.
                    self._output_buffer.add_batch(b)
                    yield from self._yield_available_blocks()
            except ValueError as e:
                read_only_msgs = [
                    "assignment destination is read-only",
                    "buffer source array is read-only",
                ]
                err_msg = str(e)
                if any(msg in err_msg for msg in read_only_msgs):
                    raise ValueError(
                        f"Batch mapper function {fn.__name__} tried to mutate a "
                        "zero-copy read-only batch. To be able to mutate the "
                        "batch, pass zero_copy_batch=False to map_batches(); "
                        "this will create a writable copy of the batch before "
                        "giving it to fn. To elide this copy, modify your mapper "
                        "function so it doesn't try to mutate its input."
                    ) from e
                else:
                    raise e from None

        if not has_batches:
            # If the input blocks are all empty, then yield an empty block with same
            # format as the input blocks.
            yield empty_block
        else:
            self._output_buffer.finalize()
            yield from self._yield_available_blocks()


class FusedMapDataProcessor(MapDataProcessor):
    def __init__(self, processors: List[MapDataProcessor]):
        self._processors = processors

    def init(self):
        for processor in self._processors:
            processor.init()

    def process(self, input_blocks: Iterable[Block]) -> Iterable[Block]:
        blocks = input_blocks
        for processor in self._processors:
            blocks = processor.process(blocks)
        yield from blocks
