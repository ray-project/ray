import collections
import itertools
from abc import abstractmethod
from typing import Any, Callable, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa

from ray.data._internal.block_batching.block_batching import batch_blocks
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.context import DataContext

RowType = Dict[str, Any]
RowBasedMapTransformFn = Callable[[RowType], Iterable[RowType]]
BatchBasedMapTransformFn = Callable[[DataBatch], Iterable[DataBatch]]


class MapDataProcessor:
    def __init__(self, init_fn: Optional[Callable[[], None]] = None):
        self._init_fn = init_fn
        self._output_buffer = BlockOutputBuffer(
            None, DataContext.get_current().target_max_block_size
        )

    def init(self) -> None:
        if self._init_fn is not None:
            self._init_fn()

    @abstractmethod
    def process(self, input_blocks: Iterable[Block]) -> Iterable[Block]:
        pass

    def fuse(self, other: "MapDataProcessor") -> "MapDataProcessor":
        return FusedMapDataProcessor([self, other])

    def _yield_available_blocks(self) -> Iterable[Block]:
        while self._output_buffer.has_next():
            yield self._output_buffer.next()


class RowBasedMapDataProcessor(MapDataProcessor):
    def __init__(
        self,
        transform_fn: RowBasedMapTransformFn,
        init_fn: Optional[Callable[[], None]] = None,
    ):
        super().__init__(init_fn)
        self._transform_fn = transform_fn

    def process(self, input_blocks: Iterable[Block]) -> Iterable[Block]:
        for block in input_blocks:
            block = BlockAccessor.for_block(block)
            for row in block.iter_rows(public_row_format=True):
                for item in self._transform_fn(row):
                    self._validate_output(item)
                    self._output_buffer.add(item)
                    yield from self._yield_available_blocks()
        self._output_buffer.finalize()
        yield from self._yield_available_blocks()

    def _validate_output(self, item):
        if not isinstance(item, collections.abc.Mapping):
            raise ValueError(
                f"Error validating {_truncated_repr(item)}: "
                "Standalone Python objects are not "
                "allowed in Ray 2.5. To return Python objects from map(), "
                "wrap them in a dict, e.g., "
                "return `{'item': item}` instead of just `item`."
            )


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

    def _validate_batch(self, batch: Block) -> None:
        if not isinstance(
            batch,
            (
                list,
                pa.Table,
                np.ndarray,
                collections.abc.Mapping,
                pd.core.frame.DataFrame,
            ),
        ):
            raise ValueError(
                "The `fn` you passed to `map_batches` returned a value of type "
                f"{type(batch)}. This isn't allowed -- `map_batches` expects "
                "`fn` to return a `pandas.DataFrame`, `pyarrow.Table`, "
                "`numpy.ndarray`, `list`, or `dict[str, numpy.ndarray]`."
            )

        if isinstance(batch, list):
            raise ValueError(
                f"Error validating {_truncated_repr(batch)}: "
                "Returning a list of objects from `map_batches` is not "
                "allowed in Ray 2.5. To return Python objects, "
                "wrap them in a named dict field, e.g., "
                "return `{'results': objects}` instead of just `objects`."
            )

        if isinstance(batch, collections.abc.Mapping):
            for key, value in list(batch.items()):
                if not is_valid_udf_return(value):
                    raise ValueError(
                        f"Error validating {_truncated_repr(batch)}: "
                        "The `fn` you passed to `map_batches` returned a "
                        f"`dict`. `map_batches` expects all `dict` values "
                        f"to be `list` or `np.ndarray` type, but the value "
                        f"corresponding to key {key!r} is of type "
                        f"{type(value)}. To fix this issue, convert "
                        f"the {type(value)} to a `np.ndarray`."
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
