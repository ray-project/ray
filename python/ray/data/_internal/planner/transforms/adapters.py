from abc import ABC, abstractmethod
from enum import Enum
from typing import Iterator, Union, Dict, Tuple, Optional

from ray.data.block import Block, BlockAccessor, DataBatch, T as Row
from ray.data.context import DatasetContext
from ray.data._internal.block_batching import batch_blocks
from ray.data._internal.block_builder import BlockBuilder
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import (
    MapBatches,
    MapRows,
    Filter,
    FlatMap,
    Write,
)
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.metrics import MetricsCollector
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.table_block import TableRow


class DataType(Enum):
    BLOCK = 1
    BATCH = 2
    ROW = 3


# Map from logical transformation operators (maps, reads) to their input and output data
# type.
OP_TO_DATA_TYPE_MAP = {
    Read: DataType.BLOCK,
    MapBatches: DataType.BATCH,
    MapRows: DataType.ROW,
    Filter: DataType.ROW,
    FlatMap: DataType.ROW,
    Write: DataType.BLOCK,
}


DataT = Union[Block, DataBatch, Row]


class Adapter(ABC):
    """A data adapter that converts Iterator[T] to Iterator[V]."""

    def __init__(self):
        self._builder = None
        self._metrics_collector = None

    @abstractmethod
    def adapt(self, data: Iterator[DataT]) -> Iterator[DataT]:
        """Convert the input data iterator to a new data iterator, possibly of a
        different data format.
        """
        raise NotImplementedError

    def _delegate(self, data: Iterator[DataT], *adapters: "Adapter") -> Iterator[DataT]:
        """Delegate this adapter to one or more chained inner adapters."""
        for adapter in adapters:
            # Register this adapter's metrics collector with every inner adapter.
            adapter.register_metrics_collector(self._metrics_collector)
            # Chain adapters.
            data = adapter.adapt(data)
        yield from data
        # Use the builder from the first adapter.
        self._builder = adapters[0].builder()

    def register_metrics_collector(self, metrics_collector: MetricsCollector):
        """Register the collector for this adapter's metrics.

        This must be called before metrics_collector.adapt().
        """
        self._metrics_collector = metrics_collector

    def builder(self) -> Optional[BlockBuilder]:
        """Returns the block builder that was constructed based on this adapter's input
        data stream.

        This can be used to be an empty block of the correct type.
        """
        return self._builder

    def _set_builder_for_data(self, data: DataT):
        """Set the empty block builder for this adapter."""
        # NOTE: We call this method for every block/batch/row in an adapter, so calling
        # it should be fast (O(1), no data copying).
        if self._builder is not None:
            # We only set the builder once, short-circuit.
            return

        import pandas
        import pyarrow
        import numpy as np
        from ray.data._internal.arrow_block import ArrowRow, ArrowBlockBuilder
        from ray.data._internal.pandas_block import PandasRow, PandasBlockBuilder
        from ray.data._internal.simple_block import SimpleBlockBuilder

        if isinstance(data, (pyarrow.Table, np.ndarray, dict, bytes, ArrowRow)):
            # TODO(Clark): Simulate the DelegatingBlockBuilder try-except behavior of
            # adding a row to the ArrowBlockBuilder and trying to build a table to see
            # if it fails, falling back to the SimpleBlockBuilder if so?
            builder = ArrowBlockBuilder()
        elif isinstance(data, (pandas.DataFrame, PandasRow)):
            builder = PandasBlockBuilder()
        else:
            builder = SimpleBlockBuilder()

        self._builder = builder


# --------------
# Input Adapters
# --------------


class InputAdapter(Adapter):
    """An adapter between the inputs of a task and the input format of an operator.

    This operator converts the input format of Ray tasks (blocks) to the expected input
    format of the provided operator.
    """

    def __init__(self, down_logical_op: LogicalOperator):
        self._down_logical_op = down_logical_op
        super().__init__()

    def from_downstream_op(down_logical_op: LogicalOperator) -> "InputAdapter":
        """Create the appropriate adapter for the provided downstream operator."""
        # Map from the input data type of the input/first operator transform and the
        # adapter that can convert between the physical operator inputs (iterator of
        # blocks) and that data type.
        INPUT_OP_DATA_TYPE_TO_ADAPTER_MAP: Dict[DataType, InputAdapter] = {
            DataType.BLOCK: BlocksInputAdapter,
            DataType.BATCH: BatchesInputAdapter,
            DataType.ROW: RowsInputAdapter,
        }
        assert type(down_logical_op) in OP_TO_DATA_TYPE_MAP
        down_data_type = OP_TO_DATA_TYPE_MAP[type(down_logical_op)]
        adapter = INPUT_OP_DATA_TYPE_TO_ADAPTER_MAP[down_data_type]
        return adapter(down_logical_op)

    @abstractmethod
    def adapt(self, data: Iterator[Block]) -> Iterator[DataT]:
        raise NotImplementedError


class BlocksInputAdapter(InputAdapter):
    def adapt(self, blocks: Iterator[Block]) -> Iterator[Block]:
        for block in blocks:
            self._set_builder_for_data(block)
            yield block


class BatchesInputAdapter(InputAdapter):
    """The input adapter for converting from an iterator of blocks to a downstream
    operator taking an iterator of batches.
    """

    def adapt(self, blocks: Iterator[Block]) -> Iterator[DataBatch]:
        assert isinstance(self._down_logical_op, MapBatches)

        # TODO(Clark): Move all zero_copy_batch logic into this adapter.
        inner_adapter = BlocksToBatchesAdapter(
            self._down_logical_op._batch_size,
            self._down_logical_op._batch_format,
            self._down_logical_op._prefetch_batches,
            self._down_logical_op._zero_copy_batch,
        )
        yield from self._delegate(blocks, inner_adapter)


class RowsInputAdapter(InputAdapter):
    """The input adapter for converting from an iterator of blocks to a downstream
    operator taking an iterator of rows.
    """

    def adapt(self, blocks: Iterator[Block]) -> Iterator[Row]:
        yield from self._delegate(blocks, BlocksToRowsAdapter())


# --------------
# Output Adapters
# --------------


class OutputAdapter(Adapter):
    """An adapter between the outputs of an operator and the output format of a task.

    This operator converts the outputs of the provided upstream operator the the format
    (blocks) that is expected for Ray task outputs.
    """

    def __init__(self, up_logical_op: LogicalOperator):
        self._up_logical_op = up_logical_op
        super().__init__()

    def from_upstream_op(up_logical_op: LogicalOperator) -> "OutputAdapter":
        """Create the appropriate output adapter for the provided upstream operator."""
        # Map from the output data type of the output/last operator transform and the
        # adapter that can convert between the physical operator inputs (iterator of
        # blocks) and that data type.
        OUTPUT_OP_DATA_TYPE_TO_ADAPTER_MAP: Dict[DataType, OutputAdapter] = {
            DataType.BLOCK: BlocksOutputAdapter,
            DataType.BATCH: BatchesOutputAdapter,
            DataType.ROW: RowsOutputAdapter,
        }
        assert type(up_logical_op) in OP_TO_DATA_TYPE_MAP
        up_data_type = OP_TO_DATA_TYPE_MAP[type(up_logical_op)]
        adapter = OUTPUT_OP_DATA_TYPE_TO_ADAPTER_MAP[up_data_type]
        return adapter(up_logical_op)

    @abstractmethod
    def adapt(self, data: Iterator[DataT]) -> Iterator[Block]:
        raise NotImplementedError


class BlocksOutputAdapter(OutputAdapter):
    """The output adapter for an upstream operator returning an iterator of blocks,
    adapted to an iterator of blocks of a bounded size.
    """

    def adapt(self, blocks: Iterator[Block]) -> Iterator[Block]:
        ctx = DatasetContext.get_current()
        yield from self._delegate(
            blocks, BlocksToBlocksAdapter(ctx.target_max_block_size)
        )


class BatchesOutputAdapter(OutputAdapter):
    """The output adapter for an upstream operator returning an iterator of batches,
    adapted to an iterator of blocks of a bounded size.
    """

    def adapt(self, batches: Iterator[DataBatch]) -> Iterator[Block]:
        # Convert batches to blocks.
        batches_to_blocks_adapter = BatchesToBlocksAdapter()
        # Delegate to blocks-to-blocks output adapter, converting a stream of unbounded
        # blocks to a stream of bounded blocks.
        # NOTE: This conversion is streaming, i.e. we're only converting one batch to a
        # block at a time before adding it to the underlying output buffer, we're not
        # converting all batches to unbounded blocks and then adding to the output
        # buffer.
        blocks_to_blocks_adapter = BlocksOutputAdapter(self._up_logical_op)
        yield from self._delegate(
            batches, batches_to_blocks_adapter, blocks_to_blocks_adapter
        )


class RowsOutputAdapter(OutputAdapter):
    """The output adapter for an upstream operator returning an iterator of rows,
    adapted to an iterator of blocks of a bounded size.
    """

    def adapt(self, rows: Iterator[Row]) -> Iterator[Block]:
        ctx = DatasetContext.get_current()
        yield from self._delegate(
            rows, RowsToBlocksOfSizeBytesAdapter(ctx.target_max_block_size)
        )


# --------------
# Operator Adapters
# --------------


class OpAdapter(Adapter):
    """An adapter between two operators.

    This adapter converts the outputs of the upstream operator to the expected input
    format of the downstream operator.
    """

    def __init__(
        self, up_logical_op: LogicalOperator, down_logical_op: LogicalOperator
    ):
        self._up_logical_op = up_logical_op
        self._down_logical_op = down_logical_op
        super().__init__()

    def from_ops(
        up_logical_op: LogicalOperator, down_logical_op: LogicalOperator
    ) -> "OpAdapter":
        """Create the appropriate operator adapter for the two provided operators."""
        # Map from (upstream data type, downstream data type) to the adapter that can
        # convert between said data types.
        OP_PAIR_DATA_TYPES_TO_ADAPTER_MAP: Dict[
            Tuple[DataType, DataType], OpAdapter
        ] = {
            (DataType.BLOCK, DataType.BLOCK): BlocksToBlocksOpAdapter,
            (DataType.BLOCK, DataType.BATCH): BlocksToBatchesOpAdapter,
            (DataType.BLOCK, DataType.ROW): BlocksToRowsOpAdapter,
            (DataType.BATCH, DataType.BLOCK): BatchesToBlocksOpAdapter,
            (DataType.BATCH, DataType.BATCH): BatchesToBatchesOpAdapter,
            (DataType.BATCH, DataType.ROW): BatchesToRowsOpAdapter,
            (DataType.ROW, DataType.BLOCK): RowsToBlocksOpAdapter,
            (DataType.ROW, DataType.BATCH): RowsToBatchesOpAdapter,
            (DataType.ROW, DataType.ROW): RowsToRowsOpAdapter,
        }
        assert type(up_logical_op) in OP_TO_DATA_TYPE_MAP
        assert type(down_logical_op) in OP_TO_DATA_TYPE_MAP
        up_data_type = OP_TO_DATA_TYPE_MAP[type(up_logical_op)]
        down_data_type = OP_TO_DATA_TYPE_MAP[type(down_logical_op)]
        adapter = OP_PAIR_DATA_TYPES_TO_ADAPTER_MAP[(up_data_type, down_data_type)]
        return adapter(up_logical_op, down_logical_op)

    @abstractmethod
    def adapt(self, data: Iterator[DataT]) -> Iterator[DataT]:
        raise NotImplementedError


class BlocksToBlocksOpAdapter(OpAdapter):
    """The adapter for an upstream operator returning an iterator of blocks and a
    downstream operator taking an iterator of blocks.
    """

    def adapt(self, blocks: Iterator[Block]) -> Iterator[Block]:
        for block in blocks:
            self._set_builder_for_data(block)
            yield block


class BatchesToBlocksOpAdapter(OpAdapter):
    """The adapter for an upstream operator returning an iterator of batches and a
    downstream operator taking an iterator of blocks.
    """

    def adapt(self, batches: Iterator[DataBatch]) -> Iterator[Block]:
        yield from self._delegate(batches, BatchesToBlocksAdapter())


class RowsToBlocksOpAdapter(OpAdapter):
    """The adapter for an upstream operator returning an iterator of rows and a
    downstream operator taking an iterator of blocks.
    """

    def adapt(self, rows: Iterator[Row]) -> Iterator[Block]:
        # TODO(Clark): Find a better fallback block/batch size than the target min block
        # size.
        ctx = DatasetContext.get_current()
        rows_to_blocks_adapter = RowsToBlocksOfSizeBytesAdapter(
            ctx.target_min_block_size
        )
        yield from self._delegate(rows, rows_to_blocks_adapter)


class BlocksToBatchesOpAdapter(OpAdapter):
    """The adapter for an upstream operator returning an iterator of blocks and a
    downstream operator taking an iterator of batches.
    """

    def adapt(self, blocks: Iterator[Block]) -> Iterator[DataBatch]:
        assert isinstance(self._down_logical_op, MapBatches)
        batch_size = self._down_logical_op._batch_size
        batch_format = self._down_logical_op._batch_format
        prefetch_batches = self._down_logical_op._prefetch_batches
        if isinstance(self._up_logical_op, Read):
            # Assume that ReadTask output is not pointing to data in the object store.
            # TODO(Clark): This is a strong assumption that might not be true for custom
            # datasources that serve as a connector to other in-cluster data.
            zero_copy_batch = True
        else:
            zero_copy_batch = self._down_logical_op._zero_copy_batch

        # TODO(Clark): Move all zero_copy_batch logic into this adapter.
        inner_adapter = BlocksToBatchesAdapter(
            batch_size, batch_format, prefetch_batches, zero_copy_batch
        )
        yield from self._delegate(blocks, inner_adapter)


class BlocksToRowsOpAdapter(OpAdapter):
    """The adapter for an upstream operator returning an iterator of blocks and a
    downstream operator taking an iterator of rows.
    """

    def adapt(self, blocks: Iterator[Block]) -> Iterator[Row]:
        yield from self._delegate(blocks, BlocksToRowsAdapter())


class BatchesToBatchesOpAdapter(OpAdapter):
    """The adapter for an upstream operator returning an iterator of batches and a
    downstream operator taking an iterator of batches.
    """

    def adapt(self, batches: Iterator[DataBatch]) -> Iterator[DataBatch]:
        assert isinstance(self._up_logical_op, MapBatches)
        assert isinstance(self._down_logical_op, MapBatches)

        batch_size = self._down_logical_op._batch_size
        batch_format = self._down_logical_op._batch_format
        prefetch_batches = self._down_logical_op._prefetch_batches
        if not self._up_logical_op._zero_copy_batch:
            # Upstream MapBatches already created batch copy, so the downstream
            # MapBatches can safely mutate its input batch without creating a copy.
            zero_copy_batch = True
            # TODO(Clark): Find way to propagate this downstream to enable this
            # optimization for downstream MapBatches.
        else:
            # Upstream MapBatches may pass through object store data buffers in its
            # output, so we respect the downstream MapBatches zero-copy request.
            zero_copy_batch = self._down_logical_op._zero_copy_batch

        # Delegate to batch->block and block->batch adapters.
        # 1. Convert batches to blocks.
        # NOTE: This will be expensive for boolean NumPy data, since we'll be
        # converting it to bit-packed Arrow boolean data and then back again,
        # resulting in 2 copies.
        # TODO(Clark): Optimize this for cases in which the batch --> block --> batch
        # path is expensive.
        # TODO(Clark): Take the minimum of the two batch sizes to minimize
        # rebatching cost.
        batches_to_blocks_adapter = BatchesToBlocksAdapter()
        # 2. Rebatch the blocks.
        # Delegate to blocks-to-blocks output adapter, converting a stream of unbounded
        # blocks to a stream of bounded blocks.
        # NOTE: This conversion is streaming, i.e. we're only converting one batch to a
        # block at a time before adding it to the underlying output buffer, we're not
        # converting all batches to unbounded blocks and then adding to the output
        # buffer.
        blocks_to_blocks_adapter = BlocksToBatchesAdapter(
            batch_size, batch_format, prefetch_batches, zero_copy_batch
        )
        yield from self._delegate(
            batches, batches_to_blocks_adapter, blocks_to_blocks_adapter
        )


class BatchesToRowsOpAdapter(OpAdapter):
    """The adapter for an upstream operator returning an iterator of batches and a
    downstream operator taking an iterator of rows.
    """

    def adapt(self, batches: Iterator[DataBatch]) -> Iterator[Row]:
        # Delegate to batches->blocks and blocks->rows.
        # 1. Convert batches to unbounded blocks.
        batches_to_blocks_adapter = BatchesToBlocksAdapter()
        # 2. Iterate through each block's rows.
        blocks_to_rows_adapter = BlocksToRowsAdapter()
        yield from self._delegate(
            batches, batches_to_blocks_adapter, blocks_to_rows_adapter
        )


class RowsToBatchesOpAdapter(OpAdapter):
    """The adapter for an upstream operator returning an iterator of rows and a
    downstream operator taking an iterator of batches.
    """

    def adapt(self, rows: Iterator[Row]) -> Iterator[DataBatch]:
        assert isinstance(self._down_logical_op, MapBatches)
        batch_size = self._down_logical_op._batch_size
        if batch_size is not None:
            # Convert rows to blocks of batch_size size.
            rows_to_blocks_adapter = RowsToBlocksOfSizeRowsAdapter(batch_size)
        else:
            # TODO(Clark): Find a better fallback block/batch size than the target min
            # block size.
            ctx = DatasetContext.get_current()
            rows_to_blocks_adapter = RowsToBlocksOfSizeBytesAdapter(
                ctx.target_min_block_size
            )

        batch_format = self._down_logical_op._batch_format
        prefetch_batches = self._down_logical_op._prefetch_batches
        # We're building blocks from rows, which always results in a copy.
        zero_copy_batch = True

        # Convert these batch-sized blocks to proper batches.
        blocks_to_batches_adapter = BlocksToBatchesAdapter(
            batch_size, batch_format, prefetch_batches, zero_copy_batch
        )
        yield from self._delegate(
            rows, rows_to_blocks_adapter, blocks_to_batches_adapter
        )


class RowsToRowsOpAdapter(OpAdapter):
    """The adapter for an upstream operator returning an iterator of rows and a
    downstream operator taking an iterator of rows.
    """

    def adapt(self, rows: Iterator[Row]) -> Iterator[Row]:
        for row in rows:
            self._set_builder_for_data(row)
            yield row


# -------------------------
# Utility (Shared) Adapters
# -------------------------


class BlocksToBatchesAdapter(Adapter):
    """
    Utility adapter for converting an iterator of blocks to an iterator of batches.
    """

    def __init__(self, batch_size, batch_format, prefetch_batches, zero_copy_batch):
        self._batch_size = batch_size
        self._batch_format = batch_format
        self._prefetch_batches = prefetch_batches
        self._zero_copy_batch = zero_copy_batch
        super().__init__()

    def adapt(self, blocks: Iterator[Block]) -> Iterator[DataBatch]:
        for block in batch_blocks(
            blocks,
            batch_size=self._batch_size,
            batch_format=self._batch_format,
            prefetch_batches=self._prefetch_batches,
            ensure_copy=not self._zero_copy_batch and self._batch_size is not None,
            metrics_collector=self._metrics_collector,
        ):
            self._set_builder_for_data(block)
            yield block


class BlocksToRowsAdapter(Adapter):
    """Utility adapter for converting an iterator of blocks to an iterator of rows."""

    def adapt(self, blocks: Iterator[Block]) -> Iterator[Row]:
        for block in blocks:
            self._set_builder_for_data(block)
            yield from BlockAccessor.for_block(block).iter_rows()


class BatchesToBlocksAdapter(Adapter):
    """Utility adapter for converting an iterator of batches to an iterator of unbounded
    blocks.
    """

    def adapt(self, batches: Iterator[DataBatch]) -> Iterator[Block]:
        for batch in batches:
            self._set_builder_for_data(batch)
            # TODO(Clark): Add tracking for copies due to batch -> block conversion,
            # e.g. for boolean NumPy ndarrays.
            yield BlockAccessor.batch_to_block(batch)


class BlocksToBlocksAdapter(Adapter):
    """Utility adapter for converting an iterator of blocks to an iterator of blocks of
    a bounded size.
    """

    def __init__(self, target_max_block_size: int):
        self._target_max_block_size = target_max_block_size
        super().__init__()

    def adapt(self, blocks: Iterator[Block]) -> Iterator[Block]:
        # TODO(Clark): Move other for-output castings/conversions into this adapter,
        # such as tensor column casting to our tensor extension type.
        output_buffer = BlockOutputBuffer(self._target_max_block_size)
        for block in blocks:
            self._set_builder_for_data(block)
            output_buffer.add_block(block)
            if output_buffer.has_next():
                yield output_buffer.next()
        # Yield leftovers.
        output_buffer.finalize()
        if output_buffer.has_next():
            yield output_buffer.next()
        if self._metrics_collector is not None:
            self._metrics_collector.record_metrics(output_buffer.get_metrics())


class RowsToBlocksOfSizeBytesAdapter(Adapter):
    """Utility adapter for converting an iterator of rows to an iterator of blocks of a
    bounded size (in bytes).
    """

    def __init__(self, target_max_block_size: int):
        self._target_max_block_size = target_max_block_size
        super().__init__()

    def adapt(self, rows: Iterator[Row]) -> Iterator[Block]:
        output_buffer = BlockOutputBuffer(self._target_max_block_size)
        for row in rows:
            self._set_builder_for_data(row)
            output_buffer.add(row)
            if output_buffer.has_next():
                yield output_buffer.next()
        # Yield leftovers.
        output_buffer.finalize()
        if output_buffer.has_next():
            yield output_buffer.next()
        if self._metrics_collector is not None:
            self._metrics_collector.record_metrics(output_buffer.get_metrics())


class RowsToBlocksOfSizeRowsAdapter(Adapter):
    """Utility adapter for converting an iterator of rows to an iterator of blocks of a
    bounded number of rows.
    """

    def __init__(self, target_num_rows: int):
        self._target_num_rows = target_num_rows
        super().__init__()

    def adapt(self, rows: Iterator[Row]) -> Iterator[Block]:
        while True:
            builder = DelegatingBlockBuilder()
            try:
                for _ in range(self._target_num_rows):
                    row = next(rows)
                    self._set_builder_for_data(row)
                    if isinstance(row, TableRow):
                        # If row is a zero-copy slice view of a single row in the table,
                        # treat row as a single-row block; concatenating these
                        # single-row blocks should be faster than our row accumulation
                        # + concatenate.
                        # TODO(Clark): This might not be a clear win in all cases,
                        # should optimize this further.
                        builder.add_block(row._row)
                    else:
                        builder.add(row)
            except StopIteration:
                break
            finally:
                if builder.num_rows() > 0:
                    yield builder.build()
                    if self._metrics_collector is not None:
                        self._metrics_collector.record_metrics(builder.get_metrics())
