from typing import Callable, Iterator

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data.block import Block, BlockAccessor, RowUDF
from ray.data.context import DatasetContext


def generate_map_rows_fn() -> Callable[
    [Iterator[Block], TaskContext, RowUDF], Iterator[Block]
]:
    """Generate function to apply the UDF to each record of blocks."""

    context = DatasetContext.get_current()

    def fn(
        blocks: Iterator[Block], ctx: TaskContext, row_fn: RowUDF
    ) -> Iterator[Block]:
        DatasetContext._set_current(context)
        output_buffer = BlockOutputBuffer(None, context.target_max_block_size)
        for block in blocks:
            block = BlockAccessor.for_block(block)
            for row in block.iter_rows():
                output_buffer.add(row_fn(row))
                if output_buffer.has_next():
                    yield output_buffer.next()
        output_buffer.finalize()
        if output_buffer.has_next():
            yield output_buffer.next()

    return fn
