from typing import Iterator

from ray.data._internal.execution.interfaces import TaskContext, TransformFn
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data.block import Block, BlockAccessor, RowUDF
from ray.data.context import DatasetContext


def generate_map_rows_fn() -> TransformFn:
    """Generate function to apply the UDF to each record of blocks."""

    context = DatasetContext.get_current()

    def fn(
        blocks: Iterator[Block], ctx: TaskContext, row_fn: RowUDF
    ) -> Iterator[Block]:
        DatasetContext._set_current(context)
        for block in blocks:
            output_buffer = BlockOutputBuffer(None, context.target_max_block_size)
            block = BlockAccessor.for_block(block)
            for row in block.iter_rows():
                output_buffer.add(row_fn(row))
                if output_buffer.has_next():
                    yield output_buffer.next()
            output_buffer.finalize()
            if output_buffer.has_next():
                yield output_buffer.next()

    return fn
