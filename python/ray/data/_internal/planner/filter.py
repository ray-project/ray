from typing import Callable, Iterator

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor, UserDefinedFunction
from ray.data.context import DataContext


def generate_filter_fn() -> Callable[
    [Iterator[Block], TaskContext, UserDefinedFunction], Iterator[Block]
]:
    """Generate function to apply the UDF to each record of blocks,
    and filter out records that do not satisfy the given predicate.
    """

    context = DataContext.get_current()

    def fn(
        blocks: Iterator[Block], ctx: TaskContext, row_fn: UserDefinedFunction
    ) -> Iterator[Block]:
        DataContext._set_current(context)
        for block in blocks:
            block = BlockAccessor.for_block(block)
            builder = block.builder()
            for row in block.iter_rows(public_row_format=True):
                if row_fn(row):
                    builder.add(row)
            # NOTE: this yields an empty block if all rows are filtered out.
            # This causes different behavior between filter and other map-like
            # functions. We should revisit and try to get rid of this logic.
            yield builder.build()

    return fn
