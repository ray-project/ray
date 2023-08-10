import collections
from typing import Callable, Iterator

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.util import _truncated_repr
from ray.data.block import Block, BlockAccessor, UserDefinedFunction
from ray.data.context import DataContext


def generate_map_rows_fn() -> (
    Callable[[Iterator[Block], TaskContext, UserDefinedFunction], Iterator[Block]]
):
    """Generate function to apply the UDF to each record of blocks."""

    context = DataContext.get_current()

    def fn(
        blocks: Iterator[Block], ctx: TaskContext, row_fn: UserDefinedFunction
    ) -> Iterator[Block]:
        DataContext._set_current(context)
        output_buffer = BlockOutputBuffer(None, context.target_max_block_size)
        for block in blocks:
            block = BlockAccessor.for_block(block)
            for row in block.iter_rows(public_row_format=True):
                item = row_fn(row)
                if not isinstance(item, collections.abc.Mapping):
                    raise ValueError(
                        f"Error validating {_truncated_repr(item)}: "
                        "Standalone Python objects are not "
                        "allowed in Ray 2.5. To return Python objects from map(), "
                        "wrap them in a dict, e.g., "
                        "return `{'item': item}` instead of just `item`."
                    )
                output_buffer.add(item)
                if output_buffer.has_next():
                    yield output_buffer.next()
        output_buffer.finalize()
        if output_buffer.has_next():
            yield output_buffer.next()

    return fn
