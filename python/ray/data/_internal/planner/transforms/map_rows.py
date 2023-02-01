from typing import Callable, Iterator


from ray.data.block import Block, RowUDF, T as Row
from ray.data.context import DatasetContext
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.planner.transforms.adapters import (
    BlocksToRowsAdapter,
    RowsToBlocksOfSizeBytesAdapter,
)


def generate_map_rows_transform(fn: RowUDF) -> Callable[[Iterator[Row]], Iterator[Row]]:
    """Generate function to apply the UDF to each record of blocks."""

    def fn_(rows: Iterator[Row], ctx: TaskContext) -> Iterator[Row]:
        for row in rows:
            yield fn(row)

    return fn_


def generate_map_rows_legacy_transform() -> Callable[
    [Iterator[Block], TaskContext, RowUDF], Iterator[Block]
]:
    """Generate function to apply the UDF to each record of blocks."""
    blocks_to_rows_adapter = BlocksToRowsAdapter()

    target_max_block_size = DatasetContext.get_current().target_max_block_size
    buffer_rows_adapter = RowsToBlocksOfSizeBytesAdapter(target_max_block_size)

    def fn(
        blocks: Iterator[Block], ctx: TaskContext, row_fn: RowUDF
    ) -> Iterator[Block]:
        # Generate the map transform.
        transform = generate_map_rows_transform(row_fn)

        rows = blocks_to_rows_adapter.adapt(blocks)
        rows = transform(rows, ctx)
        yield from buffer_rows_adapter.adapt(rows)

    return fn
