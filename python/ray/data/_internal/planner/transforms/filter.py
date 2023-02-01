from typing import Callable, Iterator

from ray.data.block import Block, RowUDF, T as Row
from ray.data.context import DatasetContext
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.planner.transforms.adapters import (
    BlocksToRowsAdapter,
    RowsToBlocksOfSizeBytesAdapter,
)


def generate_filter_transform(
    fn: Callable[[Row], bool],
) -> Callable[[Iterator[Row]], Iterator[Row]]:
    """Generate function to apply the UDF to each record of blocks,
    and filter out records that do not satisfy the given predicate.
    """

    def fn_(rows: Iterator[Row], ctx: TaskContext) -> Iterator[Row]:
        for row in rows:
            if fn(row):
                yield row

    return fn_


def generate_filter_legacy_transform() -> Callable[
    [Iterator[Block], TaskContext, RowUDF], Iterator[Block]
]:
    """Generate function to apply the UDF to each record of blocks,
    and filter out records that do not satisfy the given predicate.
    """
    blocks_to_rows_adapter = BlocksToRowsAdapter()

    target_max_block_size = DatasetContext.get_current().target_max_block_size
    buffer_rows_adapter = RowsToBlocksOfSizeBytesAdapter(target_max_block_size)

    def fn(
        blocks: Iterator[Block], ctx: TaskContext, row_fn: RowUDF
    ) -> Iterator[Block]:
        # Generate the filter transform.
        transform = generate_filter_transform(row_fn)

        rows = blocks_to_rows_adapter.adapt(blocks)
        rows = transform(rows, ctx)
        out = buffer_rows_adapter.adapt(rows)
        did_yield = False
        for datum_ in out:
            did_yield = True
            yield datum_
        if not did_yield:
            # Build an empty block from the adapter before the filter.
            for adapter in blocks_to_rows_adapter:
                builder = adapter.builder()
                if builder is not None:
                    yield builder.build()
                    break
            else:
                # If no data seen, fall back to delegating block builder fallback.
                yield DelegatingBlockBuilder().build()

    return fn
