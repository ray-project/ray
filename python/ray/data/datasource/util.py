from typing import Iterable

from ray.data.block import Block


def _iter_sliced_blocks(
    blocks: Iterable[Block], per_task_row_limit: int
) -> Iterable[Block]:
    """Iterate over blocks, accumulating rows up to the per-task row limit."""
    rows_read = 0
    for block in blocks:
        if rows_read >= per_task_row_limit:
            break

        from ray.data.block import BlockAccessor

        accessor = BlockAccessor.for_block(block)
        block_rows = accessor.num_rows()

        if rows_read + block_rows <= per_task_row_limit:
            yield block
            rows_read += block_rows
        else:
            # Slice the block to meet the limit exactly
            remaining_rows = per_task_row_limit - rows_read
            sliced_block = accessor.slice(0, remaining_rows, copy=True)
            yield sliced_block
            break
