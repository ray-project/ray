from typing import Iterable

from ray.data.block import Block
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def iter_sliced_blocks(
    blocks: Iterable[Block], per_block_limit: int
) -> Iterable[Block]:
    """Iterate over blocks, slicing them to meet the per-block limit."""
    rows_read = 0
    for block in blocks:
        if rows_read >= per_block_limit:
            break

        from ray.data.block import BlockAccessor

        accessor = BlockAccessor.for_block(block)
        block_rows = accessor.num_rows()

        if rows_read + block_rows <= per_block_limit:
            yield block
            rows_read += block_rows
        else:
            # Slice the block to meet the limit exactly
            remaining_rows = per_block_limit - rows_read
            sliced_block = accessor.slice(0, remaining_rows, copy=True)
            yield sliced_block
            break
