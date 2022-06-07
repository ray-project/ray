from typing import Optional

from ray.data.block import Block, BlockAccessor
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder


class Batcher:
    """Chunks blocks into batches.

    Implementation Note: When there are multiple batches per block,
    this batcher will slice off and return each batch and add the
    remaining block back to the buffer instead of optimally slicing and
    returning all batches from the block at once. This will result in
    extra (and nested) block slicing. However, since slices are
    zero-copy views, we sacrifice what should be a small performance
    hit for better readability.
    """

    def __init__(self, batch_size: Optional[int]):
        self._batch_size = batch_size
        self._buffer = []

    def add(self, block: Block):
        """Add a block to the block buffer.

        Args:
            block: Block to add to the block buffer.
        """
        self._buffer.append(block)

    def has_batch(self) -> bool:
        """Whether this Batcher has any full batches."""
        return self._buffer and (
            self._batch_size is None
            or sum(BlockAccessor.for_block(b).num_rows() for b in self._buffer)
            >= self._batch_size
        )

    def has_any(self) -> bool:
        """Whether this Batcher has any data."""
        return any(BlockAccessor.for_block(b).num_rows() > 0 for b in self._buffer)

    def next_batch(self) -> Block:
        """Get the next batch from the block buffer.

        Returns:
            A batch represented as a Block.
        """
        # If no batch size, short-circuit.
        if self._batch_size is None:
            assert len(self._buffer) == 1
            block = self._buffer[0]
            self._buffer = []
            return block
        output = DelegatingBlockBuilder()
        leftover = []
        needed = self._batch_size
        for block in self._buffer:
            accessor = BlockAccessor.for_block(block)
            if needed <= 0:
                # We already have a full batch, so add this block to
                # the leftovers.
                leftover.append(block)
            elif accessor.num_rows() <= needed:
                # We need this entire block to fill out a batch.
                # We need to call `accessor.slice()` to ensure
                # the subsequent block's type are the same.
                output.add_block(accessor.slice(0, accessor.num_rows(), copy=False))
                needed -= accessor.num_rows()
            else:
                # We only need part of the block to fill out a batch.
                output.add_block(accessor.slice(0, needed, copy=False))
                # Add the rest of the block to the leftovers.
                leftover.append(accessor.slice(needed, accessor.num_rows(), copy=False))
                needed = 0

        # Move the leftovers into the block buffer so they're the first
        # blocks consumed on the next batch extraction.
        self._buffer = leftover
        return output.build()
