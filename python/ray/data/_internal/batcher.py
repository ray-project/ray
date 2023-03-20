import random
from typing import Optional, List

from ray.data.block import Block, BlockAccessor
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder


class BatcherInterface:
    def add(self, block: Block):
        """Add a block to the block buffer.

        Args:
            block: Block to add to the block buffer.
        """
        raise NotImplementedError()

    def can_add(self, block: Block) -> bool:
        """Whether the block can be added to the buffer."""
        raise NotImplementedError()

    def done_adding(self) -> bool:
        """Indicate to the batcher that no more blocks will be added to the buffer."""
        raise NotImplementedError()

    def has_batch(self) -> bool:
        """Whether this Batcher has any full batches."""
        raise NotImplementedError()

    def has_any(self) -> bool:
        """Whether this Batcher has any data."""
        raise NotImplementedError()

    def next_batch(self) -> Block:
        """Get the next batch from the block buffer.

        Returns:
            A batch represented as a Block.
        """
        raise NotImplementedError()


class Batcher(BatcherInterface):
    """Chunks blocks into batches."""

    # Implementation Note: When there are multiple batches per block, this batcher will
    # slice off and return each batch and add the remaining block back to the buffer
    # instead of optimally slicing and returning all batches from the block at once.
    # This will result in extra (and nested) block slicing. However, since slices are
    # zero-copy views, we sacrifice what should be a small performance hit for better
    # readability.

    def __init__(self, batch_size: Optional[int], ensure_copy: bool = False):
        """
        Construct a batcher that yields batches of batch_sizes rows.

        Args:
            batch_size: The size of batches to yield.
            ensure_copy: Whether batches are always copied from the underlying base
                blocks (not zero-copy views).
        """
        self._batch_size = batch_size
        self._buffer = []
        self._buffer_size = 0
        self._done_adding = False
        self._ensure_copy = ensure_copy

    def add(self, block: Block):
        """Add a block to the block buffer.

        Note empty block is not added to buffer.

        Args:
            block: Block to add to the block buffer.
        """
        if BlockAccessor.for_block(block).num_rows() > 0:
            assert self.can_add(block)
            self._buffer.append(block)
            self._buffer_size += BlockAccessor.for_block(block).num_rows()

    def can_add(self, block: Block) -> bool:
        """Whether the block can be added to the buffer."""
        return not self._done_adding

    def done_adding(self) -> bool:
        """Indicate to the batcher that no more blocks will be added to the batcher."""
        self._done_adding = True

    def has_batch(self) -> bool:
        """Whether this Batcher has any full batches."""
        return self.has_any() and (
            self._batch_size is None or self._buffer_size >= self._batch_size
        )

    def has_any(self) -> bool:
        """Whether this Batcher has any data."""
        return self._buffer_size > 0

    def next_batch(self) -> Block:
        """Get the next batch from the block buffer.

        Returns:
            A batch represented as a Block.
        """
        assert self.has_batch() or (self._done_adding and self.has_any())
        needs_copy = self._ensure_copy
        # If no batch size, short-circuit.
        if self._batch_size is None:
            assert len(self._buffer) == 1
            block = self._buffer[0]
            if needs_copy:
                # Copy block if needing to ensure fresh batch copy.
                block = BlockAccessor.for_block(block)
                block = block.slice(0, block.num_rows(), copy=True)
            self._buffer = []
            self._buffer_size = 0
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
        self._buffer_size -= self._batch_size
        needs_copy = needs_copy and not output.will_build_yield_copy()
        batch = output.build()
        if needs_copy:
            # Need to ensure that the batch is a fresh copy.
            batch = BlockAccessor.for_block(batch)
            batch = batch.slice(0, batch.num_rows(), copy=True)
        return batch