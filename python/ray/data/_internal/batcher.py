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
        # If no batch size, short-circuit.
        if self._batch_size is None:
            assert len(self._buffer) == 1
            block = self._buffer[0]
            if self._ensure_copy:
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
        batch = output.build()
        if self._ensure_copy:
            # Need to ensure that the batch is a fresh copy.
            batch = BlockAccessor.for_block(batch)
            # TOOD(Clark): This copy will often be unnecessary, e.g. for pandas
            # DataFrame batches that have required concatenation to construct, which
            # always requires a copy. We should elide this copy in those cases.
            batch = batch.slice(0, batch.num_rows(), copy=True)
        return batch


class ShufflingBatcher(BatcherInterface):
    """Chunks blocks into shuffled batches, using a local in-memory shuffle buffer."""

    # Implementation Note:
    #
    # This shuffling batcher lazily builds a shuffle buffer from added blocks, and once
    # a batch is requested via .next_batch(), it concatenates the blocks into a concrete
    # shuffle buffer, generates random shuffle indices, and starts returning shuffled
    # batches.
    #
    # Adding of more blocks can be intermixed with retrieving batches, but it should be
    # noted that we can end up performing two expensive operations on each retrieval:
    #  1. Build added blocks into a concrete shuffle buffer.
    #  2. Generate random shuffle indices.
    # Note that (1) and (2) only happen when new blocks are added, upon the next
    # retrieval. I.e., if no new blocks have been added since the last batch retrieval,
    # and there are still batches in the existing concrete shuffle buffer to be yielded,
    # then each batch retrieval will only involve slicing the batch out of the concrete
    # shuffle buffer.
    #
    # Similarly, adding blocks is very cheap. Each added block will be appended to a
    # list, with concatenation of the underlying data delayed until the next batch
    # retrieval.
    #
    # Since (1) runs of block additions are cheap, and (2) runs of batch retrievals are
    # cheap, callers of ShufflingBatcher are encouraged to add as many blocks as
    # possible (up to the shuffle buffer capacity), followed by retrieving as many
    # batches as possible (down to the shuffle buffer minimum size), in such contiguous
    # runs.

    def __init__(
        self,
        batch_size: Optional[int],
        shuffle_buffer_min_size: int,
        shuffle_seed: Optional[int] = None,
    ):
        """Constructs a random-shuffling block batcher.

        Args:
            batch_size: Record batch size.
            shuffle_buffer_min_size: Minimum number of rows that must be in the local
                in-memory shuffle buffer in order to yield a batch. When there are no
                more rows to be added to the buffer, the number of rows in the buffer
                *will* decrease below this value while yielding the remaining batches,
                and the final batch may have less than ``batch_size`` rows. Increasing
                this will improve the randomness of the shuffle but may increase the
                latency to the first batch.
            shuffle_seed: The seed to use for the local random shuffle.
        """
        if batch_size is None:
            raise ValueError("Must specify a batch_size if using a local shuffle.")
        self._batch_size = batch_size
        if shuffle_buffer_min_size < batch_size:
            # Round it up internally to `batch_size` since our algorithm requires it.
            # This is harmless since it only offers extra randomization.
            shuffle_buffer_min_size = batch_size
        self._buffer_capacity = max(
            2 * shuffle_buffer_min_size,
            shuffle_buffer_min_size + batch_size,
        )
        self._buffer_min_size = shuffle_buffer_min_size
        self._builder = DelegatingBlockBuilder()
        self._shuffle_buffer: Block = None
        self._shuffle_indices: List[int] = None
        self._batch_head = 0
        self._done_adding = False

        if shuffle_seed is not None:
            random.seed(shuffle_seed)

    def add(self, block: Block):
        """Add a block to the shuffle buffer.

        Note empty block is not added to buffer.

        Args:
            block: Block to add to the shuffle buffer.
        """
        if BlockAccessor.for_block(block).num_rows() > 0:
            assert self.can_add(block)
            self._builder.add_block(block)

    def can_add(self, block: Block) -> bool:
        """Whether the block can be added to the shuffle buffer.

        This does not take the to-be-added block size into account when checking the
        buffer size vs. buffer capacity, since we need to support large outlier blocks
        and have to guard against min buffer size liveness issues.
        """
        return self._buffer_size() <= self._buffer_capacity and not self._done_adding

    def done_adding(self) -> bool:
        """Indicate to the batcher that no more blocks will be added to the batcher.

        No more blocks should be added to the batcher after calling this.
        """
        self._done_adding = True

    def has_any(self) -> bool:
        """Whether this batcher has any data."""
        return self._buffer_size() > 0

    def has_batch(self) -> bool:
        """Whether this batcher has any batches."""
        buffer_size = self._buffer_size()
        # If still adding blocks, ensure that removing a batch wouldn't cause the
        # shuffle buffer to dip beneath its configured minimum size.
        return buffer_size - self._batch_size >= self._buffer_min_size or (
            self._done_adding and buffer_size >= self._batch_size
        )

    def _buffer_size(self) -> int:
        """Return shuffle buffer size."""
        buffer_size = self._builder.num_rows()
        if self._shuffle_buffer is not None:
            # Include the size of the concrete (materialized) shuffle buffer, adjusting
            # for the batch head position, which also serves as a counter of the number
            # of already-yielded rows from the current concrete shuffle buffer.
            buffer_size += (
                BlockAccessor.for_block(self._shuffle_buffer).num_rows()
                - self._batch_head
            )
        return buffer_size

    def next_batch(self) -> Block:
        """Get the next shuffled batch from the shuffle buffer.

        Returns:
            A batch represented as a Block.
        """
        assert self.has_batch() or (self._done_adding and self.has_any())
        # Add rows in the builder to the shuffle buffer.
        if self._builder.num_rows() > 0:
            if self._shuffle_buffer is not None:
                if self._batch_head > 0:
                    # Compact the materialized shuffle buffer.
                    # TODO(Clark): If alternating between adding blocks and fetching
                    # shuffled batches, this aggressive compaction could be inefficient.
                    self._shuffle_buffer = BlockAccessor.for_block(
                        self._shuffle_buffer
                    ).take(self._shuffle_indices[self._batch_head :])
                # Add the unyielded rows from the existing shuffle buffer.
                self._builder.add_block(self._shuffle_buffer)
            # Build the new shuffle buffer.
            self._shuffle_buffer = self._builder.build()
            # Reset the builder.
            self._builder = DelegatingBlockBuilder()
            # Invalidate the shuffle indices.
            self._shuffle_indices = None
            self._batch_head = 0

        assert self._shuffle_buffer is not None
        buffer_size = BlockAccessor.for_block(self._shuffle_buffer).num_rows()
        # Truncate the batch to the buffer size, if necessary.
        batch_size = min(self._batch_size, buffer_size)

        if self._shuffle_indices is None:
            # Need to generate new shuffle indices.
            self._shuffle_indices = list(range(buffer_size))
            random.shuffle(self._shuffle_indices)

        # Get the shuffle indices for this batch.
        batch_indices = self._shuffle_indices[
            self._batch_head : self._batch_head + batch_size
        ]
        self._batch_head += batch_size
        # Yield the shuffled batch.
        return BlockAccessor.for_block(self._shuffle_buffer).take(batch_indices)
