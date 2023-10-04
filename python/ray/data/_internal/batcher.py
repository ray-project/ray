from typing import Optional

from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block, BlockAccessor

# pyarrow.Table.slice is slow when the table has many chunks
# so we combine chunks into a single one to make slice faster
# with the cost of an extra copy.
# See https://github.com/ray-project/ray/issues/31108 for more details.
# TODO(jjyao): remove this once
# https://github.com/apache/arrow/issues/35126 is resolved.
MIN_NUM_CHUNKS_TO_TRIGGER_COMBINE_CHUNKS = 2

# Delay compaction until the shuffle buffer has reached this ratio over the min
# shuffle buffer size. Setting this to 1 minimizes memory usage, at the cost of
# frequent compactions. Setting this to higher values increases memory usage but
# reduces compaction frequency.
SHUFFLE_BUFFER_COMPACTION_RATIO = 1.5


class BatcherInterface:
    def add(self, block: Block):
        """Add a block to the block buffer.

        Args:
            block: Block to add to the block buffer.
        """
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
            self._buffer.append(block)
            self._buffer_size += BlockAccessor.for_block(block).num_rows()

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
                if (
                    isinstance(accessor, ArrowBlockAccessor)
                    and block.num_columns > 0
                    and block.column(0).num_chunks
                    >= MIN_NUM_CHUNKS_TO_TRIGGER_COMBINE_CHUNKS
                ):
                    accessor = BlockAccessor.for_block(
                        transform_pyarrow.combine_chunks(block)
                    )
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


class ShufflingBatcher(BatcherInterface):
    """Chunks blocks into shuffled batches, using a local in-memory shuffle buffer."""

    # Implementation Note:
    #
    # This shuffling batcher lazily builds a shuffle buffer from added blocks, and once
    # a batch is requested via .next_batch(), it concatenates the blocks into a concrete
    # shuffle buffer and randomly shuffles the entire buffer.
    #
    # Adding of more blocks can be intermixed with retrieving batches, but it should be
    # noted that we can end up performing two expensive operations on each retrieval:
    #  1. Build added blocks into a concrete shuffle buffer.
    #  2. Shuffling the entire buffer.
    # To amortize the overhead of this process, we only shuffle the blocks after a
    # delay designated by SHUFFLE_BUFFER_COMPACTION_RATIO.
    #
    # Similarly, adding blocks is very cheap. Each added block will be appended to a
    # list, with concatenation of the underlying data delayed until the next batch
    # compaction.

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
        self._shuffle_seed = shuffle_seed
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
        self._batch_head = 0
        self._done_adding = False

    def add(self, block: Block):
        """Add a block to the shuffle buffer.

        Note empty block is not added to buffer.

        Args:
            block: Block to add to the shuffle buffer.
        """
        if BlockAccessor.for_block(block).num_rows() > 0:
            self._builder.add_block(block)

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

        if not self._done_adding:
            # Delay pulling of batches until the buffer is large enough in order to
            # amortize compaction overhead.
            return self._materialized_buffer_size() >= self._buffer_min_size or (
                buffer_size - self._batch_size
                >= self._buffer_min_size * SHUFFLE_BUFFER_COMPACTION_RATIO
            )
        else:
            return buffer_size >= self._batch_size

    def _buffer_size(self) -> int:
        """Return shuffle buffer size."""
        buffer_size = self._builder.num_rows()
        buffer_size += self._materialized_buffer_size()
        return buffer_size

    def _materialized_buffer_size(self) -> int:
        """Return materialized (compacted portion of) shuffle buffer size."""
        if self._shuffle_buffer is None:
            return 0
        # The size of the concrete (materialized) shuffle buffer, adjusting
        # for the batch head position, which also serves as a counter of the number
        # of already-yielded rows from the current concrete shuffle buffer.
        return max(
            0,
            BlockAccessor.for_block(self._shuffle_buffer).num_rows() - self._batch_head,
        )

    def next_batch(self) -> Block:
        """Get the next shuffled batch from the shuffle buffer.

        Returns:
            A batch represented as a Block.
        """
        assert self.has_batch() or (self._done_adding and self.has_any())
        # Add rows in the builder to the shuffle buffer. Note that we delay compaction
        # as much as possible to amortize the concatenation overhead. Compaction is
        # only necessary when the materialized buffer size falls below the min size.
        if self._builder.num_rows() > 0 and (
            self._done_adding
            or self._materialized_buffer_size() <= self._buffer_min_size
        ):
            if self._shuffle_buffer is not None:
                if self._batch_head > 0:
                    # Compact the materialized shuffle buffer.
                    block = BlockAccessor.for_block(self._shuffle_buffer)
                    self._shuffle_buffer = block.slice(
                        self._batch_head, block.num_rows()
                    )
                # Add the unyielded rows from the existing shuffle buffer.
                self._builder.add_block(self._shuffle_buffer)
            # Build the new shuffle buffer.
            self._shuffle_buffer = self._builder.build()
            self._shuffle_buffer = BlockAccessor.for_block(
                self._shuffle_buffer
            ).random_shuffle(self._shuffle_seed)
            if self._shuffle_seed is not None:
                self._shuffle_seed += 1
            if (
                isinstance(
                    BlockAccessor.for_block(self._shuffle_buffer), ArrowBlockAccessor
                )
                and self._shuffle_buffer.num_columns > 0
                and self._shuffle_buffer.column(0).num_chunks
                >= MIN_NUM_CHUNKS_TO_TRIGGER_COMBINE_CHUNKS
            ):
                self._shuffle_buffer = transform_pyarrow.combine_chunks(
                    self._shuffle_buffer
                )
            # Reset the builder.
            self._builder = DelegatingBlockBuilder()
            self._batch_head = 0

        assert self._shuffle_buffer is not None
        buffer_size = BlockAccessor.for_block(self._shuffle_buffer).num_rows()
        # Truncate the batch to the buffer size, if necessary.
        batch_size = min(self._batch_size, buffer_size)
        slice_start = self._batch_head
        self._batch_head += batch_size
        # Yield the shuffled batch.
        return BlockAccessor.for_block(self._shuffle_buffer).slice(
            slice_start, self._batch_head
        )
