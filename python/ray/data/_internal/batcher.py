import warnings
from typing import Optional

import numpy as np

from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.arrow_ops.transform_pyarrow import try_combine_chunked_columns
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.util import memory_string
from ray.data._internal.random_config import RandomSeedConfig
from ray.data._internal.util import get_total_obj_store_mem_on_node
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.util import log_once

# Delay compaction until the shuffle buffer has reached this ratio over the min
# shuffle buffer size. Setting this to 1 minimizes memory usage, at the cost of
# frequent compactions. Setting this to higher values increases memory usage but
# reduces compaction frequency.
SHUFFLE_BUFFER_COMPACTION_RATIO = 1.5

# Ratio of remaining compacted rows to shuffle_buffer_min_size at which
# compaction (and re-shuffling of indices) is triggered. Experiments show 0.5
# is a good trade-off between throughput and randomness.
SHUFFLE_BUFFER_COMPACTION_THRESHOLD = 0.5


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
                output.add_block(accessor.to_block())
                needed -= accessor.num_rows()
            else:
                # Try de-fragmenting table in case its columns
                # have too many chunks (potentially hindering performance of
                # subsequent slicing operation)
                if isinstance(accessor, ArrowBlockAccessor):
                    accessor = BlockAccessor.for_block(
                        transform_pyarrow.try_combine_chunked_columns(
                            block, min_chunks_to_combine=1
                        )
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
    """Chunks blocks into shuffled batches, using a local in-memory shuffle buffer.

    Uses an **incremental index** approach: on each compaction a permutation
    array is generated over the buffer rows, and each ``next_batch()`` call
    gathers a small slice of that permutation via ``take``.

    Properties of this approach:

    * **Memory-efficient** -- the data buffer is kept as-is; only a
      lightweight int64 index array is allocated on top.
    * **Smooth per-batch latency** -- each ``take`` operates on a small
      slice of indices, so per-batch work is short and uniform, making it
      easy to hide behind prefetch threads.

    Example with ``batch_size=3`` and a 9-row buffer::

        buffer:  [A, B, C, D, E, F, G, H, I]
        indices: [4, 7, 1, 0, 8, 3, 6, 2, 5]   # random permutation

        next_batch() -> take([4, 7, 1]) -> [E, H, B]   # batch_head 0 -> 3
        next_batch() -> take([0, 8, 3]) -> [A, I, D]   # batch_head 3 -> 6
        next_batch() -> take([6, 2, 5]) -> [G, C, F]   # batch_head 6 -> 9
    """

    def __init__(
        self,
        batch_size: Optional[int],
        shuffle_buffer_min_size: int,
        shuffle_seed: RandomSeedConfig | None = None,
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
            shuffle_seed: The seed configuration for the local random shuffle.
                Use :meth:`RandomSeedConfig.create_with_split_index` to create
                a config with the split index set for multi-worker scenarios.
        """
        if batch_size is None:
            raise ValueError("Must specify a batch_size if using a local shuffle.")
        self._batch_size = batch_size
        self._shuffle_seed = shuffle_seed
        self._compaction_idx = 0
        if shuffle_buffer_min_size < batch_size:
            # Round it up internally to `batch_size` since our algorithm requires it.
            # This is harmless since it only offers extra randomization.
            shuffle_buffer_min_size = batch_size
        self._shuffle_buffer_min_size = shuffle_buffer_min_size

        self._min_rows_to_yield_batch = max(
            1, int(shuffle_buffer_min_size * SHUFFLE_BUFFER_COMPACTION_THRESHOLD)
        )
        self._min_rows_to_trigger_compaction = int(
            shuffle_buffer_min_size * SHUFFLE_BUFFER_COMPACTION_RATIO
        )
        self._builder = DelegatingBlockBuilder()
        self._shuffle_buffer: Block = None
        self._shuffled_indices: Optional[np.ndarray] = None
        self._batch_head = 0
        self._done_adding = False

        self._total_object_store_nbytes = get_total_obj_store_mem_on_node()
        self._total_num_rows_added = 0
        self._total_nbytes_added = 0

    def add(self, block: Block):
        """Add a block to the shuffle buffer.

        Note empty block is not added to buffer.

        Args:
            block: Block to add to the shuffle buffer.
        """
        # Because Arrow tables are memory mapped, blocks in the builder reside in object
        # store memory and not local heap memory. So, if you specify a large buffer size
        # and there isn't enough object store memory on the node, you encounter
        # spilling.
        if (
            self._estimated_min_nbytes_in_buffers is not None
            and self._estimated_min_nbytes_in_buffers > self._total_object_store_nbytes
            and log_once("shuffle_buffer_mem_warning")
        ):
            warnings.warn(
                "The node you're iterating on has "
                f"{memory_string(self._total_object_store_nbytes)} object "
                "store memory, but the shuffle buffer is estimated to use "
                f"{memory_string(self._estimated_min_nbytes_in_buffers)}. If you don't "
                f"decrease the shuffle buffer size from "
                f"{self._shuffle_buffer_min_size} rows, you might encounter spilling."
            )

        block_accessor = BlockAccessor.for_block(block)
        if block_accessor.num_rows() > 0:
            self._builder.add_block(block)
            self._total_num_rows_added += block_accessor.num_rows()
            self._total_nbytes_added += block_accessor.size_bytes()

    @property
    def _average_row_nbytes(self) -> Optional[int]:
        """Return the average number of bytes per row added to this batcher."""
        return (
            self._total_nbytes_added // self._total_num_rows_added
            if self._total_num_rows_added > 0
            else None
        )

    @property
    def _estimated_min_nbytes_in_buffers(self) -> Optional[int]:
        """Return the estimated minimum number of bytes across all buffers.

        This includes data in both the compacted and uncompacted buffers.
        """
        if self._average_row_nbytes is None:
            return None

        return self._average_row_nbytes * self._min_rows_to_trigger_compaction

    def done_adding(self) -> bool:
        """Indicate to the batcher that no more blocks will be added to the batcher.

        No more blocks should be added to the batcher after calling this.
        """
        self._done_adding = True

    def has_any(self) -> bool:
        """Whether this batcher has any data."""
        return self._num_rows() > 0

    def has_batch(self) -> bool:
        """Whether this batcher has any batches."""
        num_rows = self._num_rows()

        if not self._done_adding:
            # Delay pulling of batches until the buffer is large enough in order to
            # amortize compaction overhead.
            return num_rows >= self._batch_size and (
                self._num_compacted_rows() >= self._min_rows_to_yield_batch
                or num_rows - self._batch_size >= self._min_rows_to_trigger_compaction
            )
        else:
            return num_rows >= self._batch_size

    def _num_rows(self) -> int:
        """Return the total number of rows that haven't been yielded yet.

        This includes rows in both the compacted and uncompacted buffers.
        """
        return self._num_compacted_rows() + self._num_uncompacted_rows()

    def _num_compacted_rows(self) -> int:
        """Return number of unyielded rows in the compacted buffer."""
        if self._shuffle_buffer is None:
            return 0
        return max(0, len(self._shuffled_indices) - self._batch_head)

    def _num_uncompacted_rows(self) -> int:
        """Return number of unyielded rows in the uncompacted buffer."""
        return self._builder.num_rows()

    def next_batch(self) -> Block:
        """Get the next shuffled batch from the shuffle buffer.

        Returns:
            A batch represented as a Block.
        """
        assert self.has_batch() or (self._done_adding and self.has_any())
        if self._num_uncompacted_rows() > 0 and (
            self._done_adding
            or self._num_compacted_rows() <= self._min_rows_to_yield_batch
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
            # Build a hierarchical seed (least-variable → most-variable):
            #   base_seed, [split_index,] [execution_idx,] compaction_idx
            # Materialized (reversed) for np.random.default_rng():
            #   (compaction_idx, [execution_idx,] [split_index,] base_seed)
            shuffle_seed_tuple: tuple[int, ...] | None = None
            if self._shuffle_seed is not None:
                seed = self._shuffle_seed.make_base_seed()
                if seed is not None:
                    seed = self._shuffle_seed.apply_execution_idx(
                        seed, data_context=DataContext.get_current()
                    )
                    seed = seed.spawn(self._compaction_idx)
                    shuffle_seed_tuple = seed.as_rng_seed()
            self._shuffle_buffer = BlockAccessor.for_block(
                self._shuffle_buffer
            ).random_shuffle(shuffle_seed_tuple)
            self._compaction_idx += 1

            if isinstance(
                BlockAccessor.for_block(self._shuffle_buffer), ArrowBlockAccessor
            ):
                remaining_indices = self._shuffled_indices[self._batch_head :]
                remaining_block = BlockAccessor.for_block(self._shuffle_buffer).take(
                    remaining_indices
                )
                self._builder.add_block(remaining_block)
            self._shuffle_buffer = self._builder.build()

            accessor = BlockAccessor.for_block(self._shuffle_buffer)
            if isinstance(accessor, ArrowBlockAccessor):
                self._shuffle_buffer = try_combine_chunked_columns(
                    self._shuffle_buffer, min_chunks_to_combine=1
                )
                accessor = BlockAccessor.for_block(self._shuffle_buffer)

            num_rows = accessor.num_rows()
            self._shuffled_indices = self._rng.permutation(num_rows)

            self._builder = DelegatingBlockBuilder()
            self._batch_head = 0

        assert self._shuffle_buffer is not None
        assert self._shuffled_indices is not None
        remaining = len(self._shuffled_indices) - self._batch_head
        batch_size = min(self._batch_size, remaining)
        batch_indices = self._shuffled_indices[
            self._batch_head : self._batch_head + batch_size
        ]
        self._batch_head += batch_size
        return BlockAccessor.for_block(self._shuffle_buffer).take(batch_indices)
