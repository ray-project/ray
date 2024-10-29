from typing import Any

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.context import MAX_SAFE_BLOCK_SIZE_FACTOR


class BlockOutputBuffer:
    """Generates output blocks of a given size given a stream of inputs.

    This class is used to turn a stream of items / blocks of arbitrary size
    into a stream of blocks of ``target_max_block_size``. The caller should
    check ``has_next()`` after each ``add()`` call, and call ``next()`` to get
    the next block when ``has_next()`` returns True.

    When all items have been added, the caller must call ``finalize()`` and
    then check ``has_next()`` one last time.

    Examples:
        >>> from ray.data._internal.output_buffer import BlockOutputBuffer
        >>> udf = ... # doctest: +SKIP
        >>> generator = ... # doctest: +SKIP
        >>> # Yield a stream of output blocks.
        >>> output = BlockOutputBuffer(udf, 500 * 1024 * 1024) # doctest: +SKIP
        >>> for item in generator(): # doctest: +SKIP
        ...     output.add(item) # doctest: +SKIP
        ...     if output.has_next(): # doctest: +SKIP
        ...         yield output.next() # doctest: +SKIP
        >>> output.finalize() # doctest: +SKIP
        >>> if output.has_next() # doctest: +SKIP
        ...     yield output.next() # doctest: +SKIP
    """

    def __init__(self, target_max_block_size: int):
        self._target_max_block_size = target_max_block_size
        self._buffer = DelegatingBlockBuilder()
        self._returned_at_least_one_block = False
        self._finalized = False

    def add(self, item: Any) -> None:
        """Add a single item to this output buffer."""
        assert not self._finalized
        self._buffer.add(item)

    def add_batch(self, batch: DataBatch) -> None:
        """Add a data batch to this output buffer."""
        assert not self._finalized
        self._buffer.add_batch(batch)

    def add_block(self, block: Block) -> None:
        """Add a data block to this output buffer."""
        assert not self._finalized
        self._buffer.add_block(block)

    def finalize(self) -> None:
        """Must be called once all items have been added."""
        assert not self._finalized
        self._finalized = True

    def has_next(self) -> bool:
        """Returns true when a complete output block is produced."""
        if self._finalized:
            return not self._returned_at_least_one_block or self._buffer.num_rows() > 0
        else:
            return (
                self._buffer.get_estimated_memory_usage() > self._target_max_block_size
            )

    def next(self) -> Block:
        """Returns the next complete output block."""
        assert self.has_next()

        block_to_yield = self._buffer.build()
        block_remainder = None
        block = BlockAccessor.for_block(block_to_yield)
        if (
            block.size_bytes()
            >= MAX_SAFE_BLOCK_SIZE_FACTOR * self._target_max_block_size
        ):
            # Slice a block to respect the target max block size.  We only do
            # this if we are more than 50% above the target block size, because
            # this ensures that the last block produced will be at least half
            # the block size.
            num_bytes_per_row = block.size_bytes() // block.num_rows()
            target_num_rows = max(1, self._target_max_block_size // num_bytes_per_row)

            if target_num_rows < block.num_rows():
                # NOTE: We're maintaining following protocol of slicing underlying block
                #       into appropriately sized ones:
                #
                #         - (Finalized) Target blocks sliced from the original one
                #           and are *copied* to avoid referencing original blocks
                #         - Temporary remainder of the block should *NOT* be copied
                #           such as to avoid repeatedly copying the remainder bytes
                #           of the block, resulting in O(M * N) total bytes being
                #           copied, where N is the total number of bytes in the original
                #           block and M is the number of blocks that will be produced by
                #           this iterator
                block_to_yield = block.slice(0, target_num_rows, copy=True)
                block_remainder = block.slice(
                    target_num_rows, block.num_rows(), copy=False
                )

        self._buffer = DelegatingBlockBuilder()
        if block_remainder is not None:
            self._buffer.add_block(block_remainder)

        self._returned_at_least_one_block = True
        return block_to_yield
