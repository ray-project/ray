from dataclasses import dataclass
from typing import Any, Optional

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.context import MAX_SAFE_BLOCK_SIZE_FACTOR, MAX_SAFE_ROWS_PER_BLOCK_FACTOR


@dataclass
class OutputBlockSizeOption:
    target_max_block_size: Optional[int] = None
    target_num_rows_per_block: Optional[int] = None


class BlockOutputBuffer:
    """Generates output blocks of a given size or number of rows given a stream of
    inputs.

    This class is used to turn a stream of items / blocks of arbitrary size
    into a stream of blocks of target max block size or
    target max rows per block. The caller should check ``has_next()`` after each
    ``add()`` call, and call ``next()`` to get the next block when ``has_next()``
    returns True.

    When all items have been added, the caller must call ``finalize()`` and
    then check ``has_next()`` one last time.

    Examples:
        >>> from ray.data._internal.output_buffer import BlockOutputBuffer
        >>> udf = ... # doctest: +SKIP
        >>> generator = ... # doctest: +SKIP
        >>> # Yield a stream of output blocks.
        >>> output_block_size_option = OutputBlockSizeOption(target_max_block_size=500 * 1024 * 1024) # doctest: +SKIP
        >>> output = BlockOutputBuffer(output_block_size_option) # doctest: +SKIP
        >>> for item in generator(): # doctest: +SKIP
        ...     output.add(item) # doctest: +SKIP
        ...     if output.has_next(): # doctest: +SKIP
        ...         yield output.next() # doctest: +SKIP
        >>> output.finalize() # doctest: +SKIP
        >>> if output.has_next() # doctest: +SKIP
        ...     yield output.next() # doctest: +SKIP
    """

    def __init__(self, output_block_size_option: OutputBlockSizeOption):
        self._output_block_size_option = output_block_size_option
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

    def _exceeded_buffer_row_limit(self) -> bool:
        return (
            self._output_block_size_option.target_num_rows_per_block is not None
            and self._buffer.num_rows()
            > self._output_block_size_option.target_num_rows_per_block
        )

    def _exceeded_buffer_size_limit(self) -> bool:
        return (
            self._output_block_size_option.target_max_block_size is not None
            and self._buffer.get_estimated_memory_usage()
            > self._output_block_size_option.target_max_block_size
        )

    def has_next(self) -> bool:
        """Returns true when a complete output block is produced."""
        if self._finalized:
            return not self._returned_at_least_one_block or self._buffer.num_rows() > 0
        else:
            return (
                self._exceeded_buffer_row_limit() or self._exceeded_buffer_size_limit()
            )

    def _exceeded_block_size_slice_limit(self, block: Block) -> bool:
        # Slice a block to respect the target max block size. We only do this if we are
        # more than 50% above the target block size, because this ensures that the last
        # block produced will be at least half the target block size.
        return (
            self._output_block_size_option.target_max_block_size is not None
            and block.size_bytes()
            >= MAX_SAFE_BLOCK_SIZE_FACTOR
            * self._output_block_size_option.target_max_block_size
        )

    def _exceeded_block_row_slice_limit(self, block: Block) -> bool:
        # Slice a block to respect the target max rows per block. We only do this if we
        # are more than 50% above the target rows per block, because this ensures that
        # the last block produced will be at least half the target row count.
        return (
            self._output_block_size_option.target_num_rows_per_block is not None
            and block.num_rows()
            >= MAX_SAFE_ROWS_PER_BLOCK_FACTOR
            * self._output_block_size_option.target_num_rows_per_block
        )

    def next(self) -> Block:
        """Returns the next complete output block."""
        assert self.has_next()

        block_to_yield = self._buffer.build()
        block_remainder = None
        block = BlockAccessor.for_block(block_to_yield)

        target_num_rows = None
        if self._exceeded_block_row_slice_limit(block):
            target_num_rows = self._output_block_size_option.target_num_rows_per_block
        elif self._exceeded_block_size_slice_limit(block):
            num_bytes_per_row = block.size_bytes() // block.num_rows()
            target_num_rows = max(
                1,
                self._output_block_size_option.target_max_block_size
                // num_bytes_per_row,
            )

        if target_num_rows is not None and target_num_rows < block.num_rows():
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
            block_remainder = block.slice(target_num_rows, block.num_rows(), copy=False)

        self._buffer = DelegatingBlockBuilder()
        if block_remainder is not None:
            self._buffer.add_block(block_remainder)

        self._returned_at_least_one_block = True
        return block_to_yield
