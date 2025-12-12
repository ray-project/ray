import math
from dataclasses import dataclass
from typing import Any, Optional

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.context import MAX_SAFE_BLOCK_SIZE_FACTOR


@dataclass
class OutputBlockSizeOption:
    target_max_block_size: Optional[int] = None
    target_num_rows_per_block: Optional[int] = None
    disable_block_shaping: bool = False

    def __post_init__(self):
        if (
            self.target_max_block_size is None
            and self.target_num_rows_per_block is None
            and not self.disable_block_shaping
        ):
            raise ValueError(
                "Either `target_max_block_size` or `target_num_rows_per_block` "
                "must be specified"
            )

    @classmethod
    def of(
        cls,
        target_max_block_size: Optional[int] = None,
        target_num_rows_per_block: Optional[int] = None,
        disable_block_shaping: bool = False,
    ) -> Optional["OutputBlockSizeOption"]:
        if (
            target_max_block_size is None
            and target_num_rows_per_block is None
            and not disable_block_shaping
        ):
            # In case
            #   - Both target_max_block_size and target_num_rows_per_block are None and
            #   - disable_block_shaping is False
            #
            # Buffer won't be yielding incrementally, instead producing just a single block.
            return None
        else:
            return OutputBlockSizeOption(
                target_max_block_size=target_max_block_size,
                target_num_rows_per_block=target_num_rows_per_block,
                disable_block_shaping=disable_block_shaping,
            )


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

    def __init__(self, output_block_size_option: Optional[OutputBlockSizeOption]):
        self._output_block_size_option = output_block_size_option
        self._buffer = DelegatingBlockBuilder()
        self._finalized = False
        self._has_yielded_blocks = False

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
        if self._output_block_size_option.disable_block_shaping:
            return False

        return (
            self._max_num_rows_per_block() is not None
            and self._buffer.num_rows() > self._max_num_rows_per_block()
        )

    def _exceeded_buffer_size_limit(self) -> bool:
        if self._output_block_size_option.disable_block_shaping:
            return False

        return (
            self._max_bytes_per_block() is not None
            and self._buffer.get_estimated_memory_usage() > self._max_bytes_per_block()
        )

    def _max_num_rows_per_block(self) -> Optional[int]:
        if self._output_block_size_option is None:
            return None

        if self._output_block_size_option.disable_block_shaping:
            return None

        return self._output_block_size_option.target_num_rows_per_block

    def _max_bytes_per_block(self) -> Optional[int]:
        if self._output_block_size_option is None:
            return None

        if self._output_block_size_option.disable_block_shaping:
            return None

        return self._output_block_size_option.target_max_block_size

    def has_next(self) -> bool:
        """Returns true when a complete output block is produced."""

        # TODO remove emitting empty blocks
        if self._finalized:
            return not self._has_yielded_blocks or self._buffer.num_rows() > 0
        elif self._output_block_size_option is None:
            # NOTE: When block sizing is disabled, buffer won't be producing
            #       incrementally, until the whole sequence is ingested. This
            #       is required to align it with semantic of producing 1 block
            #       from 1 block of the input
            return False
        elif self._output_block_size_option.disable_block_shaping:
            # When block shaping is disabled, produce blocks immediately
            return self._buffer.num_rows() > 0

        return self._exceeded_buffer_row_limit() or self._exceeded_buffer_size_limit()

    def _exceeded_block_size_slice_limit(self, block: BlockAccessor) -> bool:
        # Slice a block to respect the target max block size. We only do this if we are
        # more than 50% above the target block size, because this ensures that the last
        # block produced will be at least half the target block size.
        return (
            self._max_bytes_per_block() is not None
            and block.size_bytes()
            >= MAX_SAFE_BLOCK_SIZE_FACTOR * self._max_bytes_per_block()
        )

    def _exceeded_block_row_slice_limit(self, block: BlockAccessor) -> bool:
        # Slice a block to respect the target max rows per block. We only do this if we
        # are more than 50% above the target rows per block, because this ensures that
        # the last block produced will be at least half the target row count.
        return (
            self._max_num_rows_per_block() is not None
            and block.num_rows() > self._max_num_rows_per_block()
        )

    def next(self) -> Block:
        """Returns the next complete output block."""
        assert self.has_next()

        block = self._buffer.build()

        accessor = BlockAccessor.for_block(block)
        block_remainder = None
        target_num_rows = None

        if self._exceeded_block_row_slice_limit(accessor):
            target_num_rows = self._max_num_rows_per_block()
        elif self._exceeded_block_size_slice_limit(accessor):
            assert accessor.num_rows() > 0, "Block may not be empty"
            num_bytes_per_row = accessor.size_bytes() / accessor.num_rows()
            target_num_rows = max(
                1, math.ceil(self._max_bytes_per_block() / num_bytes_per_row)
            )

        if target_num_rows is not None and target_num_rows < accessor.num_rows():
            block = accessor.slice(0, target_num_rows, copy=False)
            block_remainder = accessor.slice(
                target_num_rows, accessor.num_rows(), copy=False
            )

        self._buffer = DelegatingBlockBuilder()
        if block_remainder is not None:
            self._buffer.add_block(block_remainder)

        self._has_yielded_blocks = True

        return block
