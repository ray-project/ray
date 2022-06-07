from typing import Callable, Any, Optional

from ray.data.block import Block, BlockAccessor
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder


class BlockOutputBuffer(object):
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

    def __init__(
        self, block_udf: Optional[Callable[[Block], Block]], target_max_block_size: int
    ):
        self._target_max_block_size = target_max_block_size
        self._block_udf = block_udf
        self._buffer = DelegatingBlockBuilder()
        self._returned_at_least_one_block = False
        self._finalized = False

    def add(self, item: Any) -> None:
        """Add a single item to this output buffer."""
        assert not self._finalized
        self._buffer.add(item)

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
        block = self._buffer.build()
        accessor = BlockAccessor.for_block(block)
        if self._block_udf and accessor.num_rows() > 0:
            block = self._block_udf(block)
        self._buffer = DelegatingBlockBuilder()
        self._returned_at_least_one_block = True
        return block
