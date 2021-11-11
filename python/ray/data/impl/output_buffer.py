from typing import Callable, Any

from ray.data.block import Block
from ray.data.impl.arrow_block import DelegatingArrowBlockBuilder


class BlockOutputBuffer(object):
    def __init__(self, block_udf: Callable[[Block], Block],
                 target_max_block_size: int):
        from ray.data.impl.arrow_block import DelegatingArrowBlockBuilder
        self._target_max_block_size = target_max_block_size
        self._block_udf = block_udf
        self._buffer = DelegatingArrowBlockBuilder()
        self._returned_at_least_one_block = False
        self._finalized = False

    def add(self, item: Any) -> None:
        assert not self._finalized
        self._buffer.add(item)

    def add_block(self, block: Block) -> None:
        assert not self._finalized
        self._buffer.add_block(block)

    def finalize(self) -> None:
        assert not self._finalized
        self._finalized = True

    def has_next(self) -> bool:
        if self._finalized:
            return not self._returned_at_least_one_block \
                or self._buffer.num_rows() > 0
        else:
            return self._buffer.get_estimated_memory_usage() > \
                self._target_max_block_size

    def next(self) -> Block:
        assert self._buffer.num_rows() > 0 \
            or not self._returned_at_least_one_block
        block = self._buffer.build()
        if self._block_udf and block.num_rows > 0:
            block = self._block_udf(block)
        self._buffer = DelegatingArrowBlockBuilder()
        self._returned_at_least_one_block = True
        return block
