from typing import Any

from ray.data.block import Block, T, BlockAccessor
from ray.data.impl.block_builder import BlockBuilder
from ray.data.impl.simple_block import SimpleBlockBuilder
from ray.data.impl.arrow_block import ArrowRow, ArrowBlockBuilder


class DelegatingBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        self._builder = None

    def add(self, item: Any) -> None:

        if self._builder is None:
            if isinstance(item, dict) or isinstance(item, ArrowRow):
                import pyarrow
                try:
                    check = ArrowBlockBuilder()
                    check.add(item)
                    check.build()
                    self._builder = ArrowBlockBuilder()
                except (TypeError, pyarrow.lib.ArrowInvalid):
                    self._builder = SimpleBlockBuilder()
            else:
                self._builder = SimpleBlockBuilder()
        self._builder.add(item)

    def add_block(self, block: Block) -> None:
        if self._builder is None:
            self._builder = BlockAccessor.for_block(block).builder()
        self._builder.add_block(block)

    def build(self) -> Block:
        if self._builder is None:
            self._builder = ArrowBlockBuilder()
        return self._builder.build()

    def num_rows(self) -> int:
        return self._builder.num_rows() if self._builder is not None else 0

    def get_estimated_memory_usage(self) -> int:
        if self._builder is None:
            return 0
        return self._builder.get_estimated_memory_usage()
