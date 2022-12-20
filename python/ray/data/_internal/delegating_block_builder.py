from typing import Any

import numpy as np

from ray.data.block import Block, DataBatch, T, BlockAccessor
from ray.data._internal.block_builder import BlockBuilder
from ray.data._internal.simple_block import SimpleBlockBuilder
from ray.data._internal.arrow_block import ArrowRow, ArrowBlockBuilder
from ray.data._internal.pandas_block import PandasRow, PandasBlockBuilder


class DelegatingBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        self._builder = None
        self._empty_block = None

    def add(self, item: Any) -> None:
        if self._builder is None:
            # TODO (kfstorm): Maybe we can use Pandas block format for dict.
            if isinstance(item, dict) or isinstance(item, ArrowRow):
                import pyarrow

                try:
                    check = ArrowBlockBuilder()
                    check.add(item)
                    check.build()
                    self._builder = ArrowBlockBuilder()
                except (TypeError, pyarrow.lib.ArrowInvalid):
                    self._builder = SimpleBlockBuilder()
            elif isinstance(item, np.ndarray):
                self._builder = ArrowBlockBuilder()
            elif isinstance(item, PandasRow):
                self._builder = PandasBlockBuilder()
            else:
                self._builder = SimpleBlockBuilder()
        self._builder.add(item)

    def add_batch(self, batch: DataBatch):
        """Add a user-facing data batch to the builder.

        This data batch will be converted to an internal block and then added to the
        underlying builder.
        """
        block = BlockAccessor.batch_to_block(batch)
        return self.add_block(block)

    def add_block(self, block: Block):
        accessor = BlockAccessor.for_block(block)
        if accessor.num_rows() == 0:
            # Don't infer types of empty lists. Store the block and use it if no
            # other data is added. https://github.com/ray-project/ray/issues/20290
            self._empty_block = block
            return
        if self._builder is None:
            self._builder = accessor.builder()
        self._builder.add_block(block)

    def build(self) -> Block:
        if self._builder is None:
            if self._empty_block is not None:
                self._builder = BlockAccessor.for_block(self._empty_block).builder()
                self._builder.add_block(self._empty_block)
            else:
                self._builder = ArrowBlockBuilder()
        return self._builder.build()

    def num_rows(self) -> int:
        return self._builder.num_rows() if self._builder is not None else 0

    def get_estimated_memory_usage(self) -> int:
        if self._builder is None:
            return 0
        return self._builder.get_estimated_memory_usage()
