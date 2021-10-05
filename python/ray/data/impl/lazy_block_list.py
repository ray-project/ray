import math
from typing import Callable, List

import numpy as np

from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, T
from ray.data.impl.block_list import BlockList


class LazyBlockList(BlockList[T]):
    def __init__(self, calls: Callable[[], ObjectRef[Block]],
                 metadata: List[BlockMetadata]):
        assert len(calls) == len(metadata), (calls, metadata)
        self._calls = calls
        self._blocks = [calls[0]()] if calls else []
        self._metadata = metadata

    def copy(self) -> "LazyBlockList":
        new_list = LazyBlockList.__new__(LazyBlockList)
        new_list._calls = self._calls
        new_list._blocks = self._blocks
        new_list._metadata = self._metadata
        return new_list

    def clear(self):
        super().clear()
        self._calls = None

    def split(self, split_size: int) -> List["LazyBlockList"]:
        # TODO(ekl) isn't this not copying already computed blocks?
        self._check_if_cleared()
        num_splits = math.ceil(len(self._calls) / split_size)
        calls = np.array_split(self._calls, num_splits)
        meta = np.array_split(self._metadata, num_splits)
        output = []
        for c, m in zip(calls, meta):
            output.append(LazyBlockList(c.tolist(), m.tolist()))
        return output

    def divide(self, block_idx: int) -> ("BlockList", "BlockList"):
        self._check_if_cleared()
        left = self.copy()
        right = self.copy()
        left._calls = left._calls[:block_idx]
        left._blocks = left._blocks[:block_idx]
        left._metadata = left._metadata[:block_idx]
        right._calls = right._calls[block_idx:]
        right._blocks = right._blocks[block_idx:]
        right._metadata = right._metadata[block_idx:]
        return left, right

    def __len__(self):
        self._check_if_cleared()
        return len(self._calls)

    def __iter__(self):
        self._check_if_cleared()
        outer = self

        class Iter:
            def __init__(self):
                self._pos = -1

            def __iter__(self):
                return self

            def __next__(self):
                self._pos += 1
                if self._pos < len(outer._calls):
                    return outer._get_or_compute(self._pos)
                raise StopIteration

        return Iter()

    def _get_or_compute(self, i: int) -> ObjectRef[Block]:
        self._check_if_cleared()
        assert i < len(self._calls), i
        # Check if we need to compute more blocks.
        if i >= len(self._blocks):
            start = len(self._blocks)
            # Exponentially increase the number of blocks computed per batch.
            for c in self._calls[start:max(i + 1, start * 2)]:
                self._blocks.append(c())
        return self._blocks[i]
