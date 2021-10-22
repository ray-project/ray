import math
from typing import Callable, List, Iterator

import numpy as np

from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, T
from ray.data.impl.block_list import BlockList


class LazyBlockList(BlockList):
    def __init__(self,
                 calls: Callable[[], ObjectRef[Block]],
                 metadata: List[BlockMetadata],
                 blocks: List[ObjectRef[Block]] = None):
        self._calls = calls
        self._metadata = metadata
        if blocks:
            self._block_futures = blocks
        else:
            self._block_futures = [None] * len(calls)
            # Immediately compute the first block at least.
            if calls:
                self._block_futures[0] = calls[0]()
        assert len(calls) == len(metadata), (calls, metadata)
        assert len(calls) == len(self._block_futures), (calls,
                                                        self._block_futures)

    def copy(self) -> "LazyBlockList":
        return LazyBlockList(self._calls.copy(), self._metadata.copy(),
                             self._block_futures.copy())

    def clear(self):
        super().clear()
        self._calls = None

    def split(self, split_size: int) -> List["LazyBlockList"]:
        self._check_if_cleared()
        num_splits = math.ceil(len(self._calls) / split_size)
        calls = np.array_split(self._calls, num_splits)
        meta = np.array_split(self._metadata, num_splits)
        blocks = np.array_split(self._block_futures, num_splits)
        output = []
        for c, m, b in zip(calls, meta, blocks):
            output.append(LazyBlockList(c.tolist(), m.tolist(), b.tolist()))
        return output

    def divide(self, block_idx: int) -> ("BlockList", "BlockList"):
        self._check_if_cleared()
        left = LazyBlockList(self._calls[:block_idx],
                             self._metadata[:block_idx],
                             self._block_futures[:block_idx])
        right = LazyBlockList(self._calls[block_idx:],
                              self._metadata[block_idx:],
                              self._block_futures[block_idx:])
        return left, right

    def iter_futures(self) -> Iterator[ObjectRef[List[Block]]]:
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

    def _get_or_compute(self, i: int) -> ObjectRef[List[Block]]:
        self._check_if_cleared()
        assert i < len(self._calls), i
        # Check if we need to compute more blocks.
        if not self._block_futures[i]:
            # Exponentially increase the number of blocks computed per batch.
            for j in range(max(i + 1, i * 2)):
                if j >= len(self._block_futures):
                    break
                if not self._block_futures[j]:
                    self._block_futures[j] = self._calls[j]()
            assert self._block_futures[i], self._block_futures
        return self._block_futures[i]

    def _num_computed(self):
        i = 0
        for b in self._block_futures:
            if b is not None:
                i += 1
        return i
