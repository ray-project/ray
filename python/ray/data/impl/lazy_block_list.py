import math
from typing import Callable, List, Iterator, Tuple

import numpy as np

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, BlockPartition, \
    BlockPartitionMetadata
from ray.data.impl.block_list import BlockList


class LazyBlockList(BlockList):
    """A BlockList that submits tasks lazily on-demand.

    This BlockList is used for implementing read operations (e.g., to avoid
    needing to read all files of a Dataset when the user is just wanting to
    .take() the first few rows or view the schema).
    """

    def __init__(self,
                 calls: Callable[[], ObjectRef[BlockPartition]],
                 metadata: List[BlockPartitionMetadata],
                 blocks: List[ObjectRef[BlockPartition]] = None):
        self._calls = calls
        self._num_blocks = len(self._calls)
        self._metadata = metadata
        if blocks:
            self._blocks = blocks
        else:
            self._blocks = [None] * len(calls)
            # Immediately compute the first block at least.
            if calls:
                self._blocks[0] = calls[0]()
        assert len(calls) == len(metadata), (calls, metadata)
        assert len(calls) == len(self._blocks), (calls, self._blocks)

    def copy(self) -> "LazyBlockList":
        return LazyBlockList(self._calls.copy(), self._metadata.copy(),
                             self._blocks.copy())

    def clear(self) -> None:
        super().clear()
        self._calls = None

    # Note: does not force execution prior to splitting.
    def split(self, split_size: int) -> List["LazyBlockList"]:
        self._check_if_cleared()
        num_splits = math.ceil(len(self._calls) / split_size)
        calls = np.array_split(self._calls, num_splits)
        meta = np.array_split(self._metadata, num_splits)
        blocks = np.array_split(self._blocks, num_splits)
        output = []
        for c, m, b in zip(calls, meta, blocks):
            output.append(LazyBlockList(c.tolist(), m.tolist(), b.tolist()))
        return output

    # Note: does not force execution prior to division.
    def divide(self, part_idx: int) -> ("LazyBlockList", "LazyBlockList"):
        self._check_if_cleared()
        left = LazyBlockList(self._calls[:part_idx], self._metadata[:part_idx],
                             self._blocks[:part_idx])
        right = LazyBlockList(self._calls[part_idx:],
                              self._metadata[part_idx:],
                              self._blocks[part_idx:])
        return left, right

    def iter_blocks_with_metadata(
            self) -> Iterator[Tuple[ObjectRef[Block], BlockMetadata]]:
        self._check_if_cleared()
        outer = self

        class Iter:
            def __init__(self):
                self._base_iter = outer._iter_partitions()
                self._buffer = []

            def __iter__(self):
                return self

            def __next__(self):
                while not self._buffer:
                    partition = ray.get(next(self._base_iter))
                    for ref, metadata in partition:
                        self._buffer.append((ref, metadata))
                return self._buffer.pop(0)

        return Iter()

    def _iter_partitions(self) -> Iterator[ObjectRef[BlockPartition]]:
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

    def _get_or_compute(self, i: int) -> ObjectRef[BlockPartition]:
        self._check_if_cleared()
        assert i < len(self._calls), i
        # Check if we need to compute more blocks.
        if not self._blocks[i]:
            # Exponentially increase the number of blocks computed per batch.
            for j in range(max(i + 1, i * 2)):
                if j >= len(self._blocks):
                    break
                if not self._blocks[j]:
                    self._blocks[j] = self._calls[j]()
            assert self._blocks[i], self._blocks
        return self._blocks[i]

    def _num_computed(self) -> int:
        i = 0
        for b in self._blocks:
            if b is not None:
                i += 1
        return i
