import math
from typing import Callable, List, Iterator, Tuple

import numpy as np

import ray
from ray.types import ObjectRef
from ray.data.block import (
    Block,
    BlockMetadata,
    BlockPartitionMetadata,
    MaybeBlockPartition,
)
from ray.data.context import DatasetContext
from ray.data.impl.block_list import BlockList


class LazyBlockList(BlockList):
    """A BlockList that submits tasks lazily on-demand.

    This BlockList is used for implementing read operations (e.g., to avoid
    needing to read all files of a Dataset when the user is just wanting to
    .take() the first few rows or view the schema).
    """

    def __init__(
        self,
        calls: Callable[[], ObjectRef[MaybeBlockPartition]],
        metadata: List[BlockPartitionMetadata],
        block_partitions: List[ObjectRef[MaybeBlockPartition]] = None,
    ):
        self._calls = calls
        self._num_blocks = len(self._calls)
        self._metadata = metadata
        if block_partitions:
            self._block_partitions = block_partitions
        else:
            self._block_partitions = [None] * len(calls)
            # Immediately compute the first block at least.
            if calls:
                self._block_partitions[0] = calls[0]()
        assert len(calls) == len(metadata), (calls, metadata)
        assert len(calls) == len(self._block_partitions), (
            calls,
            self._block_partitions,
        )

    def copy(self) -> "LazyBlockList":
        return LazyBlockList(
            self._calls.copy(), self._metadata.copy(), self._block_partitions.copy()
        )

    def clear(self) -> None:
        self._block_partitions = None
        self._calls = None

    def _check_if_cleared(self) -> None:
        if self._block_partitions is None:
            raise ValueError(
                "This Dataset's blocks have been moved, which means that you "
                "can no longer use this Dataset."
            )

    # Note: does not force execution prior to splitting.
    def split(self, split_size: int) -> List["LazyBlockList"]:
        self._check_if_cleared()
        num_splits = math.ceil(len(self._calls) / split_size)
        calls = np.array_split(self._calls, num_splits)
        meta = np.array_split(self._metadata, num_splits)
        block_partitions = np.array_split(self._block_partitions, num_splits)
        output = []
        for c, m, b in zip(calls, meta, block_partitions):
            output.append(LazyBlockList(c.tolist(), m.tolist(), b.tolist()))
        return output

    # Note: does not force execution prior to division.
    def divide(self, part_idx: int) -> ("LazyBlockList", "LazyBlockList"):
        self._check_if_cleared()
        left = LazyBlockList(
            self._calls[:part_idx],
            self._metadata[:part_idx],
            self._block_partitions[:part_idx],
        )
        right = LazyBlockList(
            self._calls[part_idx:],
            self._metadata[part_idx:],
            self._block_partitions[part_idx:],
        )
        return left, right

    def get_blocks(self) -> List[ObjectRef[Block]]:
        # Force bulk evaluation of all block partitions futures.
        list(self._iter_block_partitions())
        return list(self.iter_blocks())

    def iter_blocks_with_metadata(
        self,
    ) -> Iterator[Tuple[ObjectRef[Block], BlockMetadata]]:
        context = DatasetContext.get_current()
        self._check_if_cleared()
        outer = self

        class Iter:
            def __init__(self):
                self._base_iter = outer._iter_block_partitions()
                self._buffer = []

            def __iter__(self):
                return self

            def __next__(self):
                while not self._buffer:
                    if context.block_splitting_enabled:
                        part_ref, _ = next(self._base_iter)
                        partition = ray.get(part_ref)
                    else:
                        block, metadata = next(self._base_iter)
                        partition = [(block, metadata)]
                    for ref, metadata in partition:
                        self._buffer.append((ref, metadata))
                return self._buffer.pop(0)

        return Iter()

    def _iter_block_partitions(
        self,
    ) -> Iterator[Tuple[ObjectRef[MaybeBlockPartition], BlockPartitionMetadata]]:
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
                    return (
                        outer._get_or_compute(self._pos),
                        outer._metadata[self._pos],
                    )
                raise StopIteration

        return Iter()

    def _get_or_compute(self, i: int) -> ObjectRef[MaybeBlockPartition]:
        self._check_if_cleared()
        assert i < len(self._calls), i
        # Check if we need to compute more block_partitions.
        if not self._block_partitions[i]:
            # Exponentially increase the number computed per batch.
            for j in range(max(i + 1, i * 2)):
                if j >= len(self._block_partitions):
                    break
                if not self._block_partitions[j]:
                    self._block_partitions[j] = self._calls[j]()
            assert self._block_partitions[i], self._block_partitions
        return self._block_partitions[i]

    def _num_computed(self) -> int:
        i = 0
        for b in self._block_partitions:
            if b is not None:
                i += 1
        return i
