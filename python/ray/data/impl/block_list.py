import math
from typing import List, Iterator

import numpy as np

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata


class BlockList:
    def __init__(self, blocks: List[ObjectRef[List[Block]]],
                 metadata: List[BlockMetadata]):
        assert len(blocks) == len(metadata), (blocks, metadata)
        self._block_futures: List[ObjectRef[List[Block]]] = blocks
        self._metadata = metadata

    def set_metadata(self, i: int, metadata: BlockMetadata) -> None:
        self._metadata[i] = metadata

    def get_metadata(self) -> List[BlockMetadata]:
        return self._metadata.copy()

    def copy(self) -> "BlockList":
        return BlockList(self._block_futures, self._metadata)

    def clear(self):
        self._block_futures = None

    def _check_if_cleared(self):
        if self._block_futures is None:
            raise ValueError(
                "This Dataset's blocks have been moved, which means that you "
                "can no longer use this Dataset.")

    def split(self, split_size: int) -> List["BlockList"]:
        self._check_if_cleared()
        num_splits = math.ceil(len(self._block_futures) / split_size)
        blocks = np.array_split(self._block_futures, num_splits)
        meta = np.array_split(self._metadata, num_splits)
        output = []
        for b, m in zip(blocks, meta):
            output.append(BlockList(b.tolist(), m.tolist()))
        return output

    def divide(self, block_idx: int) -> ("BlockList", "BlockList"):
        self._check_if_cleared()
        return (BlockList(self._block_futures[:block_idx],
                          self._metadata[:block_idx]),
                BlockList(self._block_futures[block_idx:],
                          self._metadata[block_idx:]))

    def iter_futures(self) -> Iterator[ObjectRef[List[Block]]]:
        return iter(self._block_futures)

    def iter_evaluated_with_orig_metadata(
            self) -> Iterator[(ObjectRef[Block], BlockMetadata)]:
        self._check_if_cleared()
        outer = self

        class Iter:
            def __init__(self):
                self._base_iter = zip(outer.iter_futures(), self._metadata)
                self._buffer = []

            def __iter__(self):
                return self

            def __next__(self):
                if not self._buffer:
                    refs, orig_meta = next(self._base_iter)
                    for ref in refs:
                        self._buffer.append((ray.get(ref), orig_meta))
                return self._buffer.pop(0)

        return Iter()

    def iter_evaluated(self) -> Iterator[ObjectRef[Block]]:
        self._check_if_cleared()
        outer = self

        class Iter:
            def __init__(self):
                self._base_iter = outer.iter_evaluated_with_orig_metadata()

            def __iter__(self):
                return self

            def __next__(self):
                ref, _ = next(self._base_iter)
                return ref

        return Iter()

    def num_futures(self) -> int:
        return len(self._block_futures)

    def num_evaluated(self) -> int:
        self._check_if_cleared()
        return len(list(self.iter_evaluated()))
