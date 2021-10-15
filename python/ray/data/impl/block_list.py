import math
from typing import Iterable, List

import numpy as np

from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata


class BlockList(Iterable[ObjectRef[Block]]):
    def __init__(self, blocks: List[ObjectRef[Block]],
                 metadata: List[BlockMetadata]):
        assert len(blocks) == len(metadata), (blocks, metadata)
        self._blocks = blocks
        self._metadata = metadata

    def set_metadata(self, i: int, metadata: BlockMetadata) -> None:
        self._metadata[i] = metadata

    def get_metadata(self) -> List[BlockMetadata]:
        return self._metadata.copy()

    def copy(self) -> "BlockList":
        return BlockList(self._blocks, self._metadata)

    def clear(self):
        self._blocks = None

    def _check_if_cleared(self):
        if self._blocks is None:
            raise ValueError(
                "This Dataset's blocks have been moved, which means that you "
                "can no longer use this Dataset.")

    def split(self, split_size: int) -> List["BlockList"]:
        self._check_if_cleared()
        num_splits = math.ceil(len(self._blocks) / split_size)
        blocks = np.array_split(self._blocks, num_splits)
        meta = np.array_split(self._metadata, num_splits)
        output = []
        for b, m in zip(blocks, meta):
            output.append(BlockList(b.tolist(), m.tolist()))
        return output

    def divide(self, block_idx: int) -> ("BlockList", "BlockList"):
        self._check_if_cleared()
        return (BlockList(self._blocks[:block_idx],
                          self._metadata[:block_idx]),
                BlockList(self._blocks[block_idx:],
                          self._metadata[block_idx:]))

    def __len__(self):
        self._check_if_cleared()
        return len(self._blocks)

    def __iter__(self):
        self._check_if_cleared()
        return iter(self._blocks)
