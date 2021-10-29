import math
from typing import List, Iterator, Tuple

import numpy as np

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata


class BlockList:
    """A list of blocks that may be computed or pending computation.

    In the basic version of BlockList, all blocks are known ahead of time. In
    LazyBlockList, blocks are not yet computed, so the number of blocks may
    change after execution due to block splitting.
    """

    def __init__(self, blocks: List[ObjectRef[Block]],
                 metadata: List[BlockMetadata]):
        assert len(blocks) == len(metadata), (blocks, metadata)
        self._blocks: List[ObjectRef[Block]] = blocks
        self._num_blocks = len(self._blocks)
        self._metadata: List[BlockMetadata] = metadata

    def set_metadata(self, i: int, metadata: BlockMetadata) -> None:
        """Set the metadata for a given block."""
        self._metadata[i] = metadata

    def get_metadata(self) -> List[BlockMetadata]:
        """Get the metadata for a given block."""
        return self._metadata.copy()

    def copy(self) -> "BlockList":
        """Perform a shallow copy of this BlockList."""
        return BlockList(self._blocks, self._metadata)

    def clear(self) -> None:
        """Erase references to the tasks tracked by the BlockList."""
        self._blocks = None

    def _check_if_cleared(self) -> None:
        """Raise an error if this BlockList has been previously cleared."""
        if self._blocks is None:
            raise ValueError(
                "This Dataset's blocks have been moved, which means that you "
                "can no longer use this Dataset.")

    def split(self, split_size: int) -> List["BlockList"]:
        """Split this BlockList into multiple lists.

        Args:
            split_size: The number of lists to split into.
        """
        self._check_if_cleared()
        num_splits = math.ceil(len(self._blocks) / split_size)
        blocks = np.array_split(self._blocks, num_splits)
        meta = np.array_split(self._metadata, num_splits)
        output = []
        for b, m in zip(blocks, meta):
            output.append(BlockList(b.tolist(), m.tolist()))
        return output

    def divide(self, block_idx: int) -> ("BlockList", "BlockList"):
        """Divide into two BlockLists by the given block index.

        Args:
            block_idx: The block index to divide at.
        """
        self._check_if_cleared()
        return (BlockList(self._blocks[:block_idx],
                          self._metadata[:block_idx]),
                BlockList(self._blocks[block_idx:],
                          self._metadata[block_idx:]))

    def iter_blocks(self) -> Iterator[ObjectRef[Block]]:
        """Iterate over the blocks of this block list.

        This blocks on the execution of the tasks generating block outputs.
        The length of this iterator is not known until execution.
        """
        self._check_if_cleared()
        outer = self

        class Iter:
            def __init__(self):
                self._base_iter = outer.iter_blocks_with_metadata()

            def __iter__(self):
                return self

            def __next__(self):
                ref, meta = next(self._base_iter)
                assert isinstance(ref, ray.ObjectRef), (ref, meta)
                return ref

        return Iter()

    def iter_blocks_with_metadata(
            self) -> Iterator[Tuple[ObjectRef[Block], BlockMetadata]]:
        """Iterate over the blocks along with their runtime metadata.

        This blocks on the execution of the tasks generating block outputs.
        The length of this iterator is not known until execution.
        """
        self._check_if_cleared()
        return zip(self._blocks, self._metadata)

    def initial_num_blocks(self) -> int:
        """Returns the number of blocks of this BlockList."""
        return self._num_blocks

    def executed_num_blocks(self) -> int:
        """Returns the number of output blocks after execution.

        This may differ from initial_num_blocks() for LazyBlockList, which
        doesn't know how many blocks will be produced until tasks finish.
        """
        return len(list(self.iter_blocks()))
