import math
from typing import List, Iterator, Tuple

import numpy as np

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata


class BlockList:
    """A list of blocks that may be computed or pending computation."""

    def __init__(self, blocks: List[ObjectRef[Block]],
                 metadata: List[BlockMetadata]):
        assert len(blocks) == len(metadata), (blocks, metadata)
        self._blocks: List[ObjectRef[BlockPartition]] = blocks
        self._num_blocks = len(self._blocks)
        self._metadata = metadata

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

    def divide(self, part_idx: int) -> ("BlockList", "BlockList"):
        """Divide into two BlockLists by the given partition index.

        Args:
            part_idx: The partition index to divide at.
        """
        self._check_if_cleared()
        return (BlockList(self._blocks[:part_idx], self._metadata[:part_idx]),
                BlockList(self._blocks[part_idx:], self._metadata[part_idx:]))

    def iter_blocks(self) -> Iterator[ObjectRef[BlockPartition]]:
        """Iterate over the blocks of this block list.

        This does not block on the execution of the tasks generating the
        partition outputs. The length of this iterator is equal to
        ``self.num_blocks()``.
        """
        return iter(self._blocks)

    def iter_executed_blocks(self) -> Iterator[ObjectRef[Block]]:
        """Iterate over the block outputs of executing all blocks.

        This blocks on the execution of the tasks generating partition outputs.
        The length of this iterator is not known until execution.
        """
        self._check_if_cleared()
        outer = self

        class Iter:
            def __init__(self):
                self._base_iter = \
                    outer.iter_executed_blocks_with_partition_metadata()

            def __iter__(self):
                return self

            def __next__(self):
                ref, meta, n = next(self._base_iter)
                assert isinstance(ref, ray.ObjectRef), (ref, meta, n)
                return ref

        return Iter()

    def iter_executed_blocks_with_partition_metadata(self) -> Iterator[Tuple[
            ObjectRef[Block], BlockPartitionMetadata, Blockblocksize]]:
        """Like ``iter_executed_blocks()``, but including partition metadata.

        Returns:
            Iterator of tuples of the block ref, the metadata of the partition
            the block comes from, and the number of blocks in that partition.
        """
        self._check_if_cleared()
        outer = self

        class Iter:
            def __init__(self):
                self._base_iter = zip(outer.iter_blocks(), outer._metadata)
                self._buffer = []

            def __iter__(self):
                return self

            def __next__(self):
                while not self._buffer:
                    refs, part_meta = next(self._base_iter)
                    refs = ray.get(refs)
                    for ref in refs:
                        if not isinstance(ref, ray.ObjectRef):
                            raise AssertionError(
                                "Expected list of block refs, but got a list "
                                "of raw blocks elements: {}. Probably a "
                                "task returned a block ref directly instead "
                                "of [ray.put(block)].".format(refs))
                        self._buffer.append((ref, part_meta, len(refs)))
                return self._buffer.pop(0)

        return Iter()

    def initial_num_blocks(self) -> int:
        """Returns the number of blocks of this BlockList."""
        return self._num_blocks

    def executed_num_blocks(self) -> int:
        """Returns the number of output blocks after execution.

        This may differ from initial_num_blocks() for LazyBlockList, which
        doesn't know how many blocks will be produced until tasks finish.
        """
        return self.initial_num_blocks()
