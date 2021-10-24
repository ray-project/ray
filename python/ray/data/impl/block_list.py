import math
from typing import List, Iterator, Tuple

import numpy as np

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata

# A list of block references pending computation by a single task. For example,
# this may be the output of a task reading a file.
BlockPartition = List[ObjectRef[Block]]

# The metadata that describes the output of a BlockPartition. This has the
# same type as the metadata that describes each block in the parition.
BlockPartitionMetadata = BlockMetadata

# The number of blocks in a BlockPartition (1 or greater).
BlockPartitionSize = int


class BlockList:
    """A list of blocks that may be computed or pending computation.

    BlockList tracks a set of tasks that produce BlockPartition (List[Block])
    object refs as output.

    You can iterate over these tasks via ``.iter_partitions()``. Each task
    produces a list of one or more Blocks, the flat list of which can be
    accessed via ``.iter_executed_blocks()``. Note that the number of executed
    blocks may be greater than the number of tasks, if some tasks return
    multiple objects.

    Example: suppose a BlockList is created from 10 partition object refs.
    Then, the number of partitions in the BlockList is 10. The number of blocks
    is not known until the tasks have been executed, but is always >10.
    """

    def __init__(self, partitions: List[ObjectRef[BlockPartition]],
                 metadata: List[BlockPartitionMetadata]):
        assert len(partitions) == len(metadata), (partitions, metadata)
        self._partitions: List[ObjectRef[BlockPartition]] = partitions
        self._num_partitions = len(self._partitions)
        self._metadata = metadata

    def set_metadata(self, i: int, metadata: BlockPartitionMetadata) -> None:
        """Set the metadata for a given partition."""
        self._metadata[i] = metadata

    def get_metadata(self) -> List[BlockPartitionMetadata]:
        """Get the metadata for a given partition."""
        return self._metadata.copy()

    def copy(self) -> "BlockList":
        """Perform a shallow copy of this BlockList."""
        return BlockList(self._partitions, self._metadata)

    def clear(self) -> None:
        """Erase references to the tasks tracked by the BlockList."""
        self._partitions = None

    def _check_if_cleared(self) -> None:
        """Raise an error if this BlockList has been previously cleared."""
        if self._partitions is None:
            raise ValueError(
                "This Dataset's blocks have been moved, which means that you "
                "can no longer use this Dataset.")

    def split(self, split_size: int) -> List["BlockList"]:
        """Split this BlockList into multiple lists.

        Args:
            split_size: The number of lists to split into.
        """
        self._check_if_cleared()
        num_splits = math.ceil(len(self._partitions) / split_size)
        blocks = np.array_split(self._partitions, num_splits)
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
        return (BlockList(self._partitions[:part_idx],
                          self._metadata[:part_idx]),
                BlockList(self._partitions[part_idx:],
                          self._metadata[part_idx:]))

    def iter_partitions(self) -> Iterator[ObjectRef[BlockPartition]]:
        """Iterate over the partitions of this block list.

        This does not block on the execution of the tasks generating the
        partition outputs. The length of this iterator is equal to
        ``self.num_partitions()``.
        """
        return iter(self._partitions)

    def iter_executed_blocks(self) -> Iterator[ObjectRef[Block]]:
        """Iterate over the block outputs of executing all partitions.

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
            ObjectRef[Block], BlockPartitionMetadata, BlockPartitionSize]]:
        """Like ``iter_executed_blocks()``, but including partition metadata.

        Returns:
            Iterator of tuples of the block ref, the metadata of the partition
            the block comes from, and the number of blocks in that partition.
        """
        self._check_if_cleared()
        outer = self

        class Iter:
            def __init__(self):
                self._base_iter = zip(outer.iter_partitions(), outer._metadata)
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

    def num_partitions(self) -> int:
        """Returns the number of partitions of this BlockList."""
        return self._num_partitions

    def num_executed_blocks(self) -> int:
        """Returns the number of output blocks after execution.

        This blocks on execution of all partition tasks."""
        self._check_if_cleared()
        return len(list(self.iter_executed_blocks()))
