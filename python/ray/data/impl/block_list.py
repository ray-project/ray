import math
from typing import List, Iterator, Tuple, Any, Union, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

import numpy as np

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, BlockAccessor
from ray.data.impl.remote_fn import cached_remote_fn


class BlockList:
    """A list of blocks that may be computed or pending computation.

    In the basic version of BlockList, all blocks are known ahead of time. In
    LazyBlockList, blocks are not yet computed, so the number of blocks may
    change after execution due to block splitting.
    """

    def __init__(self, blocks: List[ObjectRef[Block]], metadata: List[BlockMetadata]):
        assert len(blocks) == len(metadata), (blocks, metadata)
        self._blocks: List[ObjectRef[Block]] = blocks
        self._num_blocks = len(self._blocks)
        self._metadata: List[BlockMetadata] = metadata

    def set_metadata(self, i: int, metadata: BlockMetadata) -> None:
        """Set the metadata for a given block."""
        self._metadata[i] = metadata

    def get_metadata(self) -> List[BlockMetadata]:
        """Get the metadata for all blocks."""
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
                "can no longer use this Dataset."
            )

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

    def split_by_bytes(self, bytes_per_split: int) -> List["BlockList"]:
        """Split this BlockList into multiple lists.

        Args:
            bytes_per_split: The max number of bytes per split.
        """
        self._check_if_cleared()
        output = []
        cur_blocks = []
        cur_meta = []
        cur_size = 0
        for b, m in zip(self._blocks, self._metadata):
            if m.size_bytes is None:
                raise RuntimeError(
                    "Block has unknown size, cannot use split_by_bytes()"
                )
            size = m.size_bytes
            if cur_blocks and cur_size + size > bytes_per_split:
                output.append(BlockList(cur_blocks, cur_meta))
                cur_blocks = []
                cur_meta = []
                cur_size = 0
            cur_blocks.append(b)
            cur_meta.append(m)
            cur_size += size
        if cur_blocks:
            output.append(BlockList(cur_blocks, cur_meta))
        return output

    def size_bytes(self) -> int:
        """Returns the total size in bytes of the blocks, or -1 if not known."""
        size = 0
        has_size = False
        for m in self.get_metadata():
            if m.size_bytes is not None:
                has_size = True
                size += m.size_bytes
        if not has_size:
            return -1
        else:
            return size

    def divide(self, block_idx: int) -> ("BlockList", "BlockList"):
        """Divide into two BlockLists by the given block index.

        Args:
            block_idx: The block index to divide at.
        """
        self._check_if_cleared()
        return (
            BlockList(self._blocks[:block_idx], self._metadata[:block_idx]),
            BlockList(self._blocks[block_idx:], self._metadata[block_idx:]),
        )

    def get_blocks(self) -> List[ObjectRef[Block]]:
        """Bulk version of iter_blocks().

        Prefer calling this instead of the iter form for performance if you
        don't need lazy evaluation.
        """
        # Overriden in LazyBlockList for bulk evaluation.
        return list(self.iter_blocks())

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

    def get_blocks_with_metadata(self) -> List[Tuple[ObjectRef[Block], BlockMetadata]]:
        """Bulk version of iter_blocks_with_metadata().

        Prefer calling this instead of the iter form for performance if you
        don't need lazy evaluation.
        """
        self.get_blocks()  # Force bulk evaluation in LazyBlockList.
        return list(self.iter_blocks_with_metadata())

    def iter_blocks_with_metadata(
        self,
    ) -> Iterator[Tuple[ObjectRef[Block], BlockMetadata]]:
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
        return len(self.get_blocks())

    def ensure_schema_for_first_block(self) -> Optional[Union["pyarrow.Schema", type]]:
        """Ensure that the schema is set for the first block.

        Returns None if the block list is empty.
        """
        get_schema = cached_remote_fn(_get_schema)
        try:
            block = next(self.iter_blocks())
        except (StopIteration, ValueError):
            # Dataset is empty (no blocks) or was manually cleared.
            return None
        schema = ray.get(get_schema.remote(block))
        # Set the schema.
        self._metadata[0].schema = schema
        return schema


def _get_schema(block: Block) -> Any:
    return BlockAccessor.for_block(block).schema()
