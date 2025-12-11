from typing import Iterator, List, Optional, Tuple

from ray.data._internal.memory_tracing import trace_allocation
from ray.data.block import Block, BlockMetadata, Schema
from ray.types import ObjectRef


class BlockList:
    """A list of blocks that may be computed or pending computation.

    All blocks are known ahead of time
    """

    def __init__(
        self,
        blocks: List[ObjectRef[Block]],
        metadata: List[BlockMetadata],
        schema: Optional["Schema"] = None,
        *,
        owned_by_consumer: bool,
    ):
        assert len(blocks) == len(metadata), (blocks, metadata)
        for b in blocks:
            trace_allocation(b, "BlockList.__init__")
        self._blocks: List[ObjectRef[Block]] = blocks
        self._num_blocks = len(self._blocks)
        self._metadata: List[BlockMetadata] = metadata
        # Whether the block list is owned by consuming APIs, and if so it can be
        # eagerly deleted after read by the consumer.
        self._owned_by_consumer = owned_by_consumer
        # This field can be set to indicate the number of estimated output blocks,
        # since each read task may produce multiple output blocks after splitting.
        self._estimated_num_blocks = None
        # The schema of the blocks in this block list. This is optional, and may be None.
        self._schema = schema

    def __repr__(self):
        return f"BlockList(owned_by_consumer={self._owned_by_consumer})"

    def get_schema(self) -> Optional["Schema"]:
        """Get the schema for all blocks."""
        return self._schema

    def get_metadata(self, fetch_if_missing: bool = False) -> List[BlockMetadata]:
        """Get the metadata for all blocks."""
        return self._metadata.copy()

    def copy(self) -> "BlockList":
        """Perform a shallow copy of this BlockList."""
        return BlockList(
            self._blocks,
            self._metadata,
            owned_by_consumer=self._owned_by_consumer,
            schema=self._schema,
        )

    def clear(self) -> None:
        """Erase references to the tasks tracked by the BlockList."""
        self._blocks = None

    def is_cleared(self) -> bool:
        """Whether this BlockList has been cleared."""
        return self._blocks is None

    def _check_if_cleared(self) -> None:
        """Raise an error if this BlockList has been previously cleared."""
        if self.is_cleared():
            raise ValueError(
                "This Dataset's blocks have been moved, which means that you "
                "can no longer use this Dataset."
            )

    def get_blocks(self) -> List[ObjectRef[Block]]:
        """Get list of the blocks of this block list.

        This blocks on the execution of the tasks generating block outputs.
        The length of this iterator is not known until execution.
        """
        self._check_if_cleared()
        return list(self._blocks)

    def get_blocks_with_metadata(self) -> List[Tuple[ObjectRef[Block], BlockMetadata]]:
        """Bulk version of iter_blocks_with_metadata().

        Prefer calling this instead of the iter form for performance if you
        don't need lazy evaluation.
        """
        self.get_blocks()
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

    def estimated_num_blocks(self) -> int:
        """Estimate of number of output blocks, without triggering actual execution."""
        return self._estimated_num_blocks or self._num_blocks
