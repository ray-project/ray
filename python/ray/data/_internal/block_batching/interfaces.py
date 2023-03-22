from dataclasses import dataclass
from typing import List, Optional

import ray
from ray.types import ObjectRef
from ray.data.block import Block, DataBatch

@dataclass
class LogicalBatch:
    """A logical "batch" of data.

    This is not a fully created batch, but rather a conceptual batch
    consisting of unresolved Block Object references.

    Attributes:
        bundle_idx: The global index of this bundle so that downstream operations can
            maintain ordering.
        block_refs: The list of block object references for this batch.
        blocks: The resolved blocks for this batch. This attribute can only be accessed
            after calling `.resolve()`
        starting_block_idx: The index of the first block where this batch starts.
        ending_block_idx: The index of the last block where this batch ends. This can
            also be None, meaning the entirety of the last block is included in this
            batch. If this value is None, this allows us to eagerly clear the last
            block in this batch after reading, since the last block is not included in
            any other batches.
        num_rows: The number of rows in this batch. This should be equivalent to the
            provided batch size, except for the final batch.
    """

    batch_idx: int
    block_refs: List[ObjectRef[Block]]
    starting_block_idx: int
    ending_block_idx: Optional[int]
    num_rows: int

    def __post_init__(self):
        self._resolved = False

    def resolve(self):
        """Resolves the block_refs in this LogicalBatch."""
        if self._resolved:
            return
        self._resolved = True
        self._blocks = ray.get(self.block_refs)

    @property
    def blocks(self) -> List[Block]:
        if not self._resolved:
            raise RuntimeError("The resolved blocks for this logical batch can only be "
                               "accessed after calling `resolve`.")
        return self._blocks

@dataclass
class Batch:
    """A batch of data.
    
    Attributes:
        batch_idx: The global index of this batch so that downstream operations can
            maintain ordering.
        data: The batch of data.
        logical_batch: The logical batch that was used to create this batch.
    """
    batch_idx: int
    data: DataBatch
    logical_batch: LogicalBatch

class BlockPrefetcher:
    """Interface for prefetching blocks."""

    def prefetch_blocks(self, blocks: ObjectRef[Block]):
        """Prefetch the provided blocks to this node."""
        raise NotImplementedError