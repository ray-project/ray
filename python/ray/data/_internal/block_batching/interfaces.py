from dataclasses import dataclass
from typing import Any

from ray.types import ObjectRef
from ray.data.block import Block, DataBatch


@dataclass
class Batch:
    """A batch of data with a corresponding index.

    Attributes:
        batch_idx: The global index of this batch so that downstream operations can
            maintain ordering.
        data: The batch of data.
    """

    batch_idx: int
    data: DataBatch


class CollatedBatch(Batch):
    """A batch of collated data with a corresponding index.

    Attributes:
        batch_idx: The global index of this batch so that downstream operations can
            maintain ordering.
        data: The batch of data which is the output of a user provided collate_fn
            Therefore, the type of this data can be Any.
    """

    batch_idx: int
    data: Any


class BlockPrefetcher:
    """Interface for prefetching blocks."""

    def prefetch_blocks(self, blocks: ObjectRef[Block]):
        """Prefetch the provided blocks to this node."""
        raise NotImplementedError
