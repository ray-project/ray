from dataclasses import dataclass
from typing import Any

from ray.types import ObjectRef
from ray.data.block import Block


@dataclass
class Batch:
    """A batch of data.

    Attributes:
        batch_idx: The global index of this batch so that downstream operations can
            maintain ordering.
        data: The batch of data.
    """

    batch_idx: int
    data: Any


class BlockPrefetcher:
    """Interface for prefetching blocks."""

    def prefetch_blocks(self, blocks: ObjectRef[Block]):
        """Prefetch the provided blocks to this node."""
        raise NotImplementedError
