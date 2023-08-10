import abc
from dataclasses import dataclass
from typing import Any, List

from ray.data.block import Block, DataBatch
from ray.types import ObjectRef


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


class BlockPrefetcher(metaclass=abc.ABCMeta):
    """Interface for prefetching blocks."""

    @abc.abstractmethod
    def prefetch_blocks(self, blocks: List[ObjectRef[Block]]):
        """Prefetch the provided blocks to this node."""
        pass

    def stop(self):
        """Stop prefetching and release resources."""
        pass
