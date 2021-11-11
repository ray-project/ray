from typing import List, Tuple, Callable,

from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, BlockBuilder
from ray.data.impl.arrow_block import DelegatingArrowBlockBuilder


# A list of block references pending computation by a single task. For example,
# this may be the output of a task reading a file.
BlockPartition = List[Tuple[ObjectRef[Block], BlockMetadata]]

# The metadata that describes the output of a BlockPartition. This has the
# same type as the metadata that describes each block in the partition.
BlockPartitionMetadata = BlockMetadata


class BlockPartitionBuilder(object):
    def __init__(self, block_udf: Callable[[Block], Block],
                 target_max_block_size: int):
        self._target_max_block_size = target_max_block_size
        self._block_udf = block_udf
        self._buffer: BlockBuilder = DelegatingArrowBlockBuilder()
        self._blocks: List[ObjectRef[Block]] = []

    def add_block(self, block: Block) -> None:
        self._buffer.add_block(block)
        if self._buffer.get_estimated_memory_usage() > self._target_max_block_size():
            self._blocks.append(ray.put(self._buffer.build()))
            self._buffer = DelegatingArrowBlockBuilder()

    def iterator(self) -> Iterator[ObjectRef[Block]]:
        if self._buffer.num_rows() > 0:
            self._blocks.append(ray.put(self._buffer.build()))
        for block in self._blocks:
            yield block
