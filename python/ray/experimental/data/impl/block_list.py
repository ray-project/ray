from typing import Iterable, List

from ray.experimental.data.impl.block import Block, BlockMetadata, ObjectRef, T


class BlockList(Iterable[ObjectRef[Block[T]]]):
    def __init__(self, blocks: List[ObjectRef[Block[T]]],
                 metadata: List[BlockMetadata]):
        assert len(blocks) == len(metadata), (blocks, metadata)
        self._blocks = blocks
        self._metadata = metadata

    def get_metadata(self) -> List[BlockMetadata]:
        return self._metadata.copy()

    def __len__(self):
        return len(self._blocks)

    def __iter__(self):
        return iter(self._blocks)
