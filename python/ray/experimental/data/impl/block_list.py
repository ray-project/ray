from typing import Iterable, List

from ray.types import ObjectRef
from ray.experimental.data.block import Block, BlockMetadata


class BlockList(Iterable[ObjectRef[Block]]):
    def __init__(self, blocks: List[ObjectRef[Block]],
                 metadata: List[BlockMetadata]):
        assert len(blocks) == len(metadata), (blocks, metadata)
        self._blocks = blocks
        self._metadata = metadata

    def set_metadata(self, i: int, metadata: BlockMetadata) -> None:
        self._metadata[i] = metadata

    def get_metadata(self) -> List[BlockMetadata]:
        return self._metadata.copy()

    def __len__(self):
        return len(self._blocks)

    def __iter__(self):
        return iter(self._blocks)
