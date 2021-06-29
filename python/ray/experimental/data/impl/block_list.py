from typing import Iterable, List, Optional, Any, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

from ray.experimental.data.impl.block import Block, ObjectRef, T


class BlockMetadata:
    def __init__(self, *, num_rows: Optional[int], size_bytes: Optional[int],
                 schema: Union[type, "pyarrow.lib.Schema"],
                 input_files: List[str]):
        self.num_rows: Optional[int] = num_rows
        self.size_bytes: Optional[int] = size_bytes
        self.schema: Optional[Any] = schema
        self.input_files: List[str] = input_files or []


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
