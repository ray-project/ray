import functools
from typing import List, Optional

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.util import unify_block_metadata_schema
from ray.data.block import Block, BlockMetadata


class FromBlocks(LogicalOperator):
    """Logical operator for in-heap-memory (not object store) input data.

    If you want to create a Dataset from in-object-store data, use the `InputData`
    logical operator instead.
    """

    def __init__(
        self,
        input_blocks: List[Block],
        input_metadata: List[BlockMetadata],
    ):
        super().__init__(self.__class__.__name__, [], len(input_blocks))
        assert len(input_blocks) == len(input_metadata), (
            len(input_blocks),
            len(input_metadata),
        )
        assert all(m.num_rows is not None for m in self._input_metadata)
        assert all(m.size_bytes is not None for m in self._input_metadata)

        self._input_blocks = input_blocks
        self._input_metadata = input_metadata

    @property
    def input_blocks(self) -> List[Block]:
        return self._input_blocks

    @property
    def input_metadata(self) -> List[BlockMetadata]:
        return self._input_metadata

    def output_data(self) -> Optional[List[RefBundle]]:
        return None

    def aggregate_output_metadata(self) -> BlockMetadata:
        return self._cached_output_metadata

    @functools.cached_property
    def _cached_output_metadata(self) -> BlockMetadata:
        return BlockMetadata(
            num_rows=self._num_rows(),
            size_bytes=self._size_bytes(),
            schema=self._schema(),
            input_files=None,
            exec_stats=None,
        )

    def _num_rows(self):
        return sum(m.num_rows for m in self._input_metadata)

    def _size_bytes(self):
        return sum(m.size_bytes for m in self._input_metadata)

    def _schema(self):
        return unify_block_metadata_schema(self._input_metadata)
