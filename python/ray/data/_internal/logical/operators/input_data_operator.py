import functools
from typing import Callable, List, Optional, Iterator

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.util import unify_block_metadata_schema
from ray.data.block import BlockMetadata


class InputData(LogicalOperator):
    """Logical operator for input data.

    This may hold cached blocks from a previous Dataset execution, or
    the arguments for read tasks.
    """

    def __init__(
        self,
        input_data: Optional[List[RefBundle]] = None,
        input_data_iter: Optional[Iterator[RefBundle]] = None,
        input_data_factory: Optional[Callable[[int], List[RefBundle]]] = None,
    ):
        if (input_data is not None) ^ (input_data_factory is not None) ^ (input_data_iter is not None):
            raise ValueError("Only one of input_data, input_data_iter or input_data_factory has to be provided.")

        super().__init__(
            "InputData", [], len(input_data) if input_data is not None else None
        )

        self.input_data = input_data
        self.input_data_iter = input_data_iter
        self.input_data_factory = input_data_factory

    def output_data(self) -> Optional[List[RefBundle]]:
        if self.input_data is None:
            return None
        return self.input_data

    def aggregate_output_metadata(self) -> BlockMetadata:
        return self._cached_output_metadata

    @functools.cached_property
    def _cached_output_metadata(self) -> BlockMetadata:
        if self.input_data is None:
            return BlockMetadata(None, None, None, None, None)

        return BlockMetadata(
            num_rows=self._num_rows(),
            size_bytes=self._size_bytes(),
            schema=self._schema(),
            input_files=None,
            exec_stats=None,
        )

    def _num_rows(self):
        assert self.input_data is not None
        if all(bundle.num_rows() is not None for bundle in self.input_data):
            return sum(bundle.num_rows() for bundle in self.input_data)
        else:
            return None

    def _size_bytes(self):
        assert self.input_data is not None
        metadata = [m for bundle in self.input_data for m in bundle.metadata]
        if all(m.size_bytes is not None for m in metadata):
            return sum(m.size_bytes for m in metadata)
        else:
            return None

    def _schema(self):
        assert self.input_data is not None
        metadata = [m for bundle in self.input_data for m in bundle.metadata]
        return unify_block_metadata_schema(metadata)

    def is_lineage_serializable(self) -> bool:
        # This operator isn't serializable because it contains ObjectRefs.
        return False
