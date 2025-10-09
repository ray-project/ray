import functools
from typing import List, Optional

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import LogicalOperator, SourceOperator
from ray.data._internal.util import unify_schemas_with_validation
from ray.data.block import BlockMetadata


class InputData(LogicalOperator, SourceOperator):
    """Logical operator for input data.

    This may hold cached blocks from a previous Dataset execution.
    """

    def __init__(
        self,
        input_data: List[RefBundle],
    ):
        super().__init__("InputData", [], len(input_data))
        self.input_data = input_data

    def output_data(self) -> Optional[List[RefBundle]]:
        return self.input_data

    def infer_metadata(self) -> BlockMetadata:
        return self._cached_output_metadata

    @functools.cached_property
    def _cached_output_metadata(self) -> BlockMetadata:
        return BlockMetadata(
            num_rows=self._num_rows(),
            size_bytes=self._size_bytes(),
            input_files=None,
            exec_stats=None,
        )

    def _num_rows(self):
        if all(bundle.num_rows() is not None for bundle in self.input_data):
            return sum(bundle.num_rows() for bundle in self.input_data)
        else:
            return None

    def _size_bytes(self):
        metadata = [m for bundle in self.input_data for m in bundle.metadata]
        if all(m.size_bytes is not None for m in metadata):
            return sum(m.size_bytes for m in metadata)
        else:
            return None

    def infer_schema(self):
        return unify_schemas_with_validation([data.schema for data in self.input_data])

    def is_lineage_serializable(self) -> bool:
        # This operator isn't serializable because it contains ObjectRefs.
        return False
