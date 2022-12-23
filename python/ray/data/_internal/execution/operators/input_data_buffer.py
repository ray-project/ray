from typing import List, Optional, Dict

from ray.data.block import BlockMetadata
from ray.data._internal.execution.interfaces import (
    RefBundle,
    PhysicalOperator,
)


class InputDataBuffer(PhysicalOperator):
    """Defines the input data for the operator DAG."""

    def __init__(self, input_data: List[RefBundle]):
        self._input_data = input_data
        self._num_outputs = len(input_data)
        block_metadata = []
        for bundle in input_data:
            block_metadata.extend([m for (_, m) in bundle.blocks])
        self._stats = {
            "input": block_metadata,
        }
        super().__init__("Input", [])

    def has_next(self) -> bool:
        return len(self._input_data) > 0

    def get_next(self) -> RefBundle:
        return self._input_data.pop(0)

    def num_outputs_total(self) -> Optional[int]:
        return self._num_outputs

    def get_stats(self) -> Dict[str, List[BlockMetadata]]:
        return {}
