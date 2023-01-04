from typing import List, Optional

from ray.data._internal.stats import StatsDict
from ray.data._internal.execution.interfaces import (
    RefBundle,
    PhysicalOperator,
)


class InputDataBuffer(PhysicalOperator):
    """Defines the input data for the operator DAG.

    For example, this may hold cached blocks from a previous Dataset execution, or
    the arguments for read tasks.
    """

    def __init__(self, input_data: List[RefBundle]):
        """Create an InputDataBuffer.

        Args:
            input_data: The list of bundles to output from this operator.
        """
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

    def get_stats(self) -> StatsDict:
        return {}

    def add_input(self, refs, input_index) -> None:
        raise ValueError("Inputs are not allowed for this operator.")
