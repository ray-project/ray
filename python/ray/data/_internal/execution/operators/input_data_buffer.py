from typing import Callable, List, Optional

from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.stats import StatsDict


class InputDataBuffer(PhysicalOperator):
    """Defines the input data for the operator DAG.

    For example, this may hold cached blocks from a previous Dataset execution, or
    the arguments for read tasks.
    """

    def __init__(
        self,
        input_data: Optional[List[RefBundle]] = None,
        input_data_factory: Callable[[], List[RefBundle]] = None,
        num_output_blocks: Optional[int] = None,
    ):
        """Create an InputDataBuffer.

        Args:
            input_data: The list of bundles to output from this operator.
            input_data_factory: The factory to get input data, if input_data is None.
            num_output_blocks: The number of output blocks. If not specified, progress
                bars total will be set based on num output bundles instead.
        """
        if input_data is not None:
            assert input_data_factory is None
            # Copy the input data to avoid mutating the original list.
            self._input_data = input_data[:]
            self._is_input_initialized = True
            self._initialize_metadata()
        else:
            # Initialize input lazily when execution is started.
            assert input_data_factory is not None
            self._input_data_factory = input_data_factory
            self._is_input_initialized = False
        self._num_output_blocks = num_output_blocks
        super().__init__("Input", [])

    def start(self, options: ExecutionOptions) -> None:
        if not self._is_input_initialized:
            self._input_data = self._input_data_factory()
            self._is_input_initialized = True
            self._initialize_metadata()
        super().start(options)

    def has_next(self) -> bool:
        return len(self._input_data) > 0

    def get_next(self) -> RefBundle:
        return self._input_data.pop(0)

    def num_outputs_total(self) -> Optional[int]:
        return self._num_output_blocks or self._num_output_bundles

    def get_stats(self) -> StatsDict:
        return {}

    def add_input(self, refs, input_index) -> None:
        raise ValueError("Inputs are not allowed for this operator.")

    def _initialize_metadata(self):
        assert self._input_data is not None and self._is_input_initialized

        self._num_output_bundles = len(self._input_data)
        block_metadata = []
        for bundle in self._input_data:
            block_metadata.extend([m for (_, m) in bundle.blocks])
        self._stats = {
            "input": block_metadata,
        }
