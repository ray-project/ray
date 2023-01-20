from typing import Callable, List, Optional

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

    def __init__(
        self,
        input_data: Optional[List[RefBundle]] = None,
        input_data_factory: Callable[[], List[RefBundle]] = None,
    ):
        """Create an InputDataBuffer.

        Args:
            input_data: The list of bundles to output from this operator.
            input_data_factory: The factory to get input data, if input_data is None.
        """
        if input_data is not None:
            assert input_data_factory is None
            self._input_data = input_data
            self._is_input_initialized = True
            self._initialize_metadata()
        else:
            # Initialize input lazily when execution is started.
            assert input_data_factory is not None
            self._input_data_factory = input_data_factory
            self._is_input_initialized = False
        super().__init__("Input", [])

    def has_next(self) -> bool:
        if not self._is_input_initialized:
            self._initialize_input()
        return len(self._input_data) > 0

    def get_next(self) -> RefBundle:
        if not self._is_input_initialized:
            self._initialize_input()
        return self._input_data.pop(0)

    def num_outputs_total(self) -> Optional[int]:
        if not self._is_input_initialized:
            self._initialize_input()
        return self._num_outputs

    def get_stats(self) -> StatsDict:
        if not self._is_input_initialized:
            self._initialize_input()
        return {}

    def add_input(self, refs, input_index) -> None:
        raise ValueError("Inputs are not allowed for this operator.")

    def _initialize_input(self):
        assert not self._is_input_initialized
        self._input_data = self._input_data_factory()
        self._is_input_initialized = True
        self._initialize_metadata()

    def _initialize_metadata(self):
        assert self._input_data is not None and self._is_input_initialized

        self._num_outputs = len(self._input_data)
        block_metadata = []
        for bundle in self._input_data:
            block_metadata.extend([m for (_, m) in bundle.blocks])
        self._stats = {
            "input": block_metadata,
        }
