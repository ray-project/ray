from typing import Callable, List, Optional

from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext


class InputDataBuffer(PhysicalOperator):
    """Defines the input data for the operator DAG.

    For example, this may hold cached blocks from a previous Dataset execution, or
    the arguments for read tasks.
    """

    def __init__(
        self,
        data_context: DataContext,
        input_data: Optional[List[RefBundle]] = None,
        input_data_factory: Optional[Callable[[int], List[RefBundle]]] = None,
    ):
        """Create an InputDataBuffer.

        Args:
            data_context: :class:`~ray.data.context.DataContext`
                object to use injestion.
            input_data: The list of bundles to output from this operator.
            input_data_factory: The factory to get input data, if input_data is None.
        """
        super().__init__("Input", [], data_context)
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
        self._input_data_index = 0
        self.mark_execution_finished()

    def start(self, options: ExecutionOptions) -> None:
        if not self._is_input_initialized:
            self._input_data = self._input_data_factory(
                self.target_max_block_size_override
                or self.data_context.target_max_block_size
            )
            self._is_input_initialized = True
            self._initialize_metadata()
        # InputDataBuffer does not take inputs from other operators,
        # so we record input metrics here
        for bundle in self._input_data:
            self._metrics.on_input_received(bundle)
        super().start(options)

    def has_next(self) -> bool:
        return self._input_data_index < len(self._input_data)

    def _get_next_inner(self) -> RefBundle:
        # We can't pop the input data. If we do, Ray might garbage collect the block
        # references, and Ray won't be able to reconstruct downstream objects.
        bundle = self._input_data[self._input_data_index]
        self._input_data_index += 1
        return bundle

    def get_stats(self) -> StatsDict:
        return {}

    def _add_input_inner(self, refs, input_index) -> None:
        raise ValueError("Inputs are not allowed for this operator.")

    def _initialize_metadata(self):
        assert self._input_data is not None and self._is_input_initialized
        self._estimated_num_output_bundles = len(self._input_data)

        block_metadata = []
        total_rows = 0
        for bundle in self._input_data:
            block_metadata.extend(bundle.metadata)
            bundle_num_rows = bundle.num_rows()
            if total_rows is not None and bundle_num_rows is not None:
                total_rows += bundle_num_rows
            else:
                # total row is unknown
                total_rows = None
        if total_rows:
            self._estimated_num_output_rows = total_rows
        self._stats = {
            "input": block_metadata,
        }

    def implements_accurate_memory_accounting(self) -> bool:
        return True
