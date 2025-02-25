from typing import Callable, List, Optional, Iterator

from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.stats import StatsDict
from ray.data.block import to_stats, BlockStats
from ray.data.context import DataContext


class InputDataBuffer(PhysicalOperator):
    """Defines the input data for the operator DAG that could be provided as an

        - List of ref bundles (list of blocks refs)
        - Factory producing list of ref bundles
        - Iterator yielding ref bundles

    For example, this may hold cached blocks from a previous Dataset execution, or
    the arguments for read tasks.
    """

    def __init__(
        self,
        data_context: DataContext,
        input_data: Optional[List[RefBundle]] = None,
        input_data_iter: Optional[Iterator[RefBundle]] = None,
        input_data_factory: Optional[Callable[[int], List[RefBundle]]] = None,
        num_output_blocks: Optional[int] = None,
    ):
        """Create an InputDataBuffer Operator.

        Args:
            # TODO update
            input_data: The list of bundles to output from this operator.
            input_data_factory: The factory to get input data, if input_data is None.
            num_output_blocks: The number of output blocks. If not specified, progress
                bars total will be set based on num output bundles instead.
        """
        super().__init__("Input", [], data_context, target_max_block_size=None)
        if input_data is not None:
            assert input_data_factory is None
            assert input_data_iter is None

            # Copy the input data to avoid mutating the original list.
            self._input_data = input_data[:]
            self._pre_initialize_metadata()

        elif input_data_iter is not None:
            # Initialize input lazily when execution is started.
            assert input_data is None
            assert input_data_factory is None

            self._input_data_iter = input_data_iter

        elif input_data_factory is not None:
            assert input_data is None
            assert input_data_iter is None

            self._input_data_factory = input_data_factory

        self._next_bundle: Optional[RefBundle] = None
        self._block_stats: List[BlockStats] = []


    def start(self, options: ExecutionOptions) -> None:
        if self._input_data_factory is not None:
            self._input_data = self._input_data_factory(
                self.actual_target_max_block_size
            )
            self._pre_initialize_metadata()

        if self._input_data:

            self._input_data_iter = iter(self._input_data)
            self._input_data = None

        super().start(options)

    def has_next(self) -> bool:
        if self._next_bundle is not None:
            return True

        try:
            self._next_bundle = next(self._input_data_iter)
        except StopIteration:
            self._next_bundle = None
            return False

        self._metrics.on_input_received(self._next_bundle)
        # Append fetched block stats
        self._block_stats.extend(to_stats(self._next_bundle.metadata))

        return True

    def _get_next_inner(self) -> RefBundle:
        return self._next_bundle

    def get_stats(self) -> StatsDict:
        return {
            "input": self._block_stats,
            "output": self._block_stats,
        }

    def _add_input_inner(self, refs, input_index) -> None:
        raise ValueError("Inputs are not allowed for this operator.")

    def _pre_initialize_metadata(self):
        self._estimated_num_output_bundles = len(self._input_data)

    def implements_accurate_memory_accounting(self) -> bool:
        return True
