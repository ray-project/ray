from typing import List, Callable, Optional, Tuple

from ray.data._internal.stats import StatsDict
from ray.data._internal.execution.interfaces import (
    RefBundle,
    PhysicalOperator,
)


class AllToAllOperator(PhysicalOperator):
    """A blocking operator that executes once its inputs are complete.

    This operator implements distributed sort / shuffle operations, etc.
    """

    def __init__(
        self,
        bulk_fn: Callable[[List[RefBundle]], Tuple[List[RefBundle], StatsDict]],
        input_op: PhysicalOperator,
        num_outputs: Optional[int] = None,
        name: str = "AllToAll",
    ):
        """Create an AllToAllOperator.

        Args:
            bulk_fn: The blocking transformation function to run. The inputs are the
                list of input ref bundles, and the outputs are the output ref bundles
                and a stats dict.
            input_op: Operator generating input data for this op.
            num_outputs: The number of expected output bundles for progress bar.
            name: The name of this operator.
        """
        self._bulk_fn = bulk_fn
        self._num_outputs = num_outputs
        self._input_buffer: List[RefBundle] = []
        self._output_buffer: List[RefBundle] = []
        self._stats: StatsDict = {}
        super().__init__(name, [input_op])

    def num_outputs_total(self) -> Optional[int]:
        return (
            self._num_outputs
            if self._num_outputs
            else self.input_dependencies[0].num_outputs_total()
        )

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert input_index == 0, input_index
        self._input_buffer.append(refs)

    def inputs_done(self) -> None:
        self._output_buffer, self._stats = self._bulk_fn(self._input_buffer)
        self._input_buffer.clear()
        super().inputs_done()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def get_next(self) -> RefBundle:
        return self._output_buffer.pop(0)

    def get_stats(self) -> StatsDict:
        return self._stats
