from typing import List, Optional

from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import StatsDict


class OneToOneOperator(PhysicalOperator):
    """An operator that has one input and one output dependency.

    This operator serves as the base for map, filter, limit, etc.
    """

    def __init__(
        self,
        name: str,
        input_op: PhysicalOperator,
    ):
        """Create a OneToOneOperator.
        Args:
            input_op: Operator generating input data for this op.
            name: The name of this operator.
        """
        super().__init__(name, [input_op])

    @property
    def input_dependency(self) -> PhysicalOperator:
        return self.input_dependencies[0]


class AllToAllOperator(PhysicalOperator):
    """A blocking operator that executes once its inputs are complete.

    This operator implements distributed sort / shuffle operations, etc.
    """

    def __init__(
        self,
        bulk_fn: AllToAllTransformFn,
        input_op: PhysicalOperator,
        num_outputs: Optional[int] = None,
        sub_progress_bar_names: Optional[List[str]] = None,
        name: str = "AllToAll",
    ):
        """Create an AllToAllOperator.
        Args:
            bulk_fn: The blocking transformation function to run. The inputs are the
                list of input ref bundles, and the outputs are the output ref bundles
                and a stats dict.
            input_op: Operator generating input data for this op.
            num_outputs: The number of expected output bundles for progress bar.
            sub_progress_bar_names: The names of internal sub progress bars.
            name: The name of this operator.
        """
        self._bulk_fn = bulk_fn
        self._next_task_index = 0
        self._num_outputs = num_outputs
        self._sub_progress_bar_names = sub_progress_bar_names
        self._sub_progress_bar_dict = None
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

    def all_inputs_done(self) -> None:
        ctx = TaskContext(
            task_idx=self._next_task_index,
            sub_progress_bar_dict=self._sub_progress_bar_dict,
        )
        self._output_buffer, self._stats = self._bulk_fn(self._input_buffer, ctx)
        self._next_task_index += 1
        self._input_buffer.clear()
        super().all_inputs_done()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def get_next(self) -> RefBundle:
        return self._output_buffer.pop(0)

    def get_stats(self) -> StatsDict:
        return self._stats

    def get_transformation_fn(self) -> AllToAllTransformFn:
        return self._bulk_fn

    def progress_str(self) -> str:
        return f"{len(self._output_buffer)} output"

    def initialize_sub_progress_bars(self, position: int) -> int:
        """Initialize all internal sub progress bars, and return the number of bars."""
        if self._sub_progress_bar_names is not None:
            self._sub_progress_bar_dict = {}
            for name in self._sub_progress_bar_names:
                bar = ProgressBar(name, self.num_outputs_total() or 1, position)
                # NOTE: call `set_description` to trigger the initial print of progress
                # bar on console.
                bar.set_description(f"  *- {name}")
                self._sub_progress_bar_dict[name] = bar
                position += 1
            return len(self._sub_progress_bar_dict)
        else:
            return 0

    def close_sub_progress_bars(self):
        """Close all internal sub progress bars."""
        if self._sub_progress_bar_dict is not None:
            for sub_bar in self._sub_progress_bar_dict.values():
                sub_bar.close()


class NAryOperator(PhysicalOperator):
    """An operator that has multiple input dependencies and one output.

    This operator serves as the base for union, zip, etc.
    """

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        """Create a OneToOneOperator.
        Args:
            input_op: Operator generating input data for this op.
            name: The name of this operator.
        """
        input_names = ", ".join([op._name for op in input_ops])
        op_name = f"{self.__class__.__name__}({input_names})"
        super().__init__(op_name, list(input_ops))
