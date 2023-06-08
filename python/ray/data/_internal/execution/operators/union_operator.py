from typing import Callable, List, Optional, Tuple

from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.stats import StatsDict


class UnionOperator(PhysicalOperator):
    """An operator that combines input operators of the same type.

    The order of the blocks in the datastreams is preserved, as is the
    relative ordering between the datastreams passed in the argument list.
    """

    def __init__(
        self,
        *input_ops: List[PhysicalOperator],
    ):
        """Create a UnionOperator.

        Args:
            other: List of input operators to union.
        """

        self._output_buffer: List[RefBundle] = []
        self._stats: StatsDict = {}
        op_name = f"Union[{', '.join([op._name for op in input_ops])}]"
        super().__init__(op_name, list(input_ops))

    def num_outputs_total(self) -> Optional[int]:
        # TODO(Scott)
        left_num_outputs = self.input_dependencies[0].num_outputs_total()
        right_num_outputs = self.input_dependencies[1].num_outputs_total()
        if left_num_outputs is not None and right_num_outputs is not None:
            return max(left_num_outputs, right_num_outputs)
        elif left_num_outputs is not None:
            return left_num_outputs
        else:
            return right_num_outputs

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        # TODO(Scott)
        assert not self.completed()
        assert 0 <= input_index <= len(self._input_dependencies), input_index
        if input_index == 0:
            self._left_buffer.append(refs)
        else:
            self._right_buffer.append(refs)

    def inputs_done(self) -> None:
        # TODO(Scott)
        self._output_buffer, self._stats = self._zip(
            self._left_buffer, self._right_buffer
        )
        self._left_buffer.clear()
        self._right_buffer.clear()
        super().inputs_done()

    def has_next(self) -> bool:
        # TODO(Scott)
        return len(self._output_buffer) > 0

    def get_next(self) -> RefBundle:
        # TODO(Scott)
        return self._output_buffer.pop(0)

    def get_stats(self) -> StatsDict:
        # TODO(Scott)
        return self._stats

    def get_transformation_fn(self) -> Callable:
        return self._zip

    def _union(
        self, *inputs: List[List[RefBundle]]
    ) -> Tuple[List[RefBundle], StatsDict]:
        # TODO(Scott)
        pass
