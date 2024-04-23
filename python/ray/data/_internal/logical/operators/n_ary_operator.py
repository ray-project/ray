from typing import Optional

from ray.data._internal.logical.interfaces import LogicalOperator


class NAry(LogicalOperator):
    """Base class for n-ary operators, which take multiple input operators."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
        num_outputs: Optional[int] = None,
    ):
        """
        Args:
            input_ops: The input operators.
        """
        super().__init__(self.__class__.__name__, list(input_ops), num_outputs)


class Zip(NAry):
    """Logical operator for zip."""

    def __init__(
        self,
        left_input_op: LogicalOperator,
        right_input_op: LogicalOperator,
    ):
        """
        Args:
            left_input_ops: The input operator at left hand side.
            right_input_op: The input operator at right hand side.
        """
        super().__init__(left_input_op, right_input_op)

    def estimated_num_outputs(self):
        left_num_outputs = self._input_dependencies[0].estimated_num_outputs()
        right_num_outputs = self._input_dependencies[1].estimated_num_outputs()
        if left_num_outputs is None or right_num_outputs is None:
            return None
        return max(left_num_outputs, right_num_outputs)


class Union(NAry):
    """Logical operator for union."""

    def __init__(
        self,
        *input_ops: LogicalOperator,
    ):
        super().__init__(*input_ops)

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self._input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs += num_outputs
        return total_num_outputs
