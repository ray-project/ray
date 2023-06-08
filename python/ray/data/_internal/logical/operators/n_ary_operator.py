from typing import List

from ray.data._internal.logical.interfaces import LogicalOperator


class Zip(LogicalOperator):
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
        op_name = f"Zip({left_input_op._name}, {right_input_op._name})"
        super().__init__(op_name, [left_input_op, right_input_op])


class Union(LogicalOperator):
    """Logical operator for union."""

    def __init__(self, *input_ops: List[LogicalOperator]):
        op_name = f"Union({', '.join([op._name for op in input_ops])})"
        super().__init__(
            op_name,
            list(input_ops),
        )
