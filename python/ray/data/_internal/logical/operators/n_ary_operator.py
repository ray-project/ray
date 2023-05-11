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
        super().__init__("Zip", [left_input_op, right_input_op])
