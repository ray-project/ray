from typing import Tuple

from ray.data._internal.logical.interfaces import LogicalOperator


class Zip(LogicalOperator):
    """Logical operator for zip."""

    def __init__(
        self,
        input_ops: Tuple[LogicalOperator, LogicalOperator],
    ):
        """
        Args:
            input_ops: The pair of operators to zip together.
        """
        super().__init__("Zip", input_ops)
