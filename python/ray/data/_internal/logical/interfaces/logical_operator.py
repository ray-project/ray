from typing import List, Optional

from .operator import Operator


class LogicalOperator(Operator):
    """Abstract class for logical operators.

    A logical operator describes transformation, and later is converted into
    physical operator.
    """

    def __init__(
        self,
        name: str,
        input_dependencies: List["LogicalOperator"],
        target_max_block_size: Optional[int] = None,
    ):
        super().__init__(
            name, input_dependencies, target_max_block_size=target_max_block_size
        )
        for x in input_dependencies:
            assert isinstance(x, LogicalOperator), x
