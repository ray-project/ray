from typing import List

from .operator import Operator


class LogicalOperator(Operator):
    """Abstract class for logical operators.

    A logical operator describes transformation, and later is converted into
    physical operator.
    """

    def __init__(self, name: str, input_dependencies: List["LogicalOperator"]):
        super().__init__(name, input_dependencies)
        for x in input_dependencies:
            assert isinstance(x, LogicalOperator), x
