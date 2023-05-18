from typing import List, Optional, Callable

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import LogicalOperator


class InputData(LogicalOperator):
    """Logical operator for input data.

    This may hold cached blocks from a previous Dataset execution, or
    the arguments for read tasks.
    """

    def __init__(
        self,
        input_data: Optional[List[RefBundle]] = None,
        input_data_factory: Callable[[], List[RefBundle]] = None,
    ):
        super().__init__("InputData", [])
        self.input_data = input_data
        self.input_data_factory = input_data_factory
