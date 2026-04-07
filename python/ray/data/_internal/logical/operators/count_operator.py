from dataclasses import InitVar, dataclass, field
from typing import Callable, Optional

from ray.data._internal.logical.interfaces import LogicalOperator

__all__ = [
    "Count",
]


@dataclass(frozen=True, repr=False, eq=False)
class Count(LogicalOperator):
    """Logical operator that represents counting the number of rows in inputs.

    Physical operators that implement this logical operator should produce one or more
    rows with a single column named `Count.COLUMN_NAME`. When you sum the values in
    this column, you should get the total number of rows in the dataset.
    """

    COLUMN_NAME = "__num_rows"

    input_op: InitVar[LogicalOperator]
    _name: str = field(init=False, repr=False)
    _input_dependencies: list[LogicalOperator] = field(init=False, repr=False)
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self, input_op: LogicalOperator):
        assert isinstance(input_op, LogicalOperator), input_op
        object.__setattr__(self, "_name", self.__class__.__name__)
        object.__setattr__(self, "_input_dependencies", [input_op])
        object.__setattr__(self, "_num_outputs", None)

    @property
    def num_outputs(self) -> Optional[int]:
        return self._num_outputs

    def _apply_transform(
        self, transform: Callable[[LogicalOperator], LogicalOperator]
    ) -> LogicalOperator:
        transformed_input = self.input_dependencies[0]._apply_transform(transform)
        target: LogicalOperator
        if transformed_input is self.input_dependencies[0]:
            target = self
        else:
            target = Count(transformed_input)
        return transform(target)
