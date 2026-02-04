from dataclasses import dataclass
from typing import Optional

from ray.data._internal.logical.interfaces import LogicalOperator

__all__ = [
    "Count",
]


@dataclass(frozen=True, repr=False)
class Count(LogicalOperator):
    """Logical operator that represents counting the number of rows in inputs.

    Physical operators that implement this logical operator should produce one or more
    rows with a single column named `Count.COLUMN_NAME`. When you sum the values in
    this column, you should get the total number of rows in the dataset.
    """

    COLUMN_NAME = "__num_rows"

    input_op: Optional[LogicalOperator] = None

    def __post_init__(self) -> None:
        if not self.input_dependencies and self.input_op is not None:
            object.__setattr__(self, "input_dependencies", (self.input_op,))
        if self.name is None:
            object.__setattr__(self, "name", "Count")
        super().__post_init__()
