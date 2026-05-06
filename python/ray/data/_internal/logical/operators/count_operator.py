from dataclasses import dataclass, field

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

    input_dependencies: list[LogicalOperator] = field(repr=False, kw_only=True)

    def __post_init__(self):
        super().__post_init__()
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)
        object.__setattr__(self, "name", self.__class__.__name__)
