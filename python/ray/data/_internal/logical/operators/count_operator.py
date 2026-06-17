from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from ray.data._internal.logical.interfaces import LogicalOperator

if TYPE_CHECKING:
    from ray.data.block import Schema

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
    _num_outputs: Optional[int] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        assert len(self.input_dependencies) == 1, len(self.input_dependencies)

    @property
    def num_outputs(self) -> Optional[int]:
        return self._num_outputs

    def infer_schema(self) -> Optional["Schema"]:
        # Fixed output: one row per partial count with a single ``__num_rows``
        # int64 column.
        import pyarrow as pa

        return pa.schema([pa.field(self.COLUMN_NAME, pa.int64(), nullable=False)])
