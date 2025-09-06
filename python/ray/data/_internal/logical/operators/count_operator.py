from typing import List

from ray.data._internal.logical.interfaces import LogicalOperator


class Count(LogicalOperator):
    """Logical operator that represents counting the number of rows in inputs.

    Physical operators that implement this logical operator should produce one or more
    rows with a single column named `Count.COLUMN_NAME`. When you sum the values in
    this column, you should get the total number of rows in the dataset.
    """

    COLUMN_NAME = "__num_rows"

    def __init__(
        self,
        input_dependencies: List["LogicalOperator"],
    ):
        super().__init__("Count", input_dependencies)
