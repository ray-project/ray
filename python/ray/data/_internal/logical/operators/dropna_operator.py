"""
DropNa logical operator.

This module defines the DropNa logical operator for removing rows with missing
values from Ray datasets.
"""

from typing import Any, Dict, List, Optional

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap


class DropNa(AbstractMap):
    """Logical operator for dropna operation.

    This operator represents the logical intent to remove rows containing
    missing values (null/None/NaN) from a dataset based on specified criteria.

    Examples:
        This operator is used internally by Ray Data's dropna() method.

        Typical usage:
        - Drop rows with any missing values: DropNa(input_op, how="any")
        - Drop rows only if all values are missing: DropNa(input_op, how="all")
        - Drop rows with missing values in specific columns: DropNa(input_op, subset=["col1", "col2"])
        - Keep rows with at least N non-missing values: DropNa(input_op, thresh=2)

    Args:
        input_op: The input logical operator.
        how: Determines which rows to drop. 'any' drops rows with any missing
            values, 'all' drops rows where all values are missing.
        subset: Optional list of column names to consider for missing values.
            If None, all columns will be considered.
        thresh: Optional minimum number of non-missing values required to keep
            a row. If specified, overrides the 'how' parameter.
        compute: Optional compute strategy for the operation.
        ray_remote_args: Optional Ray remote arguments for distributed execution.
    """

    def __init__(
        self,
        input_op: LogicalOperator,
        how: str = "any",
        subset: Optional[List[str]] = None,
        thresh: Optional[int] = None,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "DropNa",
            input_op=input_op,
            compute=compute,
            ray_remote_args=ray_remote_args,
        )
        self._how = how
        self._subset = subset
        self._thresh = thresh
        self._batch_format = "pyarrow"
        self._zero_copy_batch = True

    @property
    def how(self) -> str:
        """The strategy for determining which rows to drop.

        Returns:
            Either 'any' (drop if any column has missing values) or
            'all' (drop only if all columns have missing values).
        """
        return self._how

    @property
    def subset(self) -> Optional[List[str]]:
        """The subset of columns to consider for missing values.

        Returns:
            List of column names, or None if all columns should be considered.
        """
        return self._subset

    @property
    def thresh(self) -> Optional[int]:
        """The minimum number of non-missing values required to keep a row.

        Returns:
            Integer threshold, or None if not using threshold-based dropping.
        """
        return self._thresh

    def can_modify_num_rows(self) -> bool:
        """Check if this operator can modify the number of rows.

        Returns:
            True, as dropna operations can remove rows from the dataset.
        """
        return True
