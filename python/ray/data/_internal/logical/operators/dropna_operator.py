"""
DropNa logical operator.

This module defines the DropNa logical operator for removing rows with missing
values from Ray datasets with advanced features for production use.
"""

from typing import Any, Dict, List, Optional, Literal

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap

DropMethod = Literal["any", "all"]


class DropNa(AbstractMap):
    """Logical operator for dropna operation.

    This operator represents the logical intent to remove rows containing
    missing values from a dataset based on specified criteria.

    Examples:
        This operator is used internally by Ray Data's dropna() method.

        Basic usage:
        - Drop rows with any missing values: DropNa(input_op, how="any")
        - Drop rows only if all values are missing: DropNa(input_op, how="all")
        - Drop rows with missing values in specific columns: DropNa(input_op, subset=["col1", "col2"])
        - Keep rows with at least N non-missing values: DropNa(input_op, thresh=2)

        Advanced usage:
        - Treat additional values as missing: DropNa(input_op, ignore_values=[0, ""])

    Args:
        input_op: The input logical operator.
        how: Determines which rows to drop. Options:
            - 'any': Drop rows with any missing values (default)
            - 'all': Drop rows where all values are missing
        subset: Optional list of column names to consider for missing values.
            If None, all columns will be considered.
        thresh: Optional minimum number of non-missing values required to keep
            a row. If specified, overrides the 'how' parameter.
        ignore_values: Optional list of additional values to treat as missing
            (beyond None and NaN). Useful for treating empty strings, zeros, etc.
            as missing values.
        inplace: Whether to modify the dataset in-place. Note: Ray Data datasets
            are immutable, so this always returns a new dataset.
        compute: Optional compute strategy for the operation.
        ray_remote_args: Optional Ray remote arguments for distributed execution.

    Raises:
        ValueError: If 'how' is not "any" or "all", or if thresh is negative.
    """

    def __init__(
        self,
        input_op: LogicalOperator,
        how: DropMethod = "any",
        subset: Optional[List[str]] = None,
        thresh: Optional[int] = None,
        ignore_values: Optional[List[Any]] = None,
        inplace: bool = False,  # For pandas compatibility, ignored in Ray Data
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "DropNa",
            input_op=input_op,
            compute=compute,
            ray_remote_args=ray_remote_args,
        )

        # Validate parameters
        if how not in ["any", "all"]:
            raise ValueError(f"'how' must be 'any' or 'all', got '{how}'")

        if thresh is not None and thresh < 0:
            raise ValueError("'thresh' must be non-negative")

        if thresh is not None and subset is not None and thresh > len(subset):
            raise ValueError(
                "'thresh' cannot be greater than the number of columns in 'subset'"
            )

        self._how = how
        self._subset = subset
        self._thresh = thresh
        self._ignore_values = ignore_values or []
        self._inplace = inplace  # Store for API compatibility
        self._batch_format = "pyarrow"
        self._zero_copy_batch = True

    @property
    def how(self) -> DropMethod:
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

    @property
    def ignore_values(self) -> List[Any]:
        """Additional values to treat as missing beyond None and NaN.

        Returns:
            List of values to treat as missing.
        """
        return self._ignore_values

    @property
    def inplace(self) -> bool:
        """Whether to modify the dataset in-place (always False for Ray Data).

        Returns:
            False, as Ray Data datasets are immutable.
        """
        return self._inplace

    def can_modify_num_rows(self) -> bool:
        """Check if this operator can modify the number of rows.

        Returns:
            True, as dropna operations can remove rows from the dataset.
        """
        return True
