"""
FillNa logical operator.

This module defines the FillNa logical operator for filling missing values
in Ray datasets with advanced features for production use.
"""

from typing import Any, Dict, List, Optional, Union, Literal

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap

FillMethod = Literal["value", "forward", "backward", "interpolate"]


class FillNa(AbstractMap):
    """Logical operator for fillna operation.

    This operator represents the logical intent to fill missing values
    in a dataset with specified replacement values or methods.

    Examples:
        This operator is used internally by Ray Data's fillna() method.

        Basic usage:
        - Fill all missing values with a scalar: FillNa(input_op, value=0)
        - Fill column-specific values: FillNa(input_op, value={"col1": 0, "col2": "missing"})
        - Fill only specific columns: FillNa(input_op, value=0, subset=["col1", "col2"])

        Advanced usage:
        - Forward fill: FillNa(input_op, method="forward")
        - Backward fill: FillNa(input_op, method="backward")
        - Limit fill operations: FillNa(input_op, value=0, limit=5)

    Args:
        input_op: The input logical operator.
        value: Value(s) to use for filling missing entries. Can be a scalar
            value to fill all columns, or a dictionary mapping column names
            to fill values for column-specific filling. Required if method="value".
        method: Method to use for filling missing values. Options:
            - "value": Fill with specified values (default)
            - "forward": Forward fill (propagate last valid observation forward)
            - "backward": Backward fill (propagate next valid observation backward)
            - "interpolate": Linear interpolation (numeric columns only)
        subset: Optional list of column names to restrict the filling operation.
            If None, all columns will be processed.
        limit: Maximum number of consecutive missing values to fill. If None,
            fill all missing values.
        inplace: Whether to modify the dataset in-place. Note: Ray Data datasets
            are immutable, so this always returns a new dataset.
        compute: Optional compute strategy for the operation.
        ray_remote_args: Optional Ray remote arguments for distributed execution.

    Raises:
        ValueError: If method is not supported or if value is required but not provided.
    """

    def __init__(
        self,
        input_op: LogicalOperator,
        value: Union[Any, Dict[str, Any]] = None,
        method: FillMethod = "value",
        subset: Optional[List[str]] = None,
        limit: Optional[int] = None,
        inplace: bool = False,  # For pandas compatibility, ignored in Ray Data
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "FillNa",
            input_op=input_op,
            compute=compute,
            ray_remote_args=ray_remote_args,
        )

        # Validate parameters
        if method == "value" and value is None:
            raise ValueError("'value' parameter is required when method='value'")

        if method not in ["value", "forward", "backward", "interpolate"]:
            raise ValueError(
                f"Unsupported method '{method}'. Must be one of: value, forward, backward, interpolate"
            )

        if limit is not None and limit < 0:
            raise ValueError("'limit' must be non-negative")

        self._value = value
        self._method = method
        self._subset = subset
        self._limit = limit
        self._inplace = inplace  # Store for API compatibility
        self._batch_format = "pyarrow"
        self._zero_copy_batch = True

    @property
    def value(self) -> Union[Any, Dict[str, Any]]:
        """The fill value(s) to use for replacing missing entries.

        Returns:
            Either a scalar value or a dictionary of column-specific values.
        """
        return self._value

    @property
    def method(self) -> FillMethod:
        """The method used for filling missing values.

        Returns:
            The filling method: "value", "forward", "backward", or "interpolate".
        """
        return self._method

    @property
    def subset(self) -> Optional[List[str]]:
        """The subset of columns to apply the fill operation to.

        Returns:
            List of column names, or None if all columns should be processed.
        """
        return self._subset

    @property
    def limit(self) -> Optional[int]:
        """The maximum number of consecutive missing values to fill.

        Returns:
            Integer limit, or None if no limit is applied.
        """
        return self._limit

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
            False, as fillna operations preserve the number of rows.
        """
        return False
