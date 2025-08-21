"""
FillNa logical operator.

This module defines the FillNa logical operator for filling missing values
in Ray datasets.
"""

from typing import Any, Dict, List, Optional, Union

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap


class FillNa(AbstractMap):
    """Logical operator for fillna operation.

    This operator represents the logical intent to fill missing values
    (null/None/NaN) in a dataset with specified replacement values.

    Examples:
        This operator is used internally by Ray Data's fillna() method.

        Typical usage:
        - Fill all missing values with a scalar: FillNa(input_op, value=0)
        - Fill column-specific values: FillNa(input_op, value={"col1": 0, "col2": "missing"})
        - Fill only specific columns: FillNa(input_op, value=0, subset=["col1", "col2"])

    Args:
        input_op: The input logical operator.
        value: Value(s) to use for filling missing entries. Can be a scalar
            value to fill all columns, or a dictionary mapping column names
            to fill values for column-specific filling.
        subset: Optional list of column names to restrict the filling operation.
            If None, all columns will be processed.
        compute: Optional compute strategy for the operation.
        ray_remote_args: Optional Ray remote arguments for distributed execution.
    """

    def __init__(
        self,
        input_op: LogicalOperator,
        value: Union[Any, Dict[str, Any]],
        subset: Optional[List[str]] = None,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "FillNa",
            input_op=input_op,
            compute=compute,
            ray_remote_args=ray_remote_args,
        )
        self._value = value
        self._subset = subset
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
    def subset(self) -> Optional[List[str]]:
        """The subset of columns to apply the fill operation to.

        Returns:
            List of column names, or None if all columns should be processed.
        """
        return self._subset

    def can_modify_num_rows(self) -> bool:
        """Check if this operator can modify the number of rows.

        Returns:
            False, as fillna operations preserve the number of rows.
        """
        return False
