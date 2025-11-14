"""
FillNa logical operator.
"""

from typing import Any, Dict, List, Literal, Optional, Union

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap

FillMethod = Literal["value", "forward", "backward", "interpolate"]


class FillNa(AbstractMap):
    """Logical operator for fillna operation."""

    def __init__(
        self,
        input_op: LogicalOperator,
        value: Union[Any, Dict[str, Any]] = None,
        method: FillMethod = "value",
        subset: Optional[List[str]] = None,
        limit: Optional[int] = None,
        inplace: bool = False,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "FillNa",
            input_op=input_op,
            compute=compute,
            ray_remote_args=ray_remote_args,
        )

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
        self._inplace = inplace
        self._batch_format = "pyarrow"
        self._zero_copy_batch = True

    @property
    def value(self) -> Union[Any, Dict[str, Any]]:
        return self._value

    @property
    def method(self) -> FillMethod:
        return self._method

    @property
    def subset(self) -> Optional[List[str]]:
        return self._subset

    @property
    def limit(self) -> Optional[int]:
        return self._limit

    @property
    def inplace(self) -> bool:
        return self._inplace

    def can_modify_num_rows(self) -> bool:
        return False
