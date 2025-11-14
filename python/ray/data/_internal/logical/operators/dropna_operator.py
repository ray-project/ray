"""
DropNa logical operator.
"""

from typing import Any, Dict, List, Literal, Optional

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap

DropMethod = Literal["any", "all"]


class DropNa(AbstractMap):
    """Logical operator for dropna operation."""

    def __init__(
        self,
        input_op: LogicalOperator,
        how: DropMethod = "any",
        subset: Optional[List[str]] = None,
        thresh: Optional[int] = None,
        ignore_values: Optional[List[Any]] = None,
        inplace: bool = False,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            "DropNa",
            input_op=input_op,
            compute=compute,
            ray_remote_args=ray_remote_args,
        )

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
        self._inplace = inplace
        self._batch_format = "pyarrow"
        self._zero_copy_batch = True

    @property
    def how(self) -> DropMethod:
        return self._how

    @property
    def subset(self) -> Optional[List[str]]:
        return self._subset

    @property
    def thresh(self) -> Optional[int]:
        return self._thresh

    @property
    def ignore_values(self) -> List[Any]:
        return self._ignore_values

    @property
    def inplace(self) -> bool:
        return self._inplace

    def can_modify_num_rows(self) -> bool:
        return True
