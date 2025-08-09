"""
DropNa logical operator.
"""

from typing import Any, Dict, List, Optional

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap


class DropNa(AbstractMap):
    """Logical operator for dropna operation."""

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
        return self._how

    @property
    def subset(self) -> Optional[List[str]]:
        return self._subset

    @property
    def thresh(self) -> Optional[int]:
        return self._thresh

    def can_modify_num_rows(self) -> bool:
        return True
