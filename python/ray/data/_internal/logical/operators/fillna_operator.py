"""
FillNa logical operator.
"""

from typing import Any, Dict, List, Optional

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data._internal.compute import ComputeStrategy


class FillNa(AbstractMap):
    """Logical operator for fillna operation."""

    def __init__(
        self,
        input_op: LogicalOperator,
        value: Any,
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
    def value(self) -> Any:
        return self._value

    @property
    def subset(self) -> Optional[List[str]]:
        return self._subset

    def can_modify_num_rows(self) -> bool:
        return False
