from typing import TYPE_CHECKING, List, Union

import ray
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pyarrow


class FromArrowRefs(LogicalOperator):
    """Logical operator for `from_arrow_refs`."""

    def __init__(
        self,
        tables: List[ObjectRef[Union["pyarrow.Table", bytes]]],
        op_name: str = "FromArrowRefs",
    ):
        super().__init__(op_name, [])
        self._tables = tables


class FromArrow(FromArrowRefs):
    """Logical operator for `from_arrow`."""

    def __init__(
        self,
        tables: List[Union["pyarrow.Table", bytes]],
    ):
        super().__init__([ray.put(t) for t in tables], "FromArrow")
