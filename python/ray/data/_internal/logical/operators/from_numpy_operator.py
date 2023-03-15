from typing import TYPE_CHECKING, List, Union

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.types import ObjectRef

if TYPE_CHECKING:
    import numpy as np


class FromNumpyRefs(LogicalOperator):
    """Logical operator for `from_numpy_refs`."""

    def __init__(
        self,
        ndarrays: Union[List[ObjectRef["np.ndarray"]], List["np.ndarray"]],
        op_name: str = "FromNumpyRefs",
    ):
        super().__init__(op_name, [])
        self._ndarrays = ndarrays
