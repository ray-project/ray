from typing import TYPE_CHECKING, List, Union

import ray
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
        self._ndarrays: List[ObjectRef["np.ndarray"]] = []
        for arr_or_ref in ndarrays:
            if isinstance(arr_or_ref, ray.ObjectRef):
                self._ndarrays.append(arr_or_ref)
            else:
                self._ndarrays.append(ray.put(arr_or_ref))
