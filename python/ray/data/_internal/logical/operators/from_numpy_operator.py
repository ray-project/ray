from typing import TYPE_CHECKING, List

import ray
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.types import ObjectRef

if TYPE_CHECKING:
    import numpy as np


class FromNumpyRefs(LogicalOperator):
    """Logical operator for `from_numpy_refs`."""

    def __init__(
        self, ndarrays: List[ObjectRef["np.ndarray"]], op_name: str = "FromNumpyRefs"
    ):
        super().__init__(op_name, [])
        self._ndarrays = ndarrays


class FromNumpy(FromNumpyRefs):
    """Logical operator for `from_numpy`."""

    def __init__(
        self,
        ndarrays: List["np.ndarray"],
    ):
        super().__init__([ray.put(ndarray) for ndarray in ndarrays], "FromNumpy")
