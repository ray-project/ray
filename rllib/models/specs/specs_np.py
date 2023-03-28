from typing import Tuple, Any, Union, Type
import numpy as np

from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.models.specs.specs_base import TensorSpec


@DeveloperAPI
class NPTensorSpec(TensorSpec):
    @override(TensorSpec)
    def get_type(cls) -> Type:
        return np.ndarray

    @override(TensorSpec)
    def get_shape(self, tensor: np.ndarray) -> Tuple[int]:
        return tuple(tensor.shape)

    @override(TensorSpec)
    def get_dtype(self, tensor: np.ndarray) -> Any:
        return tensor.dtype

    @override(TensorSpec)
    def _full(self, shape: Tuple[int], fill_value: Union[float, int] = 0) -> np.ndarray:
        return np.full(shape, fill_value, dtype=self.dtype)
