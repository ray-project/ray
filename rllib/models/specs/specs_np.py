from typing import Tuple, Any, Union, Type
import numpy as np

from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.models.specs.specs_base import TensorSpecs


@DeveloperAPI
class NPSpecs(TensorSpecs):
    @override(TensorSpecs)
    def get_type(cls) -> Type:
        return np.ndarray

    @override(TensorSpecs)
    def get_shape(self, tensor: np.ndarray) -> Tuple[int]:
        return tuple(tensor.shape)

    @override(TensorSpecs)
    def get_dtype(self, tensor: np.ndarray) -> Any:
        return tensor.dtype

    @override(TensorSpecs)
    def _full(self, shape: Tuple[int], fill_value: Union[float, int] = 0) -> np.ndarray:
        return np.full(shape, fill_value, dtype=self.dtype)
