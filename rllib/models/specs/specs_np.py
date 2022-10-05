from typing import Tuple, Any, Union
import numpy as np

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.models.specs.specs_base import TensorSpecs


@DeveloperAPI
class NPSpecs(TensorSpecs):
    def get_shape(self, tensor: np.ndarray) -> Tuple[int]:
        return tuple(tensor.shape)

    def get_dtype(self, tensor: np.ndarray) -> Any:
        return tensor.dtype

    def _full(self, shape: Tuple[int], fill_value: Union[float, int] = 0) -> np.ndarray:
        return np.full(shape, fill_value, dtype=self.dtype)
