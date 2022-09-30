from typing import Tuple, Any
import numpy as np

from .specs_base import TensorSpecs

class NPSpecs(TensorSpecs):

    def get_shape(self, tensor: np.ndarray) -> Tuple[int]:
        return tuple(tensor.shape)

    def get_dtype(self, tensor: np.ndarray) -> Any:
        return tensor.dtype

    def _sample(self, 
        shape: Tuple[int], 
        fill_value: float = 0.0
    ) -> np.ndarray:
        return np.full(shape, fill_value, dtype=self.dtype)
