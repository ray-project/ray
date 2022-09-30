import numpy as np

from .specs_base import TensorSpecs


class NPSpecs(TensorSpecs):
    def validate(self, tensor: np.ndarray) -> np.ndarray:
        return super().validate(tensor)

    def sample(self, fill_value=0.0):
        return np.full(self.shape, fill_value, dtype=self.dtype)
