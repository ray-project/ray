from typing import Tuple, Any

from ray.rllib.utils.framework import try_import_torch
from .specs_base import TensorSpecs


torch, _ = try_import_torch()


class TorchSpecs(TensorSpecs):
    def get_shape(self, tensor: torch.Tensor) -> Tuple[int]:
        return tuple(tensor.shape)

    def get_dtype(self, tensor: torch.Tensor) -> Any:
        return tensor.dtype

    def _sample(self, shape: Tuple[int], fill_value: float = 0.0) -> torch.Tensor:
        return torch.full(shape, fill_value, dtype=self.dtype)
