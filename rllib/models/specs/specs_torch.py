from typing import Tuple, Any, Union

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.specs.specs_base import TensorSpecs


torch, _ = try_import_torch()


@DeveloperAPI
class TorchSpecs(TensorSpecs):
    def get_shape(self, tensor: torch.Tensor) -> Tuple[int]:
        return tuple(tensor.shape)

    def get_dtype(self, tensor: torch.Tensor) -> Any:
        return tensor.dtype

    def _full(
        self, shape: Tuple[int], fill_value: Union[float, int] = 0
    ) -> torch.Tensor:
        return torch.full(shape, fill_value, dtype=self.dtype)
