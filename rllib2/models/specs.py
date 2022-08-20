import abc
from typing import Any, Optional

import numpy as np
import torch

from ray.rllib.utils.typing import TensorType

_INVALID_SHAPE = "Expected shape {} but found {}"
_INVALID_DTYPE = "Expected dtype {} but found {}"


class TensorSpecs(abc.ABC):
    """A base class that specifies the shape and dtype of a tensor."""

    def __init__(self, shape: Any, dtype: Optional[Any] = None) -> None:
        self._shape = shape
        self._dtype = dtype

    @property
    def shape(self):
        """Returns a `tuple` specifying the tensor shape."""
        return self._shape

    @property
    def dtype(self):
        """Returns a dtype specifying the tensor dtype."""
        return self._dtype

    def __repr__(self):
        return f"Tensor(shape={self.shape}, dtype={self.dtype})"

    def __eq__(self, other: "TensorSpecs"):
        """Checks if the shape and dtype of two specs are equal."""
        if not isinstance(other, TensorSpecs):
            return False
        return self.shape == other.shape and self.dtype == other.dtype

    def __ne__(self, other: "TensorSpecs"):
        return not self == other

    def validate(self, tensor: TensorType) -> TensorType:
        if tuple(tensor.shape) != self.shape:
            raise ValueError(_INVALID_SHAPE.format(self.shape, tensor.shape))
        if self.dtype and tensor.dtype != self.dtype:
            raise ValueError(_INVALID_DTYPE.format(self.dtype, tensor.dtype))
        return tensor

    @abc.abstractmethod
    def sample(self, fill_value=0.0) -> TensorType:
        raise NotImplementedError()


class TorchSpecs(TensorSpecs):
    def validate(self, tensor: torch.Tensor) -> torch.Tensor:
        return super().validate(tensor)

    def sample(self, fill_value=0.0) -> torch.Tensor:
        return torch.full(self.shape, fill_value, dtype=self.dtype)


class NPSpecs(TensorSpecs):
    def validate(self, tensor: np.ndarray) -> np.ndarray:
        return super().validate(tensor)

    def sample(self, fill_value=0.0):
        return np.full(self.shape, fill_value, dtype=self.dtype)


class TFSpecs(TensorSpecs):
    # TODO
    pass


class JAXSpecs(TensorSpecs):
    # TODO
    pass
