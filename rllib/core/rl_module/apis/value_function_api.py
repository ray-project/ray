import abc
from typing import Any, Dict

from ray.rllib.utils.typing import TensorType


class ValueFunctionAPI(abc.ABC):
    """An API to be implemented by RLModules for handling value function-based learning.

    RLModules implementing this API must override the `compute_values` method."""

    @abc.abstractmethod
    def compute_values(self, batch: Dict[str, Any]) -> TensorType:
        """Computes the value estimates given `batch`.

        Args:
            batch: The batch to compute value function estimates for.

        Returns:
            A tensor of shape (B,) or (B, T) (in case the input `batch` has a
            time dimension. Note that the last value dimension should already be
            squeezed out (not 1!).
        """
