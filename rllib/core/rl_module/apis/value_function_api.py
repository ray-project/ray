import abc
from typing import Any, Dict, Optional

from ray.rllib.utils.typing import TensorType


class ValueFunctionAPI(abc.ABC):
    """An API to be implemented by RLModules for handling value function-based learning.

    RLModules implementing this API must override the `compute_values` method.
    """

    @abc.abstractmethod
    def compute_values(
        self,
        batch: Dict[str, Any],
        embeddings: Optional[Any] = None,
    ) -> TensorType:
        """Computes the value estimates given `batch`.

        Args:
            batch: The batch to compute value function estimates for.
            embeddings: Optional embeddings already computed from the `batch` (by
                another forward pass through the model's encoder (or other subcomponent
                that computes an embedding). For example, the caller of thie method
                should provide `embeddings` - if available - to avoid duplicate passes
                through a shared encoder.

        Returns:
            A tensor of shape (B,) or (B, T) (in case the input `batch` has a
            time dimension. Note that the last value dimension should already be
            squeezed out (not 1!).
        """
