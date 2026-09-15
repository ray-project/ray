from typing import Any, Union

import numpy as np

from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics.stats.series import SeriesStats
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()
_, tf, _ = try_import_tf()


@DeveloperAPI
class MeanStats(SeriesStats):
    """A Stats object that tracks the mean of a series of singular values (not vectors).

    Note the following limitation: When merging multiple MeanStats objects, the resulting mean is not the true mean of all values.
    Instead, it is the mean of the means of the incoming MeanStats objects.
    This is because we calculate the mean in parallel components and potentially merge them multiple times in one reduce cycle.
    The resulting mean of means may differ significantly from the true mean, especially if some incoming means are the result of few outliers.

    Example to illustrate this limitation:
    First incoming mean: [1, 2, 3, 4, 5] -> 3
    Second incoming mean: [15] -> 15
    Mean of both merged means: [3, 15] -> 9
    True mean of all values: [1, 2, 3, 4, 5, 15] -> 5
    """

    stats_cls_identifier = "mean"

    def _np_reduce_fn(self, values: np.ndarray) -> float:
        return np.nanmean(values)

    def _torch_reduce_fn(self, values: "torch.Tensor"):
        """Reduce function for torch tensors (stays on GPU)."""
        return torch.nanmean(values.float())

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
                PyTorch GPU tensors are kept on GPU until reduce() or peek().
                TensorFlow tensors are moved to CPU immediately.
        """
        # Convert TensorFlow tensors to CPU immediately, keep PyTorch tensors as-is
        if tf and tf.is_tensor(value):
            value = value.numpy()

        self.values.append(value)

    def reduce(self, compile: bool = True) -> Union[Any, "MeanStats"]:
        reduced_values = self.window_reduce()  # Values are on CPU already after this
        self._set_values([])

        if compile:
            return reduced_values[0]

        return_stats = self.clone()
        return_stats.values = reduced_values
        return return_stats

    def __repr__(self) -> str:
        return f"MeanStats({self.peek()}; window={self._window}; len={len(self)})"
