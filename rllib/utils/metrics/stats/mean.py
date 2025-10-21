from typing import Any, Union
import numpy as np

from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.stats.series import SeriesStats

torch, _ = try_import_torch()


@DeveloperAPI
class MeanStats(SeriesStats):
    """A Stats object that tracks the mean of a series of values."""

    stats_cls_identifier = "mean"

    def _np_reduce_fn(self, values):
        return np.nanmean(values)

    def _torch_reduce_fn(self, values):
        """Reduce function for torch tensors (stays on GPU)."""
        return torch.nanmean(values)

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
                PyTorch GPU tensors are kept on GPU until reduce() or peek().
                TensorFlow tensors are moved to CPU immediately.
        """
        from ray.rllib.utils.framework import try_import_tf

        _, tf, _ = try_import_tf()

        # Convert TensorFlow tensors to CPU immediately, keep PyTorch tensors as-is
        if tf and tf.is_tensor(value):
            value = value.numpy()

        self.values.append(value)

    def reduce(self, compile: bool = True) -> Union[Any, "MeanStats"]:
        reduced_values = self.window_reduce()
        self._set_values([])

        if compile:
            return reduced_values[0]

        return_stats = self.clone(self)
        return_stats.values = reduced_values
        return return_stats

    def __repr__(self) -> str:
        return f"MeanStats({self.peek()}; window={self._window}; len={len(self)}"
