from typing import Any, List, Union
import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import SeriesStats
from ray.rllib.utils.metrics.stats.utils import single_value_to_cpu

torch, _ = try_import_torch()


@DeveloperAPI
class MeanStats(SeriesStats):
    """A Stats object that tracks the mean of a series of values."""

    stats_cls_identifier = "mean"

    def _torch_reduce_fn(self, values):
        return torch.nanmean(values)

    def _np_reduce_fn(self, values):
        return np.nanmean(values)

    def python_reduce_fn(self, x, y):
        raise ValueError("window=None is not supported for mean reduction.")

    def push(self, value: Any) -> None:
        value = single_value_to_cpu(value)
        self.values.append(value)

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        reduced_values = self.window_reduce()
        return reduced_values[0] if compile else reduced_values

    def reduce(self, compile: bool = True) -> Union[Any, List[Any]]:
        reduced_values = self.window_reduce()
        if self._clear_on_reduce:
            self._set_values([])
        else:
            self.values = reduced_values
        return reduced_values[0] if compile else reduced_values

    def __repr__(self) -> str:
        return (
            f"MeanStats({self.peek()}; window={self._window}; len={len(self)}; "
            f"clear_on_reduce={self._clear_on_reduce})"
        )
