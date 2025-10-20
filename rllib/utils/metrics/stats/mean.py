from typing import Any, List, Union
import numpy as np

from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import SeriesStats
from ray.rllib.utils.metrics.stats.utils import single_value_to_cpu


@DeveloperAPI
class MeanStats(SeriesStats):
    """A Stats object that tracks the mean of a series of values."""

    stats_cls_identifier = "mean"

    def _np_reduce_fn(self, values):
        return np.nanmean(values)

    def push(self, value: Any) -> None:
        value = single_value_to_cpu(value)
        self.values.append(value)

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        reduced_values = self.window_reduce()
        return reduced_values[0] if compile else reduced_values

    def reduce(self, compile: bool = True) -> Union[Any, "MeanStats"]:
        if self._reduce_at_root and not self._is_root_stats:
            if compile:
                raise ValueError(
                    "Can not compile at leaf level if reduce_at_root is True"
                )
            return_stats = self.clone(self)
            return_stats.values = self.values
            return return_stats

        reduced_values = self.window_reduce()
        self._set_values([])

        if compile:
            return reduced_values[0]

        return_stats = self.clone(self)
        return_stats.values = reduced_values
        return return_stats

    def __repr__(self) -> str:
        return f"MeanStats({self.peek()}; window={self._window}; len={len(self)}"
