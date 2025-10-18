import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import SeriesStats

torch, _ = try_import_torch()


@DeveloperAPI
class MinStats(SeriesStats):
    """A Stats object that tracks the min of a series of values."""

    stats_cls_identifier = "min"

    def _np_reduce_fn(self, values):
        return np.nanmin(values)

    def __repr__(self) -> str:
        return (
            f"MinStats({self.peek()}; window={self._window}; len={len(self)}; "
            f"clear_on_reduce={self._clear_on_reduce})"
        )
