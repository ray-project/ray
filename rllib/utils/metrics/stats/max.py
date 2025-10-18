import numpy as np

from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import SeriesStats


@DeveloperAPI
class MaxStats(SeriesStats):
    """A Stats object that tracks the max of a series of values."""

    stats_cls_identifier = "max"

    def _np_reduce_fn(self, values):
        return np.nanmax(values)

    def __repr__(self) -> str:
        return (
            f"MaxStats({self.peek()}; window={self._window}; len={len(self)}; "
            f"clear_on_reduce={self._clear_on_reduce})"
        )
