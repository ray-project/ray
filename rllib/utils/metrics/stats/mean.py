import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import SeriesStats

torch, _ = try_import_torch()


@DeveloperAPI
class MeanStats(SeriesStats):
    """A Stats object that tracks the mean of a series of values."""

    stats_cls_identifier = "mean"
    _torch_reduce_fn = torch.nanmean
    _np_reduce_fn = np.nanmean

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self._window is None:
            raise ValueError("window=None is not supported for mean reduction.")

    def __repr__(self) -> str:
        return (
            f"MeanStats({self.peek()}; window={self._window}; len={len(self)}; "
            f"sum_up_parallel_values={self._sum_up_parallel_values})"
        )
