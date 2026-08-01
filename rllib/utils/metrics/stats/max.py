import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.stats.series import SeriesStats
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()


@DeveloperAPI
class MaxStats(SeriesStats):
    """A Stats object that tracks the max of a series of singular values (not vectors)."""

    stats_cls_identifier = "max"

    def _np_reduce_fn(self, values):
        return np.nanmax(values)

    def _torch_reduce_fn(self, values):
        """Reduce function for torch tensors (stays on GPU)."""
        # torch.nanmax not available, use workaround
        clean_values = values[~torch.isnan(values)]
        if len(clean_values) == 0:
            return torch.tensor(float("nan"), device=values.device)
        # Cast to float32 to avoid errors from Long tensors
        return torch.max(clean_values.float())

    def __repr__(self) -> str:
        return f"MaxStats({self.peek()}; window={self._window}; len={len(self)})"
