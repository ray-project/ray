import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.stats.series import SeriesStats
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()


@DeveloperAPI
class MinStats(SeriesStats):
    """A Stats object that tracks the min of a series of singular values (not vectors)."""

    stats_cls_identifier = "min"

    def _np_reduce_fn(self, values: np.ndarray) -> float:
        return np.nanmin(values)

    def _torch_reduce_fn(self, values: "torch.Tensor"):
        """Reduce function for torch tensors (stays on GPU)."""
        # torch.nanmin not available, use workaround
        clean_values = values[~torch.isnan(values)]
        if len(clean_values) == 0:
            return torch.tensor(float("nan"), device=values.device)
        # Cast to float32 to avoid errors from Long tensors
        return torch.min(clean_values.float())

    def __repr__(self) -> str:
        return f"MinStats({self.peek()}; window={self._window}; len={len(self)})"
