from typing import Any, List, Union, Tuple

import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import SeriesStats

torch, _ = try_import_torch()


@DeveloperAPI
class MinStats(SeriesStats):
    """A Stats object that tracks the min of a series of values."""

    stats_cls_identifier = "min"

    @property
    def reduced_values(self, values=None) -> Tuple[Any, Any]:
        """A non-committed reduction procedure on given values (or `self.values`).

        Note that this method does NOT alter any state of `self` or the possibly
        provided list of `values`. It only returns new values as they should be
        adopted after a possible, actual reduction step.

        Args:
            values: The list of values to reduce. If not None, use `self.values`

        Returns:
            A tuple containing 1) the reduced values and 2) the new internal values list
            to be used. If there is no reduciton method, the reduced values will be the same as the values.
        """
        values = values if values is not None else self.values

        # Special case: Internal values list is empty -> return NaN or 0.0 for min.
        if len(values) == 0:
            return [np.nan], []

        return self._torch_or_numpy_reduce(values, torch.fmin, np.nanmin)

    def _merge_in_parallel(self, *stats: "MinStats") -> List[Union[int, float]]:
        return np.nanmin([s.reduced_values[0] for s in stats])

    def __repr__(self) -> str:
        return (
            f"MinStats({self.peek()}; window={self._window}; len={len(self)}; "
            f"clear_on_reduce={self._clear_on_reduce})"
        )
