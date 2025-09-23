from collections import defaultdict
from typing import Any, Dict, List, Union, Tuple
import uuid

import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import SeriesStats

torch, _ = try_import_torch()


@DeveloperAPI
class SumStats(SeriesStats):
    """A Stats object that tracks the sum of a series of values."""

    stats_cls_identifier = "sum"

    def __init__(self, **kwargs):
        """Initializes a SumStats instance."""
        super().__init__(**kwargs)

        if not self._clear_on_reduce:
            assert (
                self._window is None
            ), "Lifetime sum must be used with an infinite window"

        # The ID of this Stats instance.
        self._id = str(uuid.uuid4())
        self._prev_merge_values = defaultdict(int)

    def get_state(self) -> Dict[str, Any]:
        """Returns the state of the stats object."""
        state = super().get_state()
        state["id"] = self._id
        state["prev_merge_values"] = self._prev_merge_values
        return state

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

        # Special case: Internal values list is empty -> return NaN or 0.0 for sum.
        if len(values) == 0:
            return [0], []

        return self._torch_or_numpy_reduce(values, torch.nansum, np.nansum)

    def _merge_in_parallel(self, *stats: "SumStats") -> List[Union[int, float]]:
        if not self._clear_on_reduce:
            # For a lifetime sum, we need to subtract the previous merge values to not count
            # older "lifetime counts" more than once.
            merged_sum = 0.0
            for stat in stats:
                if stat._id in self._prev_merge_values:
                    # Subtract "lifetime counts" from the Stat's values to not count
                    # older "lifetime counts" more than once.
                    prev_reduction = self._prev_merge_values[stat._id]
                    new_reduction = stat.peek(compile=True)
                    self.values[-1] -= prev_reduction
                    # Keep track of how many counts we actually gained (for throughput
                    # recomputation).
                    merged_sum += new_reduction - prev_reduction
                    self._prev_merge_values[stat._id] = new_reduction
                else:
                    stat_peek = stat.peek()
                    merged_sum += stat_peek
                    self._prev_merge_values[stat._id] = stat_peek
            return [merged_sum]
        else:
            return [np.nansum([s.reduced_values[0] for s in stats])]

    def __repr__(self) -> str:
        return f"SumStats({self.peek()}; window={self._window}; len={len(self)}"
