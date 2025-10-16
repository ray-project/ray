from typing import Any, Dict, List, Union, Tuple
import uuid
import time
import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import SeriesStats

torch, _ = try_import_torch()


@DeveloperAPI
class SumStats(SeriesStats):
    """A Stats object that tracks the sum of a series of values."""

    stats_cls_identifier = "sum"

    def __init__(self, throughput: bool = False, **kwargs):
        """Initializes a SumStats instance.

        Args:
            throughput: If True, track a throughput estimate based on the time between consecutive calls to reduce().
        """
        super().__init__(**kwargs)

        if not self._clear_on_reduce:
            assert (
                self._window is not None
            ), "For lifetime sum, use LifetimeSumStats class."

        self.track_throughput = throughput
        # We need to initialize this to None becasue if we restart from a checkpoint, we should start over with througput calculation
        self._value_at_last_reduce = None
        # We initialize this to the current time which may result in a low first throughput value
        # It seems reasonable that starting from a checkpoint or starting an experiment results in a low first throughput value
        self._last_throughput_measure_time = time.perf_counter()

        # The ID of this Stats instance.
        self._id = str(uuid.uuid4())

    def get_state(self) -> Dict[str, Any]:
        """Returns the state of the stats object."""
        state = super().get_state()
        state["id"] = self._id
        state["value_at_last_reduce"] = self._value_at_last_reduce
        state["track_throughput"] = self.track_throughput
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
        return [np.nansum([s.stats[0] for s in stats])]

    def __repr__(self) -> str:
        return f"SumStats({self.peek()}; window={self._window}; len={len(self)}"
