from typing import Any, Dict, List, Union
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

    _torch_reduce_fn = torch.nansum
    _np_reduce_fn = np.nansum

    def python_reduce_fn(self, x, y):
        return sum([x, y])

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
        self._last_reduce_value = 0.0
        # We initialize this to the current time which may result in a low first throughput value
        # It seems reasonable that starting from a checkpoint or starting an experiment results in a low first throughput value
        self._last_throughput_measure_time = time.perf_counter()

    def has_throughputs(self) -> bool:
        return self.track_throughput

    @property
    def throughputs(self) -> Dict[str, float]:
        """Returns the throughput since the last reduce."""
        assert (
            self.has_throughputs()
        ), "Throughput tracking is not enabled on this Stats object"
        return {
            "throughput": (self.peek(compile=True) - self._last_reduce_value)
            / (time.perf_counter() - self._last_throughput_measure_time)
        }

    def reduce(self, compile: bool = True) -> Union[Any, List[Any]]:
        self._last_reduce_value = self.peek(compile=compile)
        return super().reduce(compile)

    def get_state(self) -> Dict[str, Any]:
        """Returns the state of the stats object."""
        state = super().get_state()
        state["series_at_last_reduce"] = self._series_at_last_reduce
        state["track_throughput"] = self.track_throughput
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self._series_at_last_reduce = state["series_at_last_reduce"]
        self.track_throughput = state["track_throughput"]

    def __repr__(self) -> str:
        return f"SumStats({self.peek()}; window={self._window}; len={len(self)}"
