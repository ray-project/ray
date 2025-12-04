import time
from typing import Any, Dict, Union

import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.stats.series import SeriesStats
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()


@DeveloperAPI
class SumStats(SeriesStats):
    """A Stats object that tracks the sum of a series of singular values (not vectors)."""

    stats_cls_identifier = "sum"

    def _np_reduce_fn(self, values: np.ndarray) -> float:
        return np.nansum(values)

    def _torch_reduce_fn(self, values: "torch.Tensor"):
        """Reduce function for torch tensors (stays on GPU)."""
        # torch.nansum not available, use workaround
        clean_values = values[~torch.isnan(values)]
        if len(clean_values) == 0:
            return torch.tensor(0.0, device=values.device)
        return torch.sum(clean_values.float())

    def __init__(self, with_throughput: bool = False, **kwargs):
        """Initializes a SumStats instance.

        Args:
            throughput: If True, track a throughput estimate based on the time between consecutive calls to reduce().
        """
        super().__init__(**kwargs)

        self.track_throughput = with_throughput
        # We initialize this to the current time which may result in a low first throughput value
        # It seems reasonable that starting from a checkpoint or starting an experiment results in a low first throughput value
        self._last_throughput_measure_time = time.perf_counter()

    def initialize_throughput_reference_time(self, time: float) -> None:
        assert (
            self.is_root
        ), "initialize_throughput_reference_time can only be called on root stats"
        self._last_throughput_measure_time = time

    @property
    def has_throughputs(self) -> bool:
        return self.track_throughput

    @property
    def throughputs(self) -> float:
        """Returns the throughput since the last reduce."""
        assert (
            self.track_throughput
        ), "Throughput tracking is not enabled on this Stats object"

        return self.peek(compile=True) / (
            time.perf_counter() - self._last_throughput_measure_time
        )

    def reduce(self, compile: bool = True) -> Union[Any, "SumStats"]:
        reduce_value = super().reduce(compile=True)

        # Update the last throughput measure time for correct throughput calculations
        if self.track_throughput:
            self._last_throughput_measure_time = time.perf_counter()

        if compile:
            return reduce_value

        return_stats = self.clone()
        return_stats.values = [reduce_value]
        return return_stats

    @staticmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        super_args = SeriesStats._get_init_args(stats_object=stats_object, state=state)
        if state is not None:
            return {
                **super_args,
                "with_throughput": state["track_throughput"],
            }
        elif stats_object is not None:
            return {
                **super_args,
                "with_throughput": stats_object.track_throughput,
            }
        else:
            raise ValueError("Either stats_object or state must be provided")

    def get_state(self) -> Dict[str, Any]:
        """Returns the state of the stats object."""
        state = super().get_state()
        state["track_throughput"] = self.track_throughput
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self.track_throughput = state["track_throughput"]

    def __repr__(self) -> str:
        return f"SumStats({self.peek()}; window={self._window}; len={len(self)}"
