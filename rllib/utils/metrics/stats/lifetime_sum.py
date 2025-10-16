from typing import Any, Dict, List, Union
import time

from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.base import StatsBase

torch, _ = try_import_torch()


@DeveloperAPI
class LifetimeSumStats(StatsBase):
    """A Stats object that tracks the sum of a series of values."""

    stats_cls_identifier = "lifetime_sum"

    def __init__(self, track_throughput_since_last_restore: bool = False, **kwargs):
        """Initializes a LifetimeSumStats instance.

        Args:
            track_throughput_since_last_restore: If True, track the throughput since the last restore from a checkpoint.
        """
        super().__init__(**kwargs)

        self._item = 0.0

        self.track_throughput_last_restore = track_throughput_since_last_restore
        # We need to initialize this to None becasue if we restart from a checkpoint, we should start over with througput calculation
        self._value_at_last_reduce = None
        # We initialize this to the current time which may result in a low first throughput value
        # It seems reasonable that starting from a checkpoint or starting an experiment results in a low first throughput value
        self._last_reduce_time = time.perf_counter()
        self._last_restore_time = time.perf_counter()

    def get_state(self) -> Dict[str, Any]:
        """Returns the state of the stats object."""
        state = super().get_state()
        state["id"] = self._id
        state["prev_merge_values"] = self._prev_merge_values
        state["value_at_last_reduce"] = self._value_at_last_reduce
        state["track_throughput_last_restore"] = self.track_throughput_last_restore
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self._id = state["id"]
        self._prev_merge_values = state["prev_merge_values"]
        self._value_at_last_reduce = state["value_at_last_reduce"]
        self.track_throughput_last_restore = state["track_throughput_last_restore"]
        self.value_at_last_restore = self._item

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
        """
        if not self._is_tensor and torch and torch.is_tensor(value):
            self._is_tensor = True
            # turn item into a tensor
            self._item = torch.tensor(self._item)

        self._item += value

    def throughput_since_last_reduce(self) -> float:
        """Returns the throughput since the last reduce."""
        if self.track_throughput_last_restore:
            return self._item / (time.perf_counter() - self._last_reduce_time)
        else:
            raise ValueError(
                "Tracking of throughput since last reduce is not enabled on this Stats object"
            )

    @property
    def throughput_since_last_restore(self) -> float:
        """Returns the throughput total."""
        if self.track_throughput_last_restore:
            return self._item / (time.perf_counter() - self._last_restore_time)
        else:
            raise ValueError(
                "Tracking of throughput since last restore is not enabled on this Stats object"
            )

    def reduce(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Reduces the internal value.

        Args:
            compile: If True, the result is compiled into a single value if possible.
                If it is not possible, the result is a list of values.
                If False, the result is a list of one or more values.
        """
        if not self._is_root_stats:
            # On leaves, we need to return the current value and reset it to 0
            # Otherwise, we would be adding accumulated values at the root multiple times
            value = self._item
            if self._is_tensor:
                self._item = torch.tensor(0.0)
            else:
                self._item = 0.0

        if self._is_tensor:
            item = value.item()
        else:
            item = value

        if compile:
            return item
        else:
            return [item]

    def merge(
        self: "LifetimeSumStats", incoming_stats: List["LifetimeSumStats"]
    ) -> None:
        assert (
            self._is_root_stats
        ), "LifetimeSumStats should only be merged at root level"
        self.push(sum([stat._item for stat in incoming_stats]))

    def __repr__(self) -> str:
        return f"LifetimeSumStats({self.peek()}; track_throughput_since_last_restore={self.track_throughput_since_last_restore})"
