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

    def __init__(
        self,
        track_throughput_since_last_restore: bool = False,
        track_througput_since_last_reduce: bool = False,
        *args,
        **kwargs,
    ):
        """Initializes a LifetimeSumStats instance.

        Args:
            track_throughput_since_last_restore: If True, track the throughput since the last restore from a checkpoint.
        """
        super().__init__(*args, **kwargs)
        if self._clear_on_reduce:
            raise ValueError("LifetimeSumStats does not support clear_on_reduce")

        self._lifetime_sum = 0.0

        self.track_throughput_last_restore = track_throughput_since_last_restore
        self.track_throughput_since_last_reduce = track_througput_since_last_reduce
        # We need to initialize this to None becasue if we restart from a checkpoint, we should start over with througput calculation
        self._value_at_last_reduce = None
        self._value_at_last_restore = None
        # We initialize this to the current time which may result in a low first throughput value
        # It seems reasonable that starting from a checkpoint or starting an experiment results in a low first throughput value
        self._last_reduce_time = time.perf_counter()
        self._last_restore_time = time.perf_counter()

    def has_throughputs(self) -> bool:
        return (
            self.track_throughput_last_restore
            or self.track_throughput_since_last_reduce
        )

    @property
    def throughputs(self) -> Dict[str, float]:
        """Returns the throughput since the last reduce."""
        assert (
            self.has_throughput()
        ), "Throughput tracking is not enabled on this Stats object"
        throughputs = {}
        if self.track_throughput_since_last_reduce:
            throughputs[
                "throughput_since_last_reduce"
            ] = self.throughput_since_last_reduce
        if self.track_throughput_last_restore:
            throughputs[
                "throughput_since_last_restore"
            ] = self.throughput_since_last_restore
        return throughputs

    def __len__(self) -> int:
        return 1

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        return self._lifetime_sum if compile else [self._lifetime_sum]

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
        self.value_at_last_restore = self._lifetime_sum

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
        """
        if not self._is_tensor and torch and torch.is_tensor(value):
            self._is_tensor = True
            # turn item into a tensor
            self._lifetime_sum = torch.tensor(self._lifetime_sum)

        self._lifetime_sum += value

    @property
    def throughput_since_last_reduce(self) -> float:
        """Returns the throughput since the last reduce."""
        if self.track_throughput_last_restore:
            return self._lifetime_sum / (time.perf_counter() - self._last_reduce_time)
        else:
            raise ValueError(
                "Tracking of throughput since last reduce is not enabled on this Stats object"
            )

    @property
    def throughput_since_last_restore(self) -> float:
        """Returns the throughput total."""
        if self.track_throughput_last_restore:
            return self._lifetime_sum / (time.perf_counter() - self._last_restore_time)
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
        value = self._lifetime_sum

        if not self._is_root_stats:
            # On leaves, we need to return the current value and reset it to 0
            # Otherwise, we would be adding accumulated values at the root multiple times
            if self._is_tensor:
                self._lifetime_sum = torch.tensor(0.0)
            else:
                self._lifetime_sum = 0.0

        if self._is_tensor:
            value = value.item()

        if compile:
            return value
        else:
            return [value]

    def merge(
        self: "LifetimeSumStats", incoming_stats: List["LifetimeSumStats"]
    ) -> None:
        assert (
            self._is_root_stats
        ), "LifetimeSumStats should only be merged at root level"
        self.push(sum([stat._item for stat in incoming_stats]))

    def __repr__(self) -> str:
        return f"LifetimeSumStats({self.peek()}; track_throughput_since_last_restore={self.track_throughput_since_last_restore})"
