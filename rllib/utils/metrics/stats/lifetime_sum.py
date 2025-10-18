from typing import Any, Dict, List, Union
import time

from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.base import StatsBase
from ray.rllib.utils.metrics.stats.utils import single_value_to_cpu


@DeveloperAPI
class LifetimeSumStats(StatsBase):
    """A Stats object that tracks the sum of a series of values."""

    stats_cls_identifier = "lifetime_sum"

    def __init__(
        self,
        with_throughput: bool = False,
        *args,
        **kwargs,
    ):
        """Initializes a LifetimeSumStats instance.

        Args:
            with_throughput: If True, track the throughput since the last restore from a checkpoint.
        """
        # Make sure we don't accidentally set clear_on_reduce to False
        if "clear_on_reduce" in kwargs and kwargs["clear_on_reduce"]:
            raise ValueError("LifetimeSumStats does not support clear_on_reduce")

        super().__init__(*args, **kwargs)

        self._lifetime_sum = 0.0

        self.track_throughputs = with_throughput
        # We need to initialize this to 0.0
        # When setting state or reducing, these values are expected to be updated we calculate a throughput.
        self._value_at_last_reduce = 0.0
        self._value_at_last_restore = 0.0
        # We initialize this to the current time which may result in a low first throughput value
        # It seems reasonable that starting from a checkpoint or starting an experiment results in a low first throughput value
        self._last_reduce_time = time.perf_counter()
        self._last_restore_time = time.perf_counter()

    @property
    def has_throughputs(self) -> bool:
        return self.track_throughputs

    def initialize_throughput_reference_time(self, time: float) -> None:
        assert (
            self._is_root_stats
        ), "initialize_throughput_reference_time can only be called on root stats"
        self._last_reduce_time = time
        self._last_restore_time = time

    @staticmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        super_args = StatsBase._get_init_args(stats_object=stats_object, state=state)
        del super_args["clear_on_reduce"]
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

    @property
    def throughputs(self) -> Dict[str, float]:
        """Returns the throughput since the last reduce."""
        assert (
            self.has_throughputs
        ), "Throughput tracking is not enabled on this Stats object"
        return {
            "throughput_since_last_reduce": self.throughput_since_last_reduce,
            "throughput_since_last_restore": self.throughput_since_last_restore,
        }

    def __len__(self) -> int:
        return 1

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        lifetime_sum = single_value_to_cpu(self._lifetime_sum)
        return lifetime_sum if compile else [lifetime_sum]

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["lifetime_sum"] = self._lifetime_sum
        state["track_throughputs"] = self.track_throughputs
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self._lifetime_sum = state["lifetime_sum"]
        self.track_throughputs = state["track_throughputs"]

        # We always start over with the throughput calculation after a restore
        self._value_at_last_restore = self._lifetime_sum
        self._value_at_last_reduce = self._lifetime_sum

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        This puts the value onto CPU memory.

        Args:
            value: The value to be pushed. Can be of any type.
        """
        self._lifetime_sum += single_value_to_cpu(value)

    @property
    def throughput_since_last_reduce(self) -> float:
        """Returns the throughput since the last reduce call."""
        if self.track_throughputs:
            return (
                single_value_to_cpu(self._lifetime_sum) - self._value_at_last_reduce
            ) / (time.perf_counter() - self._last_reduce_time)
        else:
            raise ValueError(
                "Tracking of throughput since last reduce is not enabled on this Stats object"
            )

    @property
    def throughput_since_last_restore(self) -> float:
        """Returns the total throughput since the last restore."""
        if self.track_throughputs:
            return (
                single_value_to_cpu(self._lifetime_sum) - self._value_at_last_restore
            ) / (time.perf_counter() - self._last_restore_time)
        else:
            raise ValueError(
                "Tracking of throughput since last restore is not enabled on this Stats object"
            )

    def reduce(self, compile: bool = True) -> Union[Any, "LifetimeSumStats"]:
        value = self._lifetime_sum

        self._value_at_last_reduce = value

        self._lifetime_sum = 0.0
        value = single_value_to_cpu(value)

        if compile:
            return value

        return_stats = self.similar_to(self)
        return_stats._lifetime_sum = value
        return return_stats

    def merge(
        self: "LifetimeSumStats", incoming_stats: List["LifetimeSumStats"]
    ) -> None:
        assert (
            self._is_root_stats
        ), "LifetimeSumStats should only be merged at root level"
        self.push(sum([stat._item for stat in incoming_stats]))

    def __repr__(self) -> str:
        return f"LifetimeSumStats({self.peek()}; track_throughputs={self.track_throughputs})"
