import time
from typing import Any, Dict, List, Union

from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics.stats.base import StatsBase
from ray.rllib.utils.metrics.stats.utils import safe_isnan, single_value_to_cpu
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()
_, tf, _ = try_import_tf()


@DeveloperAPI
class LifetimeSumStats(StatsBase):
    """A Stats object that tracks the sum of a series of singular values (not vectors)."""

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
            not self.is_leaf
        ), "initialize_throughput_reference_time can only be called on root stats"
        self._last_reduce_time = time
        self._last_restore_time = time

    @staticmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        super_args = StatsBase._get_init_args(stats_object=stats_object, state=state)
        if state is not None:
            return {
                **super_args,
                "with_throughput": state["track_throughputs"],
            }
        elif stats_object is not None:
            return {
                **super_args,
                "with_throughput": stats_object.track_throughputs,
            }
        else:
            raise ValueError("Either stats_object or state must be provided")

    @property
    def throughputs(self) -> Dict[str, float]:
        """Returns the throughput since the last reduce.

        For root stats, also returns throughput since last restore.
        """
        assert (
            self.has_throughputs
        ), "Throughput tracking is not enabled on this Stats object"
        result = {
            "throughput_since_last_reduce": self.throughput_since_last_reduce,
        }
        # Only root stats track throughput since last restore
        if self.is_root:
            result["throughput_since_last_restore"] = self.throughput_since_last_restore
        return result

    def __len__(self) -> int:
        return 1

    def peek(
        self, compile: bool = True, latest_merged_only: bool = False
    ) -> Union[Any, List[Any]]:
        """Returns the current lifetime sum value.

        If value is a GPU tensor, it's converted to CPU.

        Args:
            compile: If True, the result is compiled into a single value if possible.
            latest_merged_only: If True, only considers the latest merged values.
                This parameter only works on aggregation stats (root or intermediate nodes).
                When enabled, peek() will only return the sum that was added in the most recent merge operation.
        """
        # Check latest_merged_only validity
        if latest_merged_only and self.is_leaf:
            raise ValueError(
                "latest_merged_only can only be used on aggregation stats objects (is_leaf=False)."
            )

        # If latest_merged_only is True, use only the latest merged sum
        if latest_merged_only:
            if self.latest_merged is None:
                # No merged values yet, return 0
                value = 0.0
            else:
                # Use only the latest merged sum
                value = self.latest_merged
        else:
            # Normal peek behavior
            value = self._lifetime_sum

        # Convert GPU tensor to CPU
        if torch and isinstance(value, torch.Tensor):
            value = single_value_to_cpu(value)
        return value if compile else [value]

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["lifetime_sum"] = single_value_to_cpu(self._lifetime_sum)
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

        Args:
            value: The value to be pushed. Can be of any type.
                PyTorch GPU tensors are kept on GPU until reduce() or peek().
                TensorFlow tensors are moved to CPU immediately.
        """
        # Convert TensorFlow tensors to CPU immediately
        if tf and tf.is_tensor(value):
            value = value.numpy()
        if safe_isnan(value):
            return

        if torch and isinstance(value, torch.Tensor):
            value = value.detach()

        self._lifetime_sum += value

    @property
    def throughput_since_last_reduce(self) -> float:
        """Returns the throughput since the last reduce call."""
        if self.track_throughputs:
            lifetime_sum = self._lifetime_sum
            # Convert GPU tensor to CPU
            if torch and isinstance(lifetime_sum, torch.Tensor):
                lifetime_sum = single_value_to_cpu(lifetime_sum)

            return (lifetime_sum - self._value_at_last_reduce) / (
                time.perf_counter() - self._last_reduce_time
            )
        else:
            raise ValueError(
                "Tracking of throughput since last reduce is not enabled on this Stats object"
            )

    @property
    def throughput_since_last_restore(self) -> float:
        """Returns the total throughput since the last restore.

        Only available for root stats, as restoring from checkpoints only happens at the root.
        """
        if not self.is_root:
            raise ValueError(
                "throughput_since_last_restore is only available for root stats"
            )
        if self.track_throughputs:
            lifetime_sum = self._lifetime_sum
            # Convert GPU tensor to CPU
            if torch and isinstance(lifetime_sum, torch.Tensor):
                lifetime_sum = single_value_to_cpu(lifetime_sum)

            return (lifetime_sum - self._value_at_last_restore) / (
                time.perf_counter() - self._last_restore_time
            )
        else:
            raise ValueError(
                "Tracking of throughput since last restore is not enabled on this Stats object"
            )

    def reduce(self, compile: bool = True) -> Union[Any, "LifetimeSumStats"]:
        """Reduces the internal value.

        If value is a GPU tensor, it's converted to CPU.
        """
        value = self._lifetime_sum
        # Convert GPU tensor to CPU
        if torch and isinstance(value, torch.Tensor):
            value = single_value_to_cpu(value)

        # Reset for all non-root stats (both leaf and intermediate aggregators)
        # Only root stats should never reset because they aggregate everything
        # Non-root stats reset so they only send deltas up the aggregation tree
        if not self.is_root:
            # Reset to 0 with same type (tensor or scalar)
            if torch and isinstance(self._lifetime_sum, torch.Tensor):
                self._lifetime_sum = torch.tensor(0.0, device=self._lifetime_sum.device)
            else:
                self._lifetime_sum = 0.0
            self._value_at_last_reduce = 0.0
        else:
            self._value_at_last_reduce = value

        # Update the last reduce time for throughput tracking
        if self.track_throughputs:
            self._last_reduce_time = time.perf_counter()

        if compile:
            return value

        return_stats = self.clone()
        return_stats._lifetime_sum = value
        return return_stats

    def merge(self, incoming_stats: List["LifetimeSumStats"]) -> None:
        """Merges LifetimeSumStats objects.

        Args:
            incoming_stats: The list of LifetimeSumStats objects to merge.

        Returns:
            None. The merge operation modifies self in place.
        """
        assert (
            not self.is_leaf
        ), "LifetimeSumStats should only be merged at aggregation stages (root or intermediate)"
        incoming_sum = sum([stat._lifetime_sum for stat in incoming_stats])

        # Directly update _lifetime_sum instead of calling push (which is disabled for non-leaf stats)
        if torch and isinstance(incoming_sum, torch.Tensor):
            incoming_sum = incoming_sum.detach()
        if tf and tf.is_tensor(incoming_sum):
            incoming_sum = incoming_sum.numpy()

        self._lifetime_sum += incoming_sum

        # Track merged values for latest_merged_only peek functionality
        if not self.is_leaf:
            # Store the sum that was added in this merge operation
            self.latest_merged = incoming_sum

    def __repr__(self) -> str:
        return f"LifetimeSumStats({self.peek()}; track_throughputs={self.track_throughputs})"
