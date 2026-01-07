import logging
from typing import Any, Dict, List, Union

import numpy as np

from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics.stats.base import StatsBase
from ray.rllib.utils.metrics.stats.utils import safe_isnan, single_value_to_cpu
from ray.util import log_once
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

torch, _ = try_import_torch()
_, tf, _ = try_import_tf()


@DeveloperAPI
class EmaStats(StatsBase):
    """A Stats object that tracks the exponential average of a series of singular values (not vectors)."""

    stats_cls_identifier = "ema"

    def __init__(
        self,
        ema_coeff: float = 0.01,
        *args,
        **kwargs,
    ):
        """Initializes a EmaStats instance.

        We calculate the EMA in parallel components.
        Also, we potentially aggregate them multiple times per reduction cycle.
        We therefore aggregate by taking the mean of all collected EMAs.
        We do this for simplicity and accept this limitation because EMAs
        inherently only approximate.

        Example to illustrate this limitation:
        Using an ema coefficient of 0.01:
        First incoming ema: [1, 2, 3, 4, 5] -> 1.1
        Second incoming ema: [15] -> 15
        Mean of both merged ema values: [1.1, 15] -> 8.05
        True mean of all values: [1, 2, 3, 4, 5, 15] -> 5

        Args:
            ema_coeff: The EMA coefficient to use. Defaults to 0.01.
        """
        super().__init__(*args, **kwargs)
        self._value = np.nan
        if not self.is_leaf:
            self._values_to_merge = []
        self._ema_coeff = ema_coeff

    def __len__(self) -> int:
        """Returns the length of the internal values list."""
        return 1

    def merge(self, incoming_stats: List["EmaStats"]) -> None:
        """Merges EmaStats objects.

        Args:
            incoming_stats: The list of EmaStats objects to merge.

        Returns:
            None. The merge operation modifies self in place.
        """
        assert (
            not self.is_leaf
        ), "EmaStats should only be merged at aggregation stages (root or intermediate)"

        all_values = [stat._value for stat in incoming_stats]
        if len(all_values) == 0:
            return

        self._values_to_merge.extend(all_values)

        # Track merged values for latest_merged_only peek functionality
        if not self.is_leaf:
            # Store the values that were merged in this operation
            self.latest_merged = all_values

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

        # If incoming value is NaN, do nothing
        if safe_isnan(value):
            return

        if torch and isinstance(value, torch.Tensor):
            # Detach the value from the graph to avoid unnecessary computation
            value = value.detach()

        # If internal value is NaN, replace it with the incoming value
        if safe_isnan(self._value):
            self._value = value
        else:
            # Otherwise, update the internal value using the EMA formula
            self._value = (
                self._ema_coeff * value + (1.0 - self._ema_coeff) * self._value
            )

    def _reduce_values_to_merge(self) -> float:
        """Reduces the internal values to merge."""
        if not np.isnan(self._value) and log_once("ema_stats_merge_push"):
            logger.warning(
                f"Merging values in {self} but self._value is not NaN. This leads to an inaccurate metric. Not erroring out to avoid breaking older checkpoints."
            )

        if len(self._values_to_merge) == 0:
            return np.nan

        if torch and isinstance(self._values_to_merge[0], torch.Tensor):
            stacked = torch.stack(list(self._values_to_merge))
            return torch.nanmean(stacked)
        return np.nanmean(self._values_to_merge)

    def peek(
        self, compile: bool = True, latest_merged_only: bool = False
    ) -> Union[Any, List[Any]]:
        """Returns the current EMA value.

        If value is a GPU tensor, it's converted to CPU.

        Args:
            compile: If True, the result is compiled into a single value if possible.
            latest_merged_only: If True, only considers the latest merged values.
                This parameter only works on aggregation stats (root or intermediate nodes).
                When enabled, peek() will only use the values from the most recent merge operation.
        """
        # Check latest_merged_only validity
        if latest_merged_only and self.is_leaf:
            raise ValueError(
                "latest_merged_only can only be used on aggregation stats objects (is_leaf=False)."
            )

        # If latest_merged_only is True, use only the latest merged values
        if latest_merged_only:
            if self.latest_merged is None:
                # No merged values yet, return NaN
                if compile:
                    return np.nan
                else:
                    return [np.nan]
            # Use only the latest merged values
            latest_merged = self.latest_merged
            if len(latest_merged) == 0:
                value = np.nan
            else:
                # Reduce latest merged values
                if torch and isinstance(latest_merged[0], torch.Tensor):
                    stacked = torch.stack(list(latest_merged))
                    value = torch.nanmean(stacked)
                else:
                    value = np.nanmean(latest_merged)
        else:
            # Normal peek behavior
            if hasattr(self, "_values_to_merge"):
                # If _values_to_merge is empty, use _value instead
                # This can happen after reduce(compile=False) returns a new stats object
                if len(self._values_to_merge) == 0:
                    value = self._value
                else:
                    value = self._reduce_values_to_merge()
            else:
                value = self._value

        value = single_value_to_cpu(value)

        return value if compile else [value]

    def reduce(self, compile: bool = True) -> Union[Any, "EmaStats"]:
        """Reduces the internal value.

        If value is a GPU tensor, it's converted to CPU.

        Args:
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            The reduced value.

        """
        if hasattr(self, "_values_to_merge"):
            # If _values_to_merge is empty, use _value instead
            # This can happen when a non-leaf stats object logs values directly
            if len(self._values_to_merge) == 0:
                value = self._value
            else:
                value = self._reduce_values_to_merge()
            self._values_to_merge = []
        else:
            value = self._value

        # Convert GPU tensor to CPU
        if torch and isinstance(value, torch.Tensor):
            value = single_value_to_cpu(value)

        self._value = np.nan

        if compile:
            return value

        return_stats = self.clone()
        return_stats._value = value
        return return_stats

    def __repr__(self) -> str:
        values_to_merge_len = (
            len(self._values_to_merge) if hasattr(self, "_values_to_merge") else 0
        )
        return (
            f"EmaStats({self.peek()}; number_of_values_to_merge=({values_to_merge_len}); "
            f"ema_coeff={self._ema_coeff}, value={self._value})"
        )

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["ema_coeff"] = self._ema_coeff
        state["value"] = self._value
        if not self.is_leaf:
            state["values_to_merge"] = self._values_to_merge
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self._ema_coeff = state["ema_coeff"]
        self._value = state["value"]
        # Handle legacy state that doesn't have values_to_merge
        if not self.is_leaf:
            self._values_to_merge = state.get("values_to_merge", [])

    @staticmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        super_args = StatsBase._get_init_args(stats_object=stats_object, state=state)
        if state is not None:
            return {
                **super_args,
                "ema_coeff": state["ema_coeff"],
            }
        if stats_object is not None:
            return {
                **super_args,
                "ema_coeff": stats_object._ema_coeff,
            }
        else:
            raise ValueError("Either stats_object or state must be provided")
