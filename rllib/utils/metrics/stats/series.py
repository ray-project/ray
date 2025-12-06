from abc import ABCMeta
from collections import deque
from itertools import chain
from typing import Any, Dict, List, Optional, Union

import numpy as np

from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics.stats.base import StatsBase
from ray.rllib.utils.metrics.stats.utils import batch_values_to_cpu, single_value_to_cpu
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()
_, tf, _ = try_import_tf()


@DeveloperAPI
class SeriesStats(StatsBase, metaclass=ABCMeta):
    """A base class for Stats that represent a series of singular values (not vectors)."""

    # Set by subclasses
    _np_reduce_fn = None
    # Set by subclasses
    _torch_reduce_fn = None

    def __init__(
        self,
        window: Optional[Union[int, float]] = None,
        *args,
        **kwargs,
    ):
        """Initializes a SeriesStats instance.

        Args:
            window: The window size to reduce over.
        """
        super().__init__(*args, **kwargs)

        self._window = window

        self.values: Union[List[Any], deque[Any]] = []
        self._set_values([])

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state = {
            **state,
            "values": batch_values_to_cpu(self.values),
            "window": self._window,
        }
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self._set_values(state["values"])
        self._window = state["window"]

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @staticmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        super_args = StatsBase._get_init_args(stats_object=stats_object, state=state)
        if state is not None:
            return {
                **super_args,
                "window": state["window"],
            }
        elif stats_object is not None:
            return {
                **super_args,
                "window": stats_object._window,
            }
        else:
            raise ValueError("Either stats_object or state must be provided")

    def reduce(self, compile: bool = True) -> Union[Any, "SeriesStats"]:
        """Reduces the internal values list according to the constructor settings."""
        if self._window is None:
            if len(self.values) <= 1 or not compile:
                reduced_values = batch_values_to_cpu(self.values)
            else:
                reduced_values = self.window_reduce()
        else:
            reduced_values = self.window_reduce()

        self._set_values([])

        if compile:
            if len(reduced_values) == 0:
                return np.nan
            else:
                return reduced_values[0]

        return_stats = self.clone()
        return_stats.values = reduced_values
        return return_stats

    def __len__(self) -> int:
        """Returns the length of the internal values list."""
        return len(self.values)

    def _set_values(self, new_values):
        # For stats with window, use a deque with maxlen=window.
        # This way, we never store more values than absolutely necessary.
        if self._window and self.is_leaf:
            # Window always counts at leafs only (or non-root stats)
            self.values = deque(new_values, maxlen=self._window)
        # For infinite windows, use `new_values` as-is (a list).
        else:
            self.values = new_values

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
                PyTorch GPU tensors are kept on GPU until reduce() or peek().
                TensorFlow tensors are moved to CPU immediately.
        """
        # Convert TensorFlow tensors to CPU immediately, keep PyTorch tensors as-is
        if tf and tf.is_tensor(value):
            value = value.numpy()

        if torch and isinstance(value, torch.Tensor):
            value = value.detach()

        if self._window is None:
            if not self.values:
                self._set_values([value])
            else:
                self._set_values(self.running_reduce(self.values[0], value))
        else:
            # For windowed operations, append to values and trim if needed
            self.values.append(value)

    def merge(self, incoming_stats: List["SeriesStats"]) -> None:
        """Merges SeriesStats objects.

        Args:
            incoming_stats: The list of SeriesStats objects to merge.

        Returns:
            None. The merge operation modifies self in place.
        """
        assert (
            not self.is_leaf
        ), "SeriesStats should only be merged at aggregation stages (root or intermediate)"

        if len(incoming_stats) == 0:
            return

        all_items = [s.values for s in incoming_stats]
        all_items = list(chain.from_iterable(all_items))
        # Implicitly may convert internal to list.
        # That's ok because we don't want to evict items from the deque if we merge in this object's values.
        all_items = list(self.values) + list(all_items)
        self.values = all_items

        # Track merged values for latest_merged_only peek functionality
        if not self.is_leaf:
            # Store the values that were merged in this operation (from incoming_stats only)
            merged_values = list(
                chain.from_iterable([s.values for s in incoming_stats])
            )
            self.latest_merged = merged_values

    def peek(
        self, compile: bool = True, latest_merged_only: bool = False
    ) -> Union[Any, List[Any]]:
        """Returns the result of reducing the internal values list.

        Note that this method does NOT alter the internal values list.

        Args:
            compile: If True, the result is compiled into a single value if possible.
            latest_merged_only: If True, only considers the latest merged values.
                This parameter only works on aggregation stats (root or intermediate nodes, is_leaf=False).
                When enabled, peek() will only use the values from the most recent merge operation.

        Returns:
            The result of reducing the internal values list.
        """
        # If latest_merged_only is True, use look at the latest merged values
        if latest_merged_only:
            if self.is_leaf:
                raise ValueError(
                    "latest_merged_only can only be used on aggregation stats objects "
                    "(is_leaf=False)"
                )
            if self.latest_merged is None:
                # No merged values yet, return NaN or empty list
                if compile:
                    return np.nan
                else:
                    return []
            # Use only the latest merged values
            latest_merged = self.latest_merged
            if len(latest_merged) == 0:
                reduced_values = [np.nan]
            else:
                reduced_values = self.window_reduce(latest_merged)
        else:
            # Normal peek behavior
            if len(self.values) == 1:
                # Note that we can not check for window=None here because merged SeriesStats may have multiple values.
                reduced_values = self.values
            else:
                reduced_values = self.window_reduce()

        if compile:
            if len(reduced_values) == 0:
                return np.nan
            else:
                return reduced_values[0]
        else:
            return reduced_values

    def running_reduce(self, value_1, value_2) -> List[Any]:
        """Reduces two values through a reduce function.

        If values are PyTorch tensors, reduction happens on GPU.
        Result stays on GPU (or CPU if values were CPU).

        Args:
            value_1: The first value to reduce.
            value_2: The second value to reduce.

        Returns:
            The reduced value (may be GPU tensor).
        """
        # If values are torch tensors, reduce on GPU
        if (
            torch
            and isinstance(value_1, torch.Tensor)
            and hasattr(self, "_torch_reduce_fn")
        ):
            return [self._torch_reduce_fn(torch.stack([value_1, value_2]))]
        # Otherwise use numpy reduction
        return [self._np_reduce_fn([value_1, value_2])]

    def window_reduce(self, values=None) -> List[Any]:
        """Reduces the internal values list according to the constructor settings.

        If values are PyTorch GPU tensors, reduction happens on GPU and result
        is moved to CPU. Otherwise returns CPU value.

        Args:
            values: The list of values to reduce. If not None, use `self.values`

        Returns:
            The reduced value on CPU.
        """
        values = values if values is not None else self.values

        # Special case: Internal values list is empty -> return NaN
        if len(values) == 0:
            return [np.nan]

        # If values are torch tensors, reduce on GPU then move to CPU
        if (
            torch
            and isinstance(values[0], torch.Tensor)
            and hasattr(self, "_torch_reduce_fn")
        ):
            stacked = torch.stack(list(values))
            # Check for all NaN
            if torch.all(torch.isnan(stacked)):
                return [np.nan]
            result = self._torch_reduce_fn(stacked)
            return [single_value_to_cpu(result)]

        # Otherwise use numpy reduction on CPU values
        if np.all(np.isnan(values)):
            return [np.nan]
        else:
            return [self._np_reduce_fn(values)]
