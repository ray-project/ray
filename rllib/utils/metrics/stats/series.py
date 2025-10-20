from collections import deque
from typing import Any, Dict, List, Union, Optional, Tuple

from itertools import chain
from abc import ABCMeta

import numpy as np

from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.metrics.stats.base import StatsBase
from ray.rllib.utils.metrics.stats.utils import single_value_to_cpu


@DeveloperAPI
class SeriesStats(StatsBase, metaclass=ABCMeta):
    """A base class for Stats that represent a series of values."""

    # Set by subclasses
    _np_reduce_fn = None

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
            # Make sure we don't return any tensors here.
            "values": self.values,
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
        if self._reduce_at_root and not self._is_root_stats:
            if compile:
                raise ValueError(
                    "Can not compile at leaf level if reduce_at_root is True"
                )
            return_stats = self.clone(self)
            return_stats.values = self.values
            return return_stats

        if self._window is None:
            reduced_values = self.values
        else:
            reduced_values = self.window_reduce()

        self._set_values([])

        if compile:
            if len(reduced_values) == 0:
                return np.nan
            else:
                return reduced_values[0]

        return_stats = self.clone(self)
        return_stats.values = reduced_values
        return return_stats

    def __len__(self) -> int:
        """Returns the length of the internal values list."""
        return len(self.values)

    def _set_values(self, new_values):
        # For stats with window, use a deque with maxlen=window.
        # This way, we never store more values than absolutely necessary.
        if self._window and not self._is_root_stats:
            # Window always counts at leafs only
            self.values = deque(new_values, maxlen=self._window)
        # For infinite windows, use `new_values` as-is (a list).
        else:
            self.values = new_values

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
        """
        value = single_value_to_cpu(value)

        if self._window is None:
            if not self.values:
                self._set_values([value])
            else:
                self._set_values(self.running_reduce(self.values[0], value))
        else:
            # For windowed operations, append to values and trim if needed
            self.values.append(value)
            if self._window is not None and len(self.values) > self._window:
                self.values.popleft()

    def merge(self, incoming_stats: List["SeriesStats"], replace=True) -> None:
        """Merges SeriesStats objects.

        Args:
            incoming_stats: The list of SeriesStats objects to merge.
            replace: If True, replace internal items with the result of the merge.

        Returns:
            The merged SeriesStats object.
        """
        assert self._is_root_stats, "SeriesStats should only be merged at root level"

        # If there is only one stat to merge, and it is the same as self, return.
        if len(incoming_stats) == 0:
            # If none of the stats have values, return.
            return

        all_items = [s.values for s in incoming_stats]
        all_items = list(chain.from_iterable(all_items))
        if not replace:
            all_items = list(self.values) + all_items
        # Don't respect window explicitly to respect all incoming values.
        self.values = all_items

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Returns the result of reducing the internal values list.

        Note that this method does NOT alter the internal values list.

        Args:
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            The result of reducing the internal values list.
        """
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

    def running_reduce(self, value_1, value_2) -> Tuple[Any, Any]:
        """Reduces two values through a native python reduce function.

        Returns a single value that is on CPU memory.

        Args:
            value_1: The first value to reduce.
            value_2: The second value to reduce.

        Returns:
            The reduced value.
        """
        return [self._np_reduce_fn([value_1, value_2])]

    def window_reduce(self, values=None) -> Tuple[Any, Any]:
        """Reduces the internal values list according to the constructor settings.

        Returns a single value that is on CPU memory.

        Args:
            values: The list of values to reduce. If not None, use `self.values`

        Returns:
            The reduced value.
        """
        values = values if values is not None else self.values

        # Special case: Internal values list is empty -> return NaN or 0.0 for max.
        if len(values) == 0 or np.all(np.isnan(values)):
            return [np.nan]
        else:
            return [self._np_reduce_fn(values)]
