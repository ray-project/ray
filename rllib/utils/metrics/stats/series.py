from collections import deque
from typing import Any, Dict, List, Union, Optional, Tuple

from itertools import chain
from abc import ABCMeta

import numpy as np

from ray.rllib.utils.framework import try_import_torch, try_import_tf
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.metrics.stats.base import StatsBase


torch, _ = try_import_torch()
_, tf, _ = try_import_tf()


@DeveloperAPI
class SeriesStats(StatsBase, metaclass=ABCMeta):
    """A base class for Stats that represent a series of values."""

    # Set by subclasses
    _torch_reduce_fn = None
    _np_reduce_fn = None
    _python_reduce_fn = None

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
        if window:
            self.values = deque(maxlen=window)
            self._set_values([])
        else:
            # If we dno't have a window, we want to always keep only one value.
            self.values = [np.nan]

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state = {
            **state,
            # Make sure we don't return any tensors here.
            "values": self.values,
            "window": self._window,
            "clear_on_reduce": self._clear_on_reduce,
        }
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self.values = state["values"]
        self._window = state["window"]
        self._clear_on_reduce = state["clear_on_reduce"]

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @staticmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        super_args = StatsBase._get_init_args(stats_object=stats_object, state=state)
        if state is not None:
            return {
                **super_args,
                "window": state["window"],
                "clear_on_reduce": state["clear_on_reduce"],
            }
        elif stats_object is not None:
            return {
                **super_args,
                "window": stats_object._window,
                "clear_on_reduce": stats_object._clear_on_reduce,
            }
        else:
            raise ValueError("Either stats_object or state must be provided")

    def reduce(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Reduces the internal values list according to the constructor settings.

        Returned values are always on CPU memory.

        Args:
            compile: If True, the result is compiled into a single value if possible.
                If it is not possible, the result is a list of values.
                If False, the result is a list of one or more values.

        Returns:
            The reduced value (can be of any type, depending on the input values and
            reduction method).
        """
        len_before_reduce = len(self)
        if self._window is None:
            # Assume we are dealing with single value
            if torch and torch.is_tensor(self.values[0]):
                reduced_values = [self.values[0].detach()]
            elif tf and tf.is_tensor(self.values[0]):
                reduced_values = [self.values[0].numpy()]
            else:
                # self.values should just be a list with a single python value
                reduced_values = self.values
        else:
            reduced_values = self.window_reduce()

        if self._clear_on_reduce:
            self._set_values([])
        else:
            self._set_values(reduced_values)

        if compile:
            if torch and torch.is_tensor(reduced_values[0]):
                return reduced_values[0].cpu()
            return reduced_values[0]
        else:
            return_stats = self.similar_to(self)
            if len_before_reduce == 0:
                # return_values will be be 0 if we reduce a sum over zero elements
                # But we don't want to create such a zero out of nothing for our new
                # Stats object that we return here
                return return_stats
            else:
                return_stats._set_values(reduced_values)
            return

    @staticmethod
    def _numpy_if_necessary(values):
        # Torch tensor handling. Convert to CPU/numpy first.
        if torch and len(values) > 0 and torch.is_tensor(values[0]):
            # Convert all tensors to numpy values.
            values = [v.cpu().numpy() for v in values]
        return values

    def __len__(self) -> int:
        """Returns the length of the internal values list."""
        return len(self.values)

    def _set_values(self, new_values):
        # For stats with window, use a deque with maxlen=window.
        # This way, we never store more values than absolutely necessary.
        if self._window:
            self.values = deque(new_values, maxlen=self._window)
        # For infinite windows, use `new_values` as-is (a list).
        else:
            self.values = new_values

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
        """
        if torch and torch.is_tensor(value):
            value = value.detach()
        elif tf and tf.is_tensor(value):
            value = tf.stop_gradient(value).numpy()

        if self._window is None:
            if self.values[0] is np.nan:
                self.values = [value]
            else:
                self.values = self.running_reduce(self.values[0], value)
        else:
            # For windowed operations, append to values and trim if needed
            self.values.append(value)
            if self._window is not None and len(self.values) > self._window:
                self.values.popleft()

    @staticmethod
    def merge(self, incoming_stats: List["SeriesStats"]) -> None:
        """Merges SeriesStats objects.

        Args:
            incoming_stats: The list of SeriesStats objects to merge.

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
        reduced_values = self.window_reduce(all_items)
        self._set_values(reduced_values)

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Returns the result of reducing the internal values list.

        Note that this method does NOT alter the internal values list.

        Args:
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            The result of reducing the internal values list.
        """
        if self._window is None:
            # Assume that the values are already reduced.
            if torch and isinstance(self.values, torch.Tensor):
                # Value should already be detached
                reduced_values = [self.values[0].cpu()]
            else:
                # self.values should just be a list with a single python value
                reduced_values = self.values
        else:
            reduced_values = self.window_reduce()

        return reduced_values[0] if compile else reduced_values

    def running_reduce(self, value_1, value_2) -> Tuple[Any, Any]:
        """Reduces two values through a native python reduce function.

        Returns a single value that is on CPU memory.

        Args:
            value_1: The first value to reduce.
            value_2: The second value to reduce.

        Returns:
            The reduced value.
        """
        return [self.python_reduce_fn(value_1, value_2)]

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
        if len(values) == 0:
            return [np.nan], []

        if torch and torch.is_tensor(values[0]):
            self._is_tensor = True
            if len(values) == 1:
                # No need to reduce if there is only one value.
                reduced = values[0].detach().cpu()
            else:
                reduce_in = torch.stack(list(values))
                reduced = self._torch_reduce_fn(reduce_in).detach().cpu().item()
        else:
            if len(values) == 1:
                # No need to reduce if there is only one value.
                reduced = values[0]
            else:
                if np.all(np.isnan(values)):
                    # This avoids warnings for taking a mean of an empty array.
                    reduced = np.nan
                else:
                    reduced = self._np_reduce_fn(values).item()

        return [reduced]
