from collections import defaultdict, deque
from typing import Any, Dict, List, Union, Optional, Tuple
from abc import ABCMeta

import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.metrics.stats.base import StatsBase

torch, _ = try_import_torch()


@DeveloperAPI
class SeriesStats(StatsBase, metaclass=ABCMeta):
    """A base class for Stats that represent a series of values."""

    # Set by subclasses to override the default torch or numpy reduce function.
    _torch_reduce_fn = None
    _np_reduce_fn = None

    def __init__(
        self,
        window: Optional[Union[int, float]] = None,
        clear_on_reduce=False,
        **kwargs,
    ):
        """Initializes a SeriesStats instance.

        Args:
            window: The window size to reduce over.
        """
        super().__init__(**kwargs)

        self._clear_on_reduce = clear_on_reduce
        self._has_new_values = True
        self._window = window

        # Timing functionality (keep start times per thread).
        self._start_times = defaultdict(lambda: None)

        # The actual, underlying data in this Stats object.
        self.values: Union[List, deque.Deque] = None
        self._set_values([])

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state = {
            **state,
            # Make sure we don't return any tensors here.
            "values": convert_to_numpy(self.values),
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

        The internal values list is set to an empty list.

        Args:
            compile: If True, the result is compiled into a single value if possible.
                If it is not possible, the result is a list of values.
                If False, the result is a list of one or more values.

        Returns:
            The reduced value (can be of any type, depending on the input values and
            reduction method).
        """
        len_before_reduce = len(self)

        return_values, new_internal_values = self.reduced_values()

        if self._clear_on_reduce:
            self._set_values([])
        else:
            self._set_values(new_internal_values)

        if compile:
            return return_values[0]
        else:
            return_stats = self.similar_to(self)
            if len_before_reduce == 0:
                # return_values will be be 0 if we reduce a sum over zero elements
                # But we don't want to create such a zero out of nothing for our new
                # Stats object that we return here
                return return_stats
            else:
                return_stats._set_values(return_values)
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

        self._has_new_values = True

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
        """
        self.check_value(value)

        # For windowed operations, append to values and trim if needed
        self.values.append(value)
        if self._window is not None and len(self.values) > self._window:
            self.values.popleft()

        # Mark that we have new values
        self._has_new_values = True

    @staticmethod
    def merge(self, incoming_stats: List["SeriesStats"]) -> None:
        """Merges SeriesStats objects.

        If `root_stats` is None, we use the first incoming SeriesStats object as the new base SeriesStats object.
        If `root_stats` is not None, we merge all incoming SeriesStats objects into the base SeriesStats object.

        Args:
            root_stats: The base SeriesStats object to merge into.
            incoming_stats: The list of SeriesStats objects to merge.

        Returns:
            The merged SeriesStats object.
        """
        assert self._is_root_stats, "SeriesStats should only be merged at root level"

        # If any of the value lists have a length of 0 or if there is only one value and
        # it is nan, we skip
        stats_to_merge = [
            s
            for s in [incoming_stats]
            if not (
                len(s) == 0
                or (
                    len(s) == 1 and np.all(np.isnan(self._numpy_if_necessary(s.values)))
                )
            )
        ]

        # If there is only one stat to merge, and it is the same as self, return.
        if len(stats_to_merge) == 0:
            # If none of the stats have values, return.
            return

        # How to merge in parallel depends on the implementation of the Stats object implementation
        new_values = [s.values for s in stats_to_merge]
        reduced_values = self.reduced_values(new_values)
        self._set_values(reduced_values)

        self._has_new_values = True

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Returns the result of reducing the internal values list.

        Note that this method does NOT alter the internal values list.

        Args:
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            The result of reducing the internal values list.
        """
        reduced_values, _ = self.reduced_values()
        return reduced_values[0] if compile else reduced_values

    def reduced_values(self, values=None) -> Tuple[Any, Any]:
        """A non-committed reduction procedure on given values (or `self.values`).

        Note that this method does NOT alter any state of `self` or the possibly
        provided list of `values`. It only returns new values as they should be
        adopted after a possible, actual reduction step.

        Args:
            values: The list of values to reduce. If not None, use `self.values`

        Returns:
            A tuple containing 1) the reduced values and 2) the new internal values list
            to be used. If there is no reduciton method, the reduced values will be the same as the values.
        """
        values = values if values is not None else self.values

        # Special case: Internal values list is empty -> return NaN or 0.0 for max.
        if len(values) == 0:
            return [np.nan], []

        if torch and torch.is_tensor(values[0]):
            self._is_tensor = True
            if len(values[0].shape) == 0:
                reduced = values[0]
            else:
                reduce_in = torch.stack(list(values))
                reduced = self._torch_reduce_fn(reduce_in)
        else:
            if np.all(np.isnan(values)):
                # This avoids warnings for taking a mean of an empty array.
                reduced = np.nan
            else:
                reduced = self._np_reduce_fn(values)

        def safe_isnan(value):
            if torch and isinstance(value, torch.Tensor):
                return torch.isnan(value)
            return np.isnan(value)

        # Convert from numpy to primitive python types, if original `values` are
        # python types.
        if (
            not safe_isnan(reduced)
            and reduced.shape == ()
            and isinstance(values[0], (int, float))
        ):
            if reduced.dtype in [np.int32, np.int64, np.int8, np.int16]:
                reduced = int(reduced)
            else:
                reduced = float(reduced)

        return [reduced], [reduced]
