from typing import Any, Dict, List, Union

import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import SeriesStats
from ray.rllib.utils.metrics.stats.base import StatsBase
from ray.rllib.utils.metrics.stats.utils import single_value_to_cpu

torch, _ = try_import_torch()


@DeveloperAPI
class EmaStats(StatsBase):
    """A Stats object that tracks the mean of a series of values."""

    stats_cls_identifier = "ema"

    def __init__(
        self,
        ema_coeff: float = 0.01,
        *args,
        **kwargs,
    ):
        """Initializes a MeanStats instance.

        Note that, when aggregating EmaStats objects with `EmaStats.merge()`, we take the
        mean of the incoming means. The output of the aggregated stats is therefore not
        the exponetially moving average of the values that are logged by parallel components
        in order, but rather the mean of the EMA logged by parallel components.

        Args:
            ema_coeff: The EMA coefficient to use. Defaults to 0.01.
            clear_on_reduce: If True, the Stats object will reset its entire values list
                to an empty one after `EmaStats.reduce()` is called.
        """
        super().__init__(*args, **kwargs)
        self._item = np.nan
        self._ema_coeff = ema_coeff

    def __len__(self) -> int:
        """Returns the length of the internal values list."""
        return 1

    def merge(self, incoming_stats: List["EmaStats"]):
        # Take the average of the incoming means
        self._item = np.nanmean([stat._item for stat in incoming_stats])

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object."""
        if self._item is not np.nan:
            self._item = self._ema_coeff * value + (1.0 - self._ema_coeff) * self._item
        else:
            self._item = value

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Returns the result of reducing the internal values list.

        Note that this method does NOT alter the internal values list in this process.
        Thus, users can call this method to get an accurate look at the reduced value(s)
        given the current internal values list.

        Returned values are always on CPU memory.

        Args:
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            Returns the ema value on CPU memory.
        """
        item = single_value_to_cpu(self._item)

        if compile:
            return item
        return [item]

    def reduce(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Reduces the internal values.

        Returned values are always on CPU memory.

        Thereby, the internal values may be changed (note that this is different from
        `peek()`, where the internal values are NOT changed). See the docstring of this
        class for details on the reduction logic applied to the values, based on
        the constructor settings, such as `window`, `reduce`, etc..

        Args:
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            The reduced ema value on CPU memory.
        """
        item = single_value_to_cpu(self._item)

        if self._clear_on_reduce:
            self._item = np.nan

        if compile:
            return item
        return [item]

    def __repr__(self) -> str:
        return f"MeanStats({self.peek()}; len=(1); " f"ema_coeff={self._ema_coeff})"

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["ema_coeff"] = self._ema_coeff
        state["clear_on_reduce"] = self._clear_on_reduce
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self._ema_coeff = state["ema_coeff"]
        self._clear_on_reduce = state["clear_on_reduce"]

    @staticmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        super_args = SeriesStats._get_init_args(stats_object=stats_object, state=state)
        if state is not None:
            return {
                **super_args,
                "ema_coeff": state["ema_coeff"],
                "clear_on_reduce": state["clear_on_reduce"],
            }
        if stats_object is not None:
            return {
                **super_args,
                "ema_coeff": stats_object._ema_coeff,
                "clear_on_reduce": stats_object._clear_on_reduce,
            }
        else:
            raise ValueError("Either stats_object or state must be provided")
