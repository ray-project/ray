from typing import Any, Dict, List, Union

import numpy as np

from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.base import StatsBase
from ray.rllib.utils.metrics.stats.utils import single_value_to_cpu


@DeveloperAPI
class EmaStats(StatsBase):
    """A Stats object that tracks the exponential average of a series of values."""

    stats_cls_identifier = "ema"

    def __init__(
        self,
        ema_coeff: float = 0.01,
        *args,
        **kwargs,
    ):
        """Initializes a EmaStats instance.

        Note the follwing limitation: That, when aggregating EmaStats objects with `EmaStats.merge()`, we take the
        mean of the incoming means. The output of the aggregated stats is therefore not
        the exponetially moving average of the values that are logged by parallel components
        in order, but rather the mean of the EMA logged by parallel components.

        The merged value of multiple components can therefore only be trusted if all there is a reasonable amount of
        values logged by each component in each 'reduce cycle'.

        Args:
            ema_coeff: The EMA coefficient to use. Defaults to 0.01.
        """
        super().__init__(*args, **kwargs)
        self._value = np.nan
        self._ema_coeff = ema_coeff

    def __len__(self) -> int:
        """Returns the length of the internal values list."""
        return 1

    def merge(self, incoming_stats: List["EmaStats"], replace=True):
        """Merges StatsBase objects.

        Args:
            incoming_stats: The list of StatsBase objects to merge.
            replace: If True, replace internal items with the result of the merge.

        Returns:
            The merged StatsBase object.
        """
        # Take the average of the incoming means
        all_values = [stat._value for stat in incoming_stats]
        if not replace:
            raise ValueError(
                "Can only replace EmaStats when merging. Otherwise we would introduce an 'EMA of EMAs'."
            )
        if len(all_values) == 0:
            return

        self._value = np.nanmean(all_values)

    def push(self, value: Any) -> None:
        value = single_value_to_cpu(value)
        if self._value is not np.nan:
            self._value = (
                self._ema_coeff * value + (1.0 - self._ema_coeff) * self._value
            )
        else:
            self._value = value

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        return self._value if compile else [self._value]

    def reduce(self, compile: bool = True) -> Union[Any, "EmaStats"]:
        value = self._value
        self._value = np.nan

        if compile:
            return value

        return_stats = self.clone(self)
        return_stats._value = value
        return return_stats

    def __repr__(self) -> str:
        return f"EmaStats({self.peek()}; len=(1); " f"ema_coeff={self._ema_coeff})"

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["ema_coeff"] = self._ema_coeff
        state["value"] = self._value
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self._ema_coeff = state["ema_coeff"]
        self._value = state["value"]

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
