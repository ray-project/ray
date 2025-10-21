from typing import Any, Dict, List, Union

import numpy as np

from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_torch, try_import_tf
from ray.rllib.utils.metrics.stats.base import StatsBase

torch, _ = try_import_torch()
_, tf, _ = try_import_tf()


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

        Note the follwing limitation: That, when calling `EmaStats.merge()`, we take the
        mean of the incoming means. This also means that merges replace the current value.
        If you merge multiple times (for example during a single reduce cycle), the reduce will only reflect the last merge.

        Args:
            ema_coeff: The EMA coefficient to use. Defaults to 0.01.
        """
        super().__init__(*args, **kwargs)
        self._value = np.nan
        self._ema_coeff = ema_coeff

    def __len__(self) -> int:
        """Returns the length of the internal values list."""
        return 1

    def merge(self, incoming_stats: List["EmaStats"]):
        """Merges StatsBase objects.

        Args:
            incoming_stats: The list of StatsBase objects to merge.

        Returns:
            The merged StatsBase object.
        """
        all_values = [stat._value for stat in incoming_stats]
        if len(all_values) == 0:
            return

        self._value = np.nanmean(all_values)

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

        # Handle EMA calculation
        if torch and isinstance(self._value, torch.Tensor):
            # GPU tensor EMA
            self._value = (
                self._ema_coeff * value + (1.0 - self._ema_coeff) * self._value
            )
        elif (
            torch
            and isinstance(value, torch.Tensor)
            and (isinstance(self._value, float) and np.isnan(self._value))
        ):
            # First value is GPU tensor
            self._value = value
        elif not (isinstance(self._value, float) and np.isnan(self._value)):
            # CPU value EMA
            self._value = (
                self._ema_coeff * value + (1.0 - self._ema_coeff) * self._value
            )
        else:
            # First value
            self._value = value

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Returns the current EMA value.

        If value is a GPU tensor, it's converted to CPU.
        """
        value = self._value
        # Convert GPU tensor to CPU
        if torch and isinstance(value, torch.Tensor):
            value = value.detach().cpu().item()
        return value if compile else [value]

    def reduce(self, compile: bool = True) -> Union[Any, "EmaStats"]:
        """Reduces the internal value.

        If value is a GPU tensor, it's converted to CPU.
        """
        value = self._value
        # Convert GPU tensor to CPU
        if torch and isinstance(value, torch.Tensor):
            value = value.detach().cpu().item()

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
