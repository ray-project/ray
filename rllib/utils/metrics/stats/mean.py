from typing import Any, Dict, List, Union, Optional, Tuple

import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.stats_base import Stats

torch, _ = try_import_torch()


@DeveloperAPI
class MeanStats(Stats):
    """A Stats object that tracks the mean of the values."""

    def __init__(
        self,
        ema_coeff: Optional[float] = None,
        sum_up_parallel_values: bool = False,
        *args,
        **kwargs,
    ):
        """Initializes a MeanStats instance.

        Args:
            ema_coeff: The EMA coefficient to use if no `window` is provided. Defaults to
                0.05.
            sum_up_parallel_values: If True, the mean is computed by summing up the values and dividing by the number of values.
        """
        super().__init__(*args, **kwargs)

        if (
            "window" in kwargs
            and kwargs["window"] is not None
            and ema_coeff is not None
        ):
            raise ValueError("Only one of `window` or `ema_coeff` can be specified!")

        if "window" in kwargs and kwargs["window"] is None and ema_coeff is None:
            ema_coeff = 0.05

        self._ema_coeff = ema_coeff
        self._sum_up_parallel_values = sum_up_parallel_values

    def _push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
        """
        # For windowed operations, append to values and trim if needed
        self.values.append(value)
        if self._window is not None and len(self.values) > self._window:
            self.values.popleft()

    @property
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

        # Special case: Internal values list is empty -> return NaN or 0.0 for mean.
        if len(values) == 0:
            return [0], []

        # Perform EMA reduction over all values in internal values list.
        mean_value = values[0]
        for v in values[1:]:
            mean_value = self._ema_coeff * v + (1.0 - self._ema_coeff) * mean_value

        return [mean_value], [mean_value]

    def _merge_in_parallel(self, *stats: "Stats") -> List[Union[int, float]]:
        new_values = []
        tmp_values = []

        for i in range(1, max(map(len, stats)) + 1):
            # Per index, loop through all involved stats, including `self` and add
            # to `tmp_values`.
            for s in stats:
                if len(s) < i:
                    continue
                tmp_values.append(s.values[-i])

            if self._sum_up_parallel_values:
                new_values.extend([np.nansum(tmp_values)])
            else:
                new_values.extend([np.nanmean(tmp_values)])

            tmp_values.clear()
            if len(new_values) >= self._window:
                new_values = new_values[: self._window]
                break

        return list(reversed(new_values))

    def __repr__(self) -> str:
        return (
            f"MeanStats({self.peek()}; window={self._window}; len={len(self)}; "
            f"clear_on_reduce={self._clear_on_reduce}); sum_up_parallel_values={self._sum_up_parallel_values})"
        )

    def _get_init_args(self, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        init_args = self._get_base_stats_init_args(stats_object=self, state=state)
        init_args["sum_up_parallel_values"] = self._sum_up_parallel_values
        init_args["ema_coeff"] = self._ema_coeff

        return init_args
