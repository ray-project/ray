from typing import Any, Dict, List, Union, Tuple

import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.stats_base import Stats

torch, _ = try_import_torch()


@DeveloperAPI
class MaxStats(Stats):
    """A Stats object that tracks the max of the values."""

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """Initializes a MaxStats instance."""
        super().__init__(*args, **kwargs)

    def _push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
        """
        # For windowed operations, append to values and trim if needed
        self.values.append(value)
        _, new_internal_values = self.reduced_values
        self._set_values(new_internal_values)

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

        # Special case: Internal values list is empty -> return NaN or 0.0 for max.
        if len(values) == 0:
            return [np.nan], []

        return self._torch_or_numpy_reduce(values, "max")

    def _merge_in_parallel(self, *stats: "Stats") -> List[Union[int, float]]:
        return np.nanmax([s.reduced_values[0] for s in stats])

    def __repr__(self) -> str:
        return (
            f"MaxStats({self.peek()}; window={self._window}; len={len(self)}; "
            f"clear_on_reduce={self._clear_on_reduce})"
        )

    def _get_init_args(self, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        return self._get_base_stats_init_args(stats_object=self, state=state)
