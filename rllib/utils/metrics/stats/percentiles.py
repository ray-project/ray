from collections import deque
from itertools import chain
from typing import Any, Dict, List, Optional, Union

from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics.stats.base import StatsBase
from ray.rllib.utils.metrics.stats.utils import batch_values_to_cpu, safe_isnan
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()
_, tf, _ = try_import_tf()


@DeveloperAPI
class PercentilesStats(StatsBase):
    """A Stats object that tracks percentiles of a series of singular values (not vectors)."""

    stats_cls_identifier = "percentiles"

    def __init__(
        self,
        percentiles: Union[List[int], bool] = None,
        window: Optional[Union[int, float]] = None,
        *args,
        **kwargs,
    ):
        """Initializes a PercentilesStats instance.

        Percentiles are computed over the last `window` values across all parallel components.
        Example: If we have 10 parallel components, and each component tracks 1,000 values, we will track the last 10,000 values across all components.
        Be careful to not track too many values because computing percentiles is O(n*log(n)) where n is the window size.
        See https://github.com/ray-project/ray/pull/52963 for more details.

        Args:
            percentiles: The percentiles to track.
                If None, track the default percentiles [0, 50, 75, 90, 95, 99, 100].
                If a list, track the given percentiles.
        """
        super().__init__(*args, **kwargs)

        self._window = window
        self.values: Union[List[Any], deque[Any]] = []
        self._set_values([])

        if percentiles is None:
            # We compute a bunch of default percentiles because computing one is just as expensive as computing all of them.
            percentiles = [0, 50, 75, 90, 95, 99, 100]
        elif isinstance(percentiles, list):
            percentiles = percentiles
        else:
            raise ValueError("`percentiles` must be a list or None")

        self._percentiles = percentiles

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["values"] = self.values
        state["window"] = self._window
        state["percentiles"] = self._percentiles
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        super().set_state(state)
        self._set_values(state["values"])
        self._window = state["window"]
        self._percentiles = state["percentiles"]

    def _set_values(self, new_values):
        # For stats with window, use a deque with maxlen=window.
        # This way, we never store more values than absolutely necessary.
        if self._window and self.is_leaf:
            # Window always counts at leafs only (or non-root stats)
            self.values = deque(new_values, maxlen=self._window)
        # For infinite windows, use `new_values` as-is (a list).
        else:
            self.values = new_values

    def __len__(self) -> int:
        """Returns the length of the internal values list."""
        return len(self.values)

    def __float__(self):
        raise ValueError(
            "Cannot convert to float because percentiles are not reduced to a single value."
        )

    def __eq__(self, other):
        self._comp_error("__eq__")

    def __le__(self, other):
        self._comp_error("__le__")

    def __ge__(self, other):
        self._comp_error("__ge__")

    def __lt__(self, other):
        self._comp_error("__lt__")

    def __gt__(self, other):
        self._comp_error("__gt__")

    def __add__(self, other):
        self._comp_error("__add__")

    def __sub__(self, other):
        self._comp_error("__sub__")

    def __mul__(self, other):
        self._comp_error("__mul__")

    def _comp_error(self, comp):
        raise NotImplementedError()

    def __format__(self, fmt):
        raise ValueError(
            "Cannot format percentiles object because percentiles are not reduced to a single value."
        )

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
        if safe_isnan(value):
            raise ValueError("NaN values are not allowed in PercentilesStats")

        if torch and isinstance(value, torch.Tensor):
            value = value.detach()

        self.values.append(value)

    def merge(self, incoming_stats: List["PercentilesStats"]) -> None:
        """Merges PercentilesStats objects.

        This method assumes that the incoming stats have the same percentiles and window size.
        It will append the incoming values to the existing values.

        Args:
            incoming_stats: The list of PercentilesStats objects to merge.

        Returns:
            None. The merge operation modifies self in place.
        """
        assert (
            not self.is_leaf
        ), "PercentilesStats should only be merged at aggregation stages (root or intermediate)"
        assert all(
            s._percentiles == self._percentiles for s in incoming_stats
        ), "All incoming PercentilesStats objects must have the same percentiles"
        assert all(
            s._window == self._window for s in incoming_stats
        ), "All incoming PercentilesStats objects must have the same window size"
        new_values = [s.values for s in incoming_stats]
        new_values = list(chain.from_iterable(new_values))
        all_values = list(self.values) + new_values
        self.values = all_values

        # Track merged values for latest_merged_only peek functionality
        if not self.is_leaf:
            # Store the values that were merged in this operation (from incoming_stats only)
            self.latest_merged = new_values

    def peek(
        self, compile: bool = True, latest_merged_only: bool = False
    ) -> Union[Any, List[Any]]:
        """Returns the result of reducing the internal values list.

        Note that this method does NOT alter the internal values list in this process.
        Thus, users can call this method to get an accurate look at the reduced value(s)
        given the current internal values list.

        Args:
            compile: If True, the result is compiled into the percentiles list.
            latest_merged_only: If True, only considers the latest merged values.
                This parameter only works on aggregation stats (root or intermediate nodes).
                When enabled, peek() will only use the values from the most recent merge operation.

        Returns:
            The result of reducing the internal values list on CPU.
        """
        # Check latest_merged_only validity
        if latest_merged_only and self.is_leaf:
            raise ValueError(
                "latest_merged_only can only be used on aggregation stats objects (is_leaf=False)."
            )

        # If latest_merged_only is True, use only the latest merged values
        if latest_merged_only:
            if self.latest_merged is None:
                # No merged values yet, return dict with None values
                if compile:
                    return {p: None for p in self._percentiles}
                else:
                    return []
            # Use only the latest merged values
            latest_merged = self.latest_merged
            values = batch_values_to_cpu(latest_merged)
        else:
            # Normal peek behavior
            values = batch_values_to_cpu(self.values)

        values.sort()

        if compile:
            return compute_percentiles(values, self._percentiles)
        return values

    def reduce(self, compile: bool = True) -> Union[Any, "PercentilesStats"]:
        """Reduces the internal values list.

        Args:
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            The reduced value on CPU.
        """
        values = batch_values_to_cpu(self.values)

        values.sort()

        self._set_values([])

        if compile:
            return compute_percentiles(values, self._percentiles)

        return_stats = self.clone()
        return_stats.values = values
        return return_stats

    def __repr__(self) -> str:
        return f"PercentilesStats({self.peek()}; window={self._window}; len={len(self)}"

    @staticmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        super_args = StatsBase._get_init_args(stats_object=stats_object, state=state)
        if state is not None:
            return {
                **super_args,
                "percentiles": state["percentiles"],
                "window": state["window"],
            }
        elif stats_object is not None:
            return {
                **super_args,
                "percentiles": stats_object._percentiles,
                "window": stats_object._window,
            }
        else:
            raise ValueError("Either stats_object or state must be provided")


@DeveloperAPI
def compute_percentiles(sorted_list, percentiles):
    """Compute percentiles from an already sorted list.

    Note that this will not raise an error if the list is not sorted to avoid overhead.

    Args:
        sorted_list: A list of numbers sorted in ascending order
        percentiles: A list of percentile values (0-100)

    Returns:
        A dictionary mapping percentile values to their corresponding data values
    """
    n = len(sorted_list)

    if n == 0:
        return {p: None for p in percentiles}

    results = {}

    for p in percentiles:
        index = (p / 100) * (n - 1)
        if index.is_integer():
            results[p] = sorted_list[int(index)]
        else:
            lower_index = int(index)
            upper_index = lower_index + 1
            weight = index - lower_index
            results[p] = (
                sorted_list[lower_index] * (1 - weight)
                + sorted_list[upper_index] * weight
            )

    return results
