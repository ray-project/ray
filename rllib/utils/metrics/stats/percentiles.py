from typing import Any, Dict, List, Union, Optional

from collections import deque
from itertools import chain

from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import StatsBase
from ray.rllib.utils.metrics.stats.utils import single_value_to_cpu


@DeveloperAPI
class PercentilesStats(StatsBase):
    """A Stats object that tracks percentiles of a series of values."""

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
        if self._window and not self._is_root_stats:
            # Window always counts at leafs only
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
        raise ValueError(
            f"Cannot {comp} percentiles object to other object because percentiles are not reduced to a single value."
        )

    def __format__(self, fmt):
        raise ValueError(
            "Cannot format percentiles object because percentiles are not reduced to a single value."
        )

    def push(self, value: Any) -> None:
        value = single_value_to_cpu(value)
        self.values.append(value)

    def merge(self, incoming_stats: List["PercentilesStats"], replace=True):
        """Merges PercentilesStats objects.

        This method assumes that the incoming stats have the same percentiles and window size.
        It will replace the internal values with the merged values.

        Args:
            incoming_stats: The list of PercentilesStats objects to merge.
        """
        assert (
            self._is_root_stats
        ), "PercentilesStats should only be merged at root level"
        all_items = [s.values for s in incoming_stats]
        all_items = list(chain.from_iterable(all_items))
        if not replace:
            all_items = list(self.values) + all_items
        self._set_values(all_items)

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Returns the result of reducing the internal values list.

        Note that this method does NOT alter the internal values list in this process.
        Thus, users can call this method to get an accurate look at the reduced value(s)
        given the current internal values list.

        Args:
            compile: If True, the result is compiled into the percentiles list.

        Returns:
            The result of reducing the internal values list.
        """
        values = list(self.values)
        # (Artur): Numpy can sort faster than Python's built-in sort for large lists. Howoever, if we convert to an array here
        # and then sort, this only slightly (<2x) improved the runtime of this method, even for an internal values list of 1M values.
        values.sort()

        if compile:
            return compute_percentiles(values, self._percentiles)
        return values

    def reduce(self, compile: bool = True) -> Union[Any, "PercentilesStats"]:
        """Reduces the internal values list.

        Args:
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            The reduced value.
        """
        values = list(self.values)
        values.sort()

        if self._reduce_at_root and not self._is_root_stats:
            # We sort values at leafs in any case to avoid sorting at root level.
            if compile:
                raise ValueError(
                    "Can not compile at leaf level if reduce_at_root is True"
                )
            return_stats = self.clone(self)
            return_stats.values = values
            return return_stats

        if self._clear_on_reduce:
            self._set_values([])
        else:
            self._set_values(values)

        if compile:
            return compute_percentiles(values, self._percentiles)

        return_stats = self.clone(self)
        return_stats.values = values
        return return_stats

    def __repr__(self) -> str:
        return (
            f"PercentilesStats({self.peek()}; window={self._window}; len={len(self)}; "
            f"clear_on_reduce={self._clear_on_reduce})"
        )

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
