import heapq
from typing import Any, Dict, List, Union, Tuple
from abc import abstractmethod


from ray.rllib.utils.framework import try_import_torch
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.metrics.stats.series import SeriesStats

torch, _ = try_import_torch()


@DeveloperAPI
class PercentilesStats(SeriesStats):
    """A Stats object that tracks percentiles of a series of values."""

    stats_cls_identifier = "percentiles"

    def __init__(
        self,
        percentiles: Union[List[int], bool] = None,
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

        if percentiles is None:
            percentiles = [0, 50, 75, 90, 95, 99, 100]
        elif isinstance(percentiles, list):
            percentiles = percentiles
        else:
            raise ValueError("`percentiles` must be a list or None")

        self._percentiles = percentiles

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
        # Sort values
        values = list(values)
        # (Artur): Numpy can sort faster than Python's built-in sort for large lists. Howoever, if we convert to an array here
        # and then sort, this only slightly (<2x) improved the runtime of this method, even for an internal values list of 1M values.
        values.sort()
        return values, values

    @abstractmethod
    def _merge_in_parallel(self, *others: "PercentilesStats") -> None:
        """Merges all internal values of `others` into `self`'s internal values list.

        Thereby, the newly incoming values of `others` are treated equally with respect
        to each other as well as with respect to the internal values of self.

        Use this method to merge other `Stats` objects, which resulted from some
        parallelly executed components, into this one. For example: n Learner workers
        all returning a loss value in the form of `{"total_loss": [some value]}`.

        The following examples demonstrate the parallel merging logic for different
        reduce- and window settings:

        Args:
            others: One or more other Stats objects that need to be parallely merged
                into `self, meaning with equal weighting as the existing values in
                `self`.
        """
        # Use heapq to sort values (assumes that the values are already sorted)
        # and then pick the correct percentiles
        lists_to_merge = [list(self.values), *[list(o.values) for o in others]]
        merged = list(heapq.merge(*lists_to_merge))
        self._set_buffer(merged)

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
        reduced_values, _ = self.reduced_values
        if compile:
            return compute_percentiles(reduced_values, self._percentiles)
        return reduced_values

    def __repr__(self) -> str:
        return (
            f"PercentilesStats({self.peek()}; window={self._window}; len={len(self)}; "
            f"clear_on_reduce={self._clear_on_reduce})"
        )

    @abstractmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        super_args = super()._get_init_args(stats_object=stats_object, state=state)
        if state is not None:
            return {
                **super_args,
                "percentiles": state["percentiles"],
            }
        elif stats_object is not None:
            return {
                **super_args,
                "percentiles": stats_object._percentiles,
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
