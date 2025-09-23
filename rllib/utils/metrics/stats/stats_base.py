from collections import defaultdict, deque
import threading
import time
from typing import Any, Dict, List, Union, Optional, Tuple
from abc import ABCMeta, abstractmethod

import numpy as np

from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)

torch, _ = try_import_torch()


@DeveloperAPI
class Stats(metaclass=ABCMeta):
    """A base class for Stats."""

    def __init__(
        self,
        throughput: Union[bool, float] = False,
        clear_on_reduce: bool = False,
        window: Optional[Union[int, float]] = None,
        _is_root_stats: bool = False,
    ):
        """Initializes a Stats instance.

        Args:
            throughput: If True, track a throughput estimate together with this
                Stats. We then keep track of the time passed between two consecutive calls to push() and update its throughput estimate. The current throughput
                `reduce()` and update its throughput estimate. The current throughput
                estimate (1/s) can be obtained through Stats.throughput:
            window: The window size to reduce over.
            _is_root_stats: If True, the Stats object is the root Stats object.
                Meaning that it is kept by the root MetricsLogger.
        """
        # Timing functionality (keep start times per thread).
        self._start_times = defaultdict(lambda: None)

        self._throughput_stats = None
        if throughput is not False:
            from ray.rllib.utils.metrics.stats.mean import MeanStats

            self._throughput_stats = MeanStats()
            self._last_throughput_measure_time = -1

        # The actual, underlying data in this Stats object.
        self.values: Union[List, deque.Deque] = []

        self._is_tensor = False
        self._has_new_values = True
        self._is_root_stats = _is_root_stats
        self._window = window
        self._clear_on_reduce = clear_on_reduce

    @staticmethod
    def _get_base_stats_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        if state is None:
            return {
                "throughput": state["throughput"],
                "clear_on_reduce": state["clear_on_reduce"],
                "window": state["window"],
                "_is_root_stats": state["_is_root_stats"],
            }
        elif stats_object is not None:
            return {
                "throughput": stats_object._throughput,
                "clear_on_reduce": stats_object._clear_on_reduce,
                "window": stats_object._window,
                "_is_root_stats": stats_object._is_root_stats,
            }
        else:
            raise ValueError("Either stats_object or state must be provided")

    def check_value(self, value: Any) -> None:
        """Checks if a value is valid for this Stats object."""
        if isinstance(value, np.ndarray) and value.shape == ():
            return
        elif torch and torch.is_tensor(value):
            self._is_tensor = True
            if tuple(value.shape) == ():
                return
        elif type(value) not in (list, tuple, deque):
            return
        raise ValueError(
            f"Value ({value}) is required to be a scalar when using a reduce " "method!"
        )

    def __enter__(self) -> "Stats":
        """Called when entering a context (with which users can measure a time delta).

        Returns:
            This Stats instance (self), unless another thread has already entered (and
            not exited yet), in which case a copy of `self` is returned. This way, the
            second thread(s) cannot mess with the original Stat's (self) time-measuring.
            This also means that only the first thread to __enter__ actually logs into
            `self` and the following threads' measurements are discarded (logged into
            a non-referenced shim-Stats object, which will simply be garbage collected).
        """
        # In case another thread already is measuring this Stats (timing), simply ignore
        # the "enter request" and return a clone of `self`.
        thread_id = threading.get_ident()
        self._start_times[thread_id] = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        """Called when exiting a context (with which users can measure a time delta)."""
        thread_id = threading.get_ident()
        assert self._start_times[thread_id] is not None
        time_delta_s = time.perf_counter() - self._start_times[thread_id]
        self.push(time_delta_s)

        del self._start_times[thread_id]

    @property
    def throughput(self) -> float:
        """Returns the current throughput estimate per second.

        Raises:
            ValueError: If throughput tracking is not enabled for this Stats object.

        Returns:
            The current throughput estimate per second.
        """
        if not self.has_throughput:
            raise ValueError("Throughput tracking is not enabled for this Stats object")
        # We can always return the first value here because throughput is a single value
        return self._throughput_stats.peek()

    @property
    def has_throughput(self) -> bool:
        """Returns whether this Stats object tracks throughput.

        Returns:
            True if this Stats object has throughput tracking enabled, False otherwise.
        """
        return self._throughput_stats is not None

    def reduce(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Reduces the internal values list according to the constructor settings.

        Thereby, the internal values list is changed (note that this is different from
        `peek()`, where the internal list is NOT changed). See the docstring of this
        class for details on the reduction logic applied to the values list, based on
        the constructor settings, such as `window`, `reduce`, etc..

        Args:
            compile: If True, the result is compiled into a single value if possible.
                If it is not possible, the result is a list of values.
                If False, the result is a list of one or more values.

        Returns:
            The reduced value (can be of any type, depending on the input values and
            reduction method).
        """
        return_values, new_internal_values = self.reduced_values

        if self._clear_on_reduce:
            self._set_values([])
        else:
            self._set_values(new_internal_values)

        if compile:
            return return_values[0]
        else:
            return return_values

    def clear_throughput(self) -> None:
        """Clears the throughput Stats, if applicable and `self` has throughput.

        Also resets `self._last_throughput_measure_time` to -1 such that the Stats
        object has to create a new timestamp first, before measuring any new throughput
        values.
        """
        if self.has_throughput:
            self._throughput_stats._set_values([])
            self._last_throughput_measure_time = -1

    def _recompute_throughput(self, value) -> None:
        """Recomputes the current throughput value of this Stats instance."""
        # Make sure this Stats object does measure throughput.
        assert self.has_throughput
        # Take the current time stamp.
        current_time = time.perf_counter()
        # Check, whether we have a previous timestamp (non -1).
        if self._last_throughput_measure_time >= 0:
            # Compute the time delta.
            time_diff = current_time - self._last_throughput_measure_time
            # Avoid divisions by zero.
            if time_diff > 0:
                # Push new throughput value into our throughput stats object.
                self._throughput_stats.push(value / time_diff)
        # Update the time stamp of the most recent throughput computation (this one).
        self._last_throughput_measure_time = current_time

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

    def __float__(self):
        return float(self.peek())

    def __eq__(self, other):
        return float(self) == float(other)

    def __le__(self, other):
        return float(self) <= float(other)

    def __ge__(self, other):
        return float(self) >= float(other)

    def __lt__(self, other):
        return float(self) < float(other)

    def __gt__(self, other):
        return float(self) > float(other)

    def __add__(self, other):
        return float(self) + float(other)

    def __sub__(self, other):
        return float(self) - float(other)

    def __mul__(self, other):
        return float(self) * float(other)

    def __format__(self, fmt):
        return f"{float(self):{fmt}}"

    def get_state(self) -> Dict[str, Any]:
        state = {
            # Make sure we don't return any tensors here.
            "values": convert_to_numpy(self.values),
            "window": self._window,
            "clear_on_reduce": self._clear_on_reduce,
            "_is_tensor": self._is_tensor,
        }
        if self._throughput_stats is not None:
            state["throughput_stats"] = self._throughput_stats.get_state()
        return state

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
        # If throughput tracking is enabled, calculate it based on time between pushes
        if self.has_throughput:
            self._recompute_throughput(value)

        self._push(value)

        # Mark that we have new values
        self._has_new_values = True

    def merge(self, *others: "Stats") -> None:
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
        assert self._is_root_stats, "Stats should only be merged at root level"

        # Make sure that all incoming state have the same type.
        for s in [self, *others]:
            if not isinstance(s, self.__class__):
                raise ValueError(f"All incoming stats must be of type {self.__class__}")

        # If any of the value lists have a length of 0 or if there is only one value and
        # it is nan, we skip
        stats_to_merge = [
            s
            for s in [self, *others]
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
        elif len(stats_to_merge) == 1:
            if stats_to_merge[0] == self:
                # If no incoming stats have values, return.
                return
            else:
                # If there is only one stat with values, and it's incoming, copy its
                # values.
                self.values = stats_to_merge[0].values
                return

        new_values = self._merge_in_parallel(*stats_to_merge)

        self._set_values(new_values)

        self._has_new_values = True

    def _torch_or_numpy_reduce(self, values: List[Any], reduce_method: str) -> Any:
        torch_reduce_meth = getattr(torch, "nan" + reduce_method)
        np_reduce_meth = getattr(np, "nan" + reduce_method)

        # Use the numpy/torch "nan"-prefix to ignore NaN's in our value lists.
        if torch and torch.is_tensor(values[0]):
            self._is_tensor = True
            if len(values[0].shape) == 0:
                reduced = values[0]
            else:
                reduce_in = torch.stack(list(values))
                reduced = torch_reduce_meth(reduce_in)
        else:
            if np.all(np.isnan(values)):
                # This avoids warnings for taking a mean of an empty array.
                reduced = np.nan
            else:
                reduced = np_reduce_meth(values)

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

    @classmethod
    def from_state(cls, state: Dict[str, Any]) -> "Stats":
        """Creates a Stats object from a state dictionary

        Any implementation of this should call this base classe's `Stats.set_state()` to set the state of the Stats object.
        """
        stats = cls(**cls._get_init_args(state=state))
        stats.set_state(state)
        return stats

    @classmethod
    def similar_to(
        cls,
        other: "Stats",
    ) -> "Stats":
        """Returns a new Stats object that's similar to `other`.

        "Similar" here means it has the exact same settings (reduce, window, ema_coeff,
        etc..). The initial values of the returned `Stats` are empty by default, but
        can be set as well.

        Args:
            other: The other Stats object to return a similar new Stats equivalent for.
            init_value: The initial value to already push into the returned Stats.

        Returns:
            A new Stats object similar to `other`, with the exact same settings and
            maybe a custom initial value (if provided; otherwise empty).
        """
        return cls(**cls._get_init_args(stats_object=other))

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def set_state(self, state: Dict[str, Any]) -> None:
        """Sets the state of the Stats object."""
        self._set_values(state["values"])
        self._clear_on_reduce = state["clear_on_reduce"]
        if "throughput_stats" in state:
            self._throughput_stats = Stats.from_state(state["throughput_stats"])
        else:
            self._throughput_stats = None

    @abstractmethod
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
        ...

    @abstractmethod
    def _merge_in_parallel(stats: List["Stats"]) -> List[Union[int, float]]:
        """Merges all internal values of `stats`.

        Thereby, the newly incoming values of `stats` are treated equally with respect
        to each other.

        Args:
            others: One or more other Stats objects that need to be parallely merged
                into `self, meaning with equal weighting as the existing values in
                `self`.
        """
        ...

    @abstractmethod
    def __repr__(self) -> str:
        ...

    @abstractmethod
    def _get_init_args(stats_object=None, state=None) -> Dict[str, Any]:
        """Returns the initialization arguments for this Stats object."""
        ...

    def peek(self, compile: bool = True) -> Union[Any, List[Any]]:
        """Returns the result of reducing the internal values list.

        Note that this method does NOT alter the internal values list in this process.
        Thus, users can call this method to get an accurate look at the reduced value(s)
        given the current internal values list.

        Args:
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            The result of reducing the internal values list.
        """
        reduced_values, _ = self.reduced_values
        return reduced_values[0] if compile else reduced_values


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


@DeveloperAPI
def merge_stats(root_stats: Optional[Stats], incoming_stats: List[Stats]) -> Stats:
    """Merges Stats objects.

    If `root_stats` is None, we use the first incoming Stats object as the new base Stats object.
    If `root_stats` is not None, we merge all incoming Stats objects into the base Stats object.

    Args:
        root_stats: The base Stats object to merge into.
        incoming_stats: The list of Stats objects to merge.

    Returns:
        The merged Stats object.
    """
    if root_stats is None:
        # This should happen the first time we reduce this stat to the root logger
        root_stats = incoming_stats[0].similar_to(incoming_stats[0])
        root_stats._is_root_stats = True

    root_stats.merge(*incoming_stats)

    if root_stats.has_throughput:
        root_stats._recompute_throughput(incoming_stats)

    return root_stats
