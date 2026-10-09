import copy
import heapq
import threading
import time
import uuid
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np

from ray._common.deprecation import Deprecated
from ray.rllib.utils import force_list
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()


@Deprecated(new="rllib.utils.metrics.stats", error=False)
@DeveloperAPI
class Stats:
    """A container class holding a number of values and executing reductions over them.

    The individual values in a Stats object may be of any type, for example python int
    or float, numpy arrays, or more complex structured (tuple, dict) and are stored in
    a list under `self.values`. This class is not meant to be interfaced with directly
    from application code. Instead, use `MetricsLogger` to log and manipulate Stats.

    Stats can be used to store metrics of the same type over time, for example a loss
    or a learning rate, and to reduce all stored values applying a certain reduction
    mechanism (for example "mean" or "sum").

    Available reduction mechanisms are:
    - "mean" using EMA with a configurable EMA coefficient.
    - "mean" using a sliding window (over the last n stored values).
    - "max/min" with an optional sliding window (over the last n stored values).
    - "sum" with an optional sliding window (over the last n stored values).
    - None: Simply store all logged values to an ever-growing list.

    Through the `reduce()` API, one of the above-mentioned reduction mechanisms will
    be executed on `self.values`.
    """

    def __init__(
        self,
        init_values: Optional[Any] = None,
        reduce: Optional[str] = "mean",
        percentiles: Union[List[int], bool] = False,
        reduce_per_index_on_aggregate: bool = False,
        window: Optional[Union[int, float]] = None,
        ema_coeff: Optional[float] = None,
        clear_on_reduce: bool = False,
        throughput: Union[bool, float] = False,
        throughput_ema_coeff: Optional[float] = None,
    ):
        """Initializes a Stats instance.

        Args:
            init_values: Optional initial values to be placed into `self.values`. If None,
                `self.values` will start empty. If percentiles is True, values must be ordered
                if provided.
            reduce: The name of the reduce method to be used. Allowed are "mean", "min",
                "max", and "sum". Use None to apply no reduction method (leave
                `self.values` as-is when reducing, except for shortening it to
                `window`). Note that if both `reduce` and `window` are None, the user of
                this Stats object needs to apply some caution over the values list not
                growing infinitely.
            percentiles: If reduce is `None`, we can compute the percentiles of the
                values list given by `percentiles`. Defaults to [0, 50, 75, 90, 95,
                99, 100] if set to True. When using percentiles, a window must be provided.
                This window should be chosen carfully. RLlib computes exact percentiles and
                the computational complexity is O(m*n*log(n/m)) where n is the window size
                and m is the number of parallel metrics loggers invovled (for example,
                m EnvRunners). To be safe, choose a window < 1M and less than 1000 Stats
                objects to aggregate. See #52963 for more details.
            window: An optional window size to reduce over.
                If `window` is not None, then the reduction operation is only applied to
                the most recent `windows` items, and - after reduction - the values list
                is shortened to hold at most `window` items (the most recent ones).
                Must be None if `ema_coeff` is not None.
                If `window` is None (and `ema_coeff` is None), reduction must not be
                "mean".
            reduce_per_index_on_aggregate: If True, when merging Stats objects, we reduce
                incoming values per index such that the new value at index `n` will be
                the reduced value of all incoming values at index `n`.
                If False, when reducing `n` Stats, the first `n` merged values will be
                the reduced value of all incoming values at index `0`, the next `n` merged
                values will be the reduced values of all incoming values at index `1`, etc.
            ema_coeff: An optional EMA coefficient to use if reduce is "mean"
                and no `window` is provided. Note that if both `window` and `ema_coeff`
                are provided, an error is thrown. Also, if `ema_coeff` is provided,
                `reduce` must be "mean".
                The reduction formula for EMA performed by Stats is:
                EMA(t1) = (1.0 - ema_coeff) * EMA(t0) + ema_coeff * new_value
            clear_on_reduce: If True, the Stats object will reset its entire values list
                to an empty one after `self.reduce()` is called. However, it will then
                return from the `self.reduce()` call a new Stats object with the
                properly reduced (not completely emptied) new values. Setting this
                to True is useful for cases, in which the internal values list would
                otherwise grow indefinitely, for example if reduce is None and there
                is no `window` provided.
            throughput: If True, track a throughput estimate together with this
                Stats. This is only supported for `reduce=sum` and
                `clear_on_reduce=False` metrics (aka. "lifetime counts"). The `Stats`
                then keeps track of the time passed between two consecutive calls to
                `reduce()` and update its throughput estimate. The current throughput
                estimate can be obtained through:
                `throughput_per_sec = Stats.peek(throughput=True)`.
                If a float, track throughput and also set current throughput estimate
                to the given value.
            throughput_ema_coeff: An optional EMA coefficient to use for throughput tracking.
                Only used if throughput=True.
        """
        # Thus far, we only support mean, max, min, and sum.
        if reduce not in [None, "mean", "min", "max", "sum", "percentiles"]:
            raise ValueError(
                "`reduce` must be one of `mean|min|max|sum|percentiles` or None!"
            )

        # One or both window and ema_coeff must be None.
        if window is not None and ema_coeff is not None:
            raise ValueError("Only one of `window` or `ema_coeff` can be specified!")
        # If `ema_coeff` is provided, `reduce` must be "mean".
        if ema_coeff is not None and reduce != "mean":
            raise ValueError(
                "`ema_coeff` arg only allowed (not None) when `reduce=mean`!"
            )

        if percentiles is not False:
            if reduce is not None:
                raise ValueError(
                    "`reduce` must be `None` when `percentiles` is not `False`!"
                )
            if window in (None, float("inf")):
                raise ValueError(
                    "A window must be specified when reduce is 'percentiles'!"
                )
            if reduce_per_index_on_aggregate is not False:
                raise ValueError(
                    f"`reduce_per_index_on_aggregate` ({reduce_per_index_on_aggregate})"
                    f" must be `False` when `percentiles` is not `False`!"
                )

            if percentiles is True:
                percentiles = [0, 50, 75, 90, 95, 99, 100]
            else:
                if type(percentiles) not in (bool, list):
                    raise ValueError("`percentiles` must be a list or bool!")
                if isinstance(percentiles, list):
                    if not all(isinstance(p, (int, float)) for p in percentiles):
                        raise ValueError(
                            "`percentiles` must contain only ints or floats!"
                        )
                    if not all(0 <= p <= 100 for p in percentiles):
                        raise ValueError(
                            "`percentiles` must contain only values between 0 and 100!"
                        )

        self._percentiles = percentiles

        # If `window` is explicitly set to inf, `clear_on_reduce` must be True.
        self._inf_window = window in [None, float("inf")]

        # If `window` is set to inf, `clear_on_reduce` must be True.
        # Otherwise, we risk a memory leak.
        if self._inf_window and not clear_on_reduce and reduce is None:
            raise ValueError(
                "When using an infinite window without reduction, `clear_on_reduce` must "
                "be set to True!"
            )

        # If reduce=mean AND window=ema_coeff=None, we use EMA by default with a coeff
        # of 0.01 (we do NOT support infinite window sizes for mean as that would mean
        # to keep data in the cache forever).
        if reduce == "mean" and self._inf_window and ema_coeff is None:
            ema_coeff = 0.01

        self._reduce_method = reduce
        self._window = window
        self._ema_coeff = ema_coeff

        if (
            self._reduce_method not in ["mean", "sum", "min", "max"]
            and reduce_per_index_on_aggregate
        ):
            raise ValueError(
                "reduce_per_index_on_aggregate is only supported for mean, sum, min, and max reduction!"
            )

        self._reduce_per_index_on_aggregate = reduce_per_index_on_aggregate

        # Timing functionality (keep start times per thread).
        self._start_times = defaultdict(lambda: None)

        # Simply store ths flag for the user of this class.
        self._clear_on_reduce = clear_on_reduce

        self._has_returned_zero = False

        # On each `.reduce()` call, we store the result of this call in
        # self._last_reduce.
        self._last_reduced = [np.nan]
        # The ID of this Stats instance.
        self.id_ = str(uuid.uuid4())
        self._prev_merge_values = defaultdict(int)

        self._throughput_ema_coeff = throughput_ema_coeff
        self._throughput_stats = None
        if throughput is not False:
            self._throughput_stats = Stats(
                # We have to check for bool here because in Python, bool is a subclass
                # of int.
                init_values=[throughput]
                if (
                    isinstance(throughput, (int, float))
                    and not isinstance(throughput, bool)
                )
                else None,
                reduce="mean",
                ema_coeff=throughput_ema_coeff,
                window=None,
                clear_on_reduce=False,
                throughput=False,
                throughput_ema_coeff=None,
            )
            if init_values is not None:
                self._last_throughput_measure_time = time.perf_counter()
            else:
                self._last_throughput_measure_time = (
                    -1
                )  # Track last push time for throughput calculation

        # The actual, underlying data in this Stats object.
        self.values: Union[List, deque.Deque] = None
        self._set_values(force_list(init_values))

        self._is_tensor = False

        # Track if new values were pushed since last reduce
        if init_values is not None:
            self._has_new_values = True
        else:
            self._has_new_values = False

    def check_value(self, value: Any) -> None:
        # If we have a reduce method, value should always be a scalar
        # If we don't reduce, we can keep track of value as it is
        if self._reduce_method is not None:
            if isinstance(value, np.ndarray) and value.shape == ():
                return
            elif torch and torch.is_tensor(value):
                self._is_tensor = True
                if tuple(value.shape) == ():
                    return
            elif type(value) not in (list, tuple, deque):
                return
            raise ValueError(
                f"Value ({value}) is required to be a scalar when using a reduce "
                "method!"
            )

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
        """
        self.check_value(value)
        # If throughput tracking is enabled, calculate it based on time between pushes
        if self.has_throughput:
            self._recompute_throughput(value)
        # Handle different reduction methods
        if self._window is not None:
            # For windowed operations, append to values and trim if needed
            self.values.append(value)
            if len(self.values) > self._window:
                self.values.popleft()
        else:
            # For non-windowed operations, use _reduced_values
            if len(self.values) == 0:
                self._set_values([value])
            else:
                self.values.append(value)
                _, values = self._reduced_values()
                self._set_values(values)

        # Mark that we have new values
        self._has_new_values = True

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
        if self._has_new_values or (not compile and not self._inf_window):
            reduced_value, reduced_values = self._reduced_values()
            if not compile and not self._inf_window:
                return reduced_values
            if compile and self._reduce_method:
                return reduced_value[0]
            if compile and self._percentiles is not False:
                return compute_percentiles(reduced_values, self._percentiles)
            return reduced_value
        else:
            return_value = self._last_reduced
            if compile:
                # We don't need to check for self._reduce_method or percentiles here
                # because we only store the reduced value if there is a reduce method.
                return_value = return_value[0]
            return return_value

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
        len_before_reduce = len(self)
        if self._has_new_values:
            # Only calculate and update history if there were new values pushed since
            # last reduce
            reduced, reduced_internal_values_list = self._reduced_values()
            # `clear_on_reduce` -> Clear the values list.
            if self._clear_on_reduce:
                self._set_values([])
            else:
                self._set_values(reduced_internal_values_list)
        else:
            reduced_internal_values_list = None
            reduced = self._last_reduced

        reduced = self._numpy_if_necessary(reduced)

        # Shift historic reduced valued by one in our reduce_history.
        if self._reduce_method is not None:
            # It only makes sense to extend the history if we are reducing to a single
            # value. We need to make a copy here because the new_values_list is a
            # reference to the internal values list
            self._last_reduced = force_list(reduced.copy())
        else:
            # If there is a window and no reduce method, we don't want to use the reduce
            # history to return reduced values in other methods
            self._has_new_values = True

        if compile and self._reduce_method is not None:
            assert (
                len(reduced) == 1
            ), f"Reduced values list must contain exactly one value, found {reduced}"
            reduced = reduced[0]

        if not compile and not self._inf_window:
            if reduced_internal_values_list is None:
                _, reduced_internal_values_list = self._reduced_values()
            return_values = self._numpy_if_necessary(
                reduced_internal_values_list
            ).copy()
        elif compile and self._percentiles is not False:
            if reduced_internal_values_list is None:
                _, reduced_internal_values_list = self._reduced_values()
            return_values = compute_percentiles(
                reduced_internal_values_list, self._percentiles
            )
        else:
            return_values = reduced

        if compile:
            return return_values
        else:
            if len_before_reduce == 0:
                # return_values will be be 0 if we reduce a sum over zero elements
                # But we don't want to create such a zero out of nothing for our new
                # Stats object that we return here
                return Stats.similar_to(self)

            return Stats.similar_to(self, init_values=return_values)

    def merge_on_time_axis(self, other: "Stats") -> None:
        """Merges another Stats object's values into this one along the time axis.

        Args:
            other: The other Stats object to merge values from.
        """
        self.values.extend(other.values)

        # Mark that we have new values since we modified the values list
        self._has_new_values = True

    def merge_in_parallel(self, *others: "Stats") -> None:
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
        win = self._window or float("inf")

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

        # Take turns stepping through `self` and `*others` values, thereby moving
        # backwards from last index to beginning and will up the resulting values list.
        # Stop as soon as we reach the window size.
        new_values = []
        tmp_values = []

        if self._percentiles is not False:
            # Use heapq to sort values (assumes that the values are already sorted)
            # and then pick the correct percentiles
            lists_to_merge = [list(self.values), *[list(o.values) for o in others]]
            merged = list(heapq.merge(*lists_to_merge))
            self._set_values(merged)
        else:
            # Loop from index=-1 backward to index=start until our new_values list has
            # at least a len of `win`.
            for i in range(1, max(map(len, stats_to_merge)) + 1):
                # Per index, loop through all involved stats, including `self` and add
                # to `tmp_values`.
                for stats in stats_to_merge:
                    if len(stats) < i:
                        continue
                    tmp_values.append(stats.values[-i])

                # Now reduce across `tmp_values` based on the reduce-settings of this
                # Stats.
                if self._reduce_per_index_on_aggregate:
                    n_values = 1
                else:
                    n_values = len(tmp_values)

                if self._ema_coeff is not None:
                    new_values.extend([np.nanmean(tmp_values)] * n_values)
                elif self._reduce_method is None:
                    new_values.extend(tmp_values)
                elif self._reduce_method == "sum":
                    # We add [sum(tmp_values) / n_values] * n_values to the new values
                    # list instead of tmp_values, because every incoming element should
                    # have the same weight.
                    added_sum = self._reduced_values(values=tmp_values)[0][0]
                    new_values.extend([added_sum / n_values] * n_values)
                    if self.has_throughput:
                        self._recompute_throughput(added_sum)
                else:
                    new_values.extend(
                        self._reduced_values(values=tmp_values)[0] * n_values
                    )

                tmp_values.clear()
                if len(new_values) >= win:
                    new_values = new_values[:win]
                    break

            self._set_values(list(reversed(new_values)))

        # Mark that we have new values since we modified the values list
        self._has_new_values = True

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

    def __repr__(self) -> str:
        win_or_ema = (
            f"; win={self._window}"
            if self._window
            else f"; ema={self._ema_coeff}"
            if self._ema_coeff
            else ""
        )
        return (
            f"Stats({self.peek()}; len={len(self)}; "
            f"reduce={self._reduce_method}{win_or_ema})"
        )

    def __int__(self):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot convert Stats object with reduce method `None` to int because "
                "it can not be reduced to a single value."
            )
        else:
            return int(self.peek())

    def __float__(self):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot convert Stats object with reduce method `None` to float "
                "because it can not be reduced to a single value."
            )
        else:
            return float(self.peek())

    def __eq__(self, other):
        if self._reduce_method is None:
            self._comp_error("__eq__")
        else:
            return float(self) == float(other)

    def __le__(self, other):
        if self._reduce_method is None:
            self._comp_error("__le__")
        else:
            return float(self) <= float(other)

    def __ge__(self, other):
        if self._reduce_method is None:
            self._comp_error("__ge__")
        else:
            return float(self) >= float(other)

    def __lt__(self, other):
        if self._reduce_method is None:
            self._comp_error("__lt__")
        else:
            return float(self) < float(other)

    def __gt__(self, other):
        if self._reduce_method is None:
            self._comp_error("__gt__")
        else:
            return float(self) > float(other)

    def __add__(self, other):
        if self._reduce_method is None:
            self._comp_error("__add__")
        else:
            return float(self) + float(other)

    def __sub__(self, other):
        if self._reduce_method is None:
            self._comp_error("__sub__")
        else:
            return float(self) - float(other)

    def __mul__(self, other):
        if self._reduce_method is None:
            self._comp_error("__mul__")
        else:
            return float(self) * float(other)

    def __format__(self, fmt):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot format Stats object with reduce method `None` because it can "
                "not be reduced to a single value."
            )
        else:
            return f"{float(self):{fmt}}"

    def _comp_error(self, comp):
        raise ValueError(
            f"Cannot {comp} Stats object with reduce method `None` to other "
            "because it can not be reduced to a single value."
        )

    def get_state(self) -> Dict[str, Any]:
        state = {
            # Make sure we don't return any tensors here.
            "values": convert_to_numpy(self.values),
            "reduce": self._reduce_method,
            "percentiles": self._percentiles,
            "reduce_per_index_on_aggregate": self._reduce_per_index_on_aggregate,
            "window": self._window,
            "ema_coeff": self._ema_coeff,
            "clear_on_reduce": self._clear_on_reduce,
            "_last_reduced": self._last_reduced,
            "_is_tensor": self._is_tensor,
        }
        if self._throughput_stats is not None:
            state["throughput_stats"] = self._throughput_stats.get_state()
        return state

    @staticmethod
    def from_state(state: Dict[str, Any]) -> "Stats":
        # If `values` could contain tensors, don't reinstate them (b/c we don't know
        # whether we are on a supported device).
        values = state["values"]
        if "_is_tensor" in state and state["_is_tensor"]:
            values = []

        if "throughput_stats" in state:
            throughput_stats = Stats.from_state(state["throughput_stats"])
            stats = Stats(
                values,
                reduce=state["reduce"],
                percentiles=state.get("percentiles", False),
                reduce_per_index_on_aggregate=state.get(
                    "reduce_per_index_on_aggregate", False
                ),
                window=state["window"],
                ema_coeff=state["ema_coeff"],
                clear_on_reduce=state["clear_on_reduce"],
                throughput=throughput_stats.peek(),
                throughput_ema_coeff=throughput_stats._ema_coeff,
            )
        elif state.get("_throughput", False):
            # Older checkpoints have a _throughput key that is boolean or
            # a float (throughput value). They don't have a throughput_ema_coeff
            # so we use a default of 0.05.
            # TODO(Artur): Remove this after a few Ray releases.
            stats = Stats(
                values,
                reduce=state["reduce"],
                percentiles=state.get("percentiles", False),
                window=state["window"],
                ema_coeff=state["ema_coeff"],
                clear_on_reduce=state["clear_on_reduce"],
                throughput=state["_throughput"],
                throughput_ema_coeff=0.05,
            )
        else:
            stats = Stats(
                values,
                reduce=state["reduce"],
                percentiles=state.get("percentiles", False),
                window=state["window"],
                ema_coeff=state["ema_coeff"],
                clear_on_reduce=state["clear_on_reduce"],
                throughput=False,
                throughput_ema_coeff=None,
            )
        # Compatibility to old checkpoints where a reduce sometimes resulted in a single
        # values instead of a list such that the history would be a list of integers
        # instead of a list of lists.
        if "_hist" in state:
            # TODO(Artur): Remove this after a few Ray releases.
            if not isinstance(state["_hist"][0], list):
                state["_hist"] = list(map(lambda x: [x], state["_hist"]))
            stats._last_reduced = state["_hist"][-1]
        else:
            stats._last_reduced = state.get("_last_reduced", [np.nan])
        return stats

    @staticmethod
    def similar_to(
        other: "Stats",
        init_values: Optional[Any] = None,
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
        stats = Stats(
            init_values=init_values,
            reduce=other._reduce_method,
            percentiles=other._percentiles,
            reduce_per_index_on_aggregate=other._reduce_per_index_on_aggregate,
            window=other._window,
            ema_coeff=other._ema_coeff,
            clear_on_reduce=other._clear_on_reduce,
            throughput=other._throughput_stats.peek()
            if other.has_throughput
            else False,
            throughput_ema_coeff=other._throughput_ema_coeff,
        )
        stats.id_ = other.id_
        stats._last_reduced = other._last_reduced
        return stats

    def _set_values(self, new_values):
        # For stats with window, use a deque with maxlen=window.
        # This way, we never store more values than absolutely necessary.
        if not self._inf_window:
            self.values = deque(new_values, maxlen=self._window)
        # For infinite windows, use `new_values` as-is (a list).
        else:
            self.values = new_values

        self._has_new_values = True

    def _reduced_values(self, values=None) -> Tuple[Any, Any]:
        """Runs a non-committed reduction procedure on given values (or `self.values`).

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

        # No reduction method. Return list as-is OR reduce list to len=window.
        if self._reduce_method is None:
            if self._percentiles is not False:
                # Sort values
                values = list(values)
                # (Artur): Numpy can sort faster than Python's built-in sort for large lists. Howoever, if we convert to an array here
                # and then sort, this only slightly (<2x) improved the runtime of this method, even for an internal values list of 1M values.
                values.sort()
            return values, values

        # Special case: Internal values list is empty -> return NaN or 0.0 for sum.
        elif len(values) == 0:
            if self._reduce_method in ["min", "max", "mean"] or self._has_returned_zero:
                # We also return np.nan if we have returned zero before.
                # This helps with cases where stats are cleared on reduce, but we don't want to log 0's, except for the first time.
                return [np.nan], []
            else:
                return [0], []

        # Do EMA (always a "mean" reduction; possibly using a window).
        elif self._ema_coeff is not None:
            # Perform EMA reduction over all values in internal values list.
            mean_value = values[0]
            for v in values[1:]:
                mean_value = self._ema_coeff * v + (1.0 - self._ema_coeff) * mean_value
            if self._inf_window:
                return [mean_value], [mean_value]
            else:
                return [mean_value], values
        # Non-EMA reduction (possibly using a window).
        else:
            # Use the numpy/torch "nan"-prefix to ignore NaN's in our value lists.
            if torch and torch.is_tensor(values[0]):
                self._is_tensor = True
                # Only one item in the
                if len(values[0].shape) == 0:
                    reduced = values[0]
                else:
                    reduce_meth = getattr(torch, "nan" + self._reduce_method)
                    reduce_in = torch.stack(list(values))
                    if self._reduce_method == "mean":
                        reduce_in = reduce_in.float()
                    reduced = reduce_meth(reduce_in)
            else:
                reduce_meth = getattr(np, "nan" + self._reduce_method)

                if np.all(np.isnan(values)):
                    # This avoids warnings for taking a mean of an empty array.
                    reduced = np.nan
                else:
                    reduced = reduce_meth(values)

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

            # For window=None|inf (infinite window) and reduce != mean, we don't have to
            # keep any values, except the last (reduced) one.
            if self._inf_window and self._reduce_method != "mean":
                # TODO (sven): What if values are torch tensors? In this case, we
                #  would have to do reduction using `torch` above (not numpy) and only
                #  then return the python primitive AND put the reduced new torch
                #  tensor in the new `self.values`.
                return [reduced], [reduced]
            else:
                # In all other cases, keep the values that were also used for the reduce
                # operation.
                return [reduced], values


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
def merge_stats(base_stats: Optional[Stats], incoming_stats: List[Stats]) -> Stats:
    """Merges Stats objects.

    If `base_stats` is None, we use the first incoming Stats object as the new base Stats object.
    If `base_stats` is not None, we merge all incoming Stats objects into the base Stats object.

    Args:
        base_stats: The base Stats object to merge into.
        incoming_stats: The list of Stats objects to merge.

    Returns:
        The merged Stats object.
    """
    if base_stats is None:
        new_root_stats = True
    else:
        new_root_stats = False
        # Nothing to be merged
        if len(incoming_stats) == 0:
            return base_stats

    if new_root_stats:
        # We need to deepcopy here first because stats from incoming_stats may be altered in the future
        base_stats = copy.deepcopy(incoming_stats[0])
        base_stats.clear_throughput()
        # Note that we may take a mean of means here, which is not the same as a
        # mean of all values. In the future, we could implement a weighted mean
        # of means here by introducing a new Stats object that counts samples
        # for each mean Stats object.
        if len(incoming_stats) > 1:
            base_stats.merge_in_parallel(*incoming_stats[1:])
        if (
            base_stats._reduce_method == "sum"
            and base_stats._inf_window
            and base_stats._clear_on_reduce is False
        ):
            for stat in incoming_stats:
                base_stats._prev_merge_values[stat.id_] = stat.peek()

    elif len(incoming_stats) > 0:
        # Special case: `base_stats` is a lifetime sum (reduce=sum,
        # clear_on_reduce=False) -> We subtract the previous value (from 2
        # `reduce()` calls ago) from all to-be-merged stats, so we don't count
        # twice the older sum from before.

        # Also, for the merged, new throughput value, we need to find out what the
        # actual value-delta is between before the last reduce and the current one.

        added_sum = 0.0  # Used in `base_stats._recompute_throughput` if applicable.
        if (
            base_stats._reduce_method == "sum"
            and base_stats._inf_window
            and base_stats._clear_on_reduce is False
        ):
            for stat in incoming_stats:
                # Subtract "lifetime counts" from the Stat's values to not count
                # older "lifetime counts" more than once.
                prev_reduction = base_stats._prev_merge_values[stat.id_]
                new_reduction = stat.peek(compile=True)
                base_stats.values[-1] -= prev_reduction
                # Keep track of how many counts we actually gained (for throughput
                # recomputation).
                added_sum += new_reduction - prev_reduction
                base_stats._prev_merge_values[stat.id_] = new_reduction

        parallel_merged_stat = copy.deepcopy(incoming_stats[0])
        if len(incoming_stats) > 1:
            # There are more than one incoming parallel others -> Merge all of
            # them in parallel (equal importance).
            parallel_merged_stat.merge_in_parallel(*incoming_stats[1:])

        # Merge incoming Stats object into base Stats object on time axis
        # (giving incoming ones priority).
        if base_stats._reduce_method == "mean" and not base_stats._clear_on_reduce:
            # If we don't clear values, values that are not cleared would contribute
            # to the mean multiple times.
            base_stats._set_values(parallel_merged_stat.values.copy())
        else:
            base_stats.merge_on_time_axis(parallel_merged_stat)
            # Keep track of throughput through the sum of added counts.
            if base_stats.has_throughput:
                base_stats._recompute_throughput(added_sum)

    return base_stats
