from collections import defaultdict, deque
import time
import threading
from typing import Any, Dict, List, Tuple, Union

import numpy as np

from ray.rllib.utils import force_list
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.util.annotations import DeveloperAPI

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


@DeveloperAPI
class Stats:
    """A container class holding a number of values and executing reductions over them.

    The individual values in a Stats object may be of any type, for example python int
    or float, numpy arrays, or more complex structured (tuple, dict) and are stored in
    a list under `self.values`.

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

    .. testcode::

        import time
        from ray.rllib.utils.metrics.stats import Stats
        from ray.rllib.utils.test_utils import check

        # By default, we reduce using EMA (with default coeff=0.01).
        stats = Stats()  # use `ema_coeff` arg to change the coeff
        stats.push(1.0)
        stats.push(2.0)
        # EMA formula used by Stats: t1 = (1.0 - ema_coeff) * t0 + ema_coeff * new_val
        check(stats.peek(), 1.0 * (1.0 - 0.01) + 2.0 * 0.01)

        # Here, we use a window over which to mean.
        stats = Stats(window=2)
        stats.push(1.0)
        stats.push(2.0)
        stats.push(3.0)
        # Only mean over the last 2 items.
        check(stats.peek(), 2.5)

        # Here, we sum over the lifetime of the Stats object.
        stats = Stats(reduce="sum")
        stats.push(1)
        check(stats.peek(), 1)
        stats.push(2)
        check(stats.peek(), 3)
        stats.push(3)
        check(stats.peek(), 6)
        # For efficiency, we keep the internal values in a reduced state for all Stats
        # with c'tor options reduce!=mean and infinite window.
        check(stats.values, [6])

        # "min" or "max" work analogous to "sum".
        stats = Stats(reduce="min")
        stats.push(10)
        check(stats.peek(), 10)
        stats.push(20)
        check(stats.peek(), 10)
        stats.push(5)
        check(stats.peek(), 5)
        stats.push(100)
        check(stats.peek(), 5)
        # For efficiency, we keep the internal values in a reduced state for all Stats
        # with c'tor options reduce!=mean and infinite window.
        check(stats.values, [5])

        # Let's try min/max/sum with a `window` now:
        stats = Stats(reduce="max", window=2)
        stats.push(2)
        check(stats.peek(), 2)
        stats.push(3)
        check(stats.peek(), 3)
        stats.push(1)
        check(stats.peek(), 3)
        # However, when we push another value, the max thus-far (3) will go
        # out of scope:
        stats.push(-1)
        check(stats.peek(), 1)  # now, 1 is the max
        # So far, we have stored the most recent 2 values (1 and -1).
        check(stats.values, [1, -1])
        # Let's call the `reduce()` method to actually reduce these values
        # to a list of the most recent 2 (window size) values:
        stats = stats.reduce()
        check(stats.peek(), 1)
        check(stats.values, [1, -1])

        # We can also choose to not reduce at all (reduce=None).
        # With a `window` given, Stats will simply keep (and return) the last
        # `window` items in the values list.
        # Note that we have to explicitly set reduce to None (b/c default is "mean").
        stats = Stats(reduce=None, window=3)
        stats.push(-5)
        stats.push(-4)
        stats.push(-3)
        stats.push(-2)
        check(stats.peek(), [-4, -3, -2])  # `window` (3) most recent values
        # We have not reduced yet (3 values are stored):
        check(stats.values, [-4, -3, -2])
        # Let's reduce:
        stats = stats.reduce()
        check(stats.peek(), [-4, -3, -2])
        # Values are now shortened to contain only the most recent `window` items.
        check(stats.values, [-4, -3, -2])

        # We can even use Stats to time stuff. Here we sum up 2 time deltas,
        # measured using a convenient with-block:
        stats = Stats(reduce="sum")
        check(len(stats.values), 0)
        # First delta measurement:
        with stats:
            time.sleep(1.0)
        check(len(stats.values), 1)
        assert 1.1 > stats.peek() > 0.9
        # Second delta measurement:
        with stats:
            time.sleep(1.0)
        assert 2.2 > stats.peek() > 1.8
        # When calling `reduce()`, the internal values list gets cleaned up.
        check(len(stats.values), 1)  # holds the sum of both deltas in the values list
        stats = stats.reduce()
        check(len(stats.values), 1)  # nothing changed (still one sum value)
        assert 2.2 > stats.values[0] > 1.8
    """

    def __init__(
        self,
        init_values,
        reduce,
        window,
        ema_coeff,
        clear_on_reduce,
        throughput,
        throughput_ema_coeff,
    ):
        """Initializes a Stats instance.

        Args:
            init_values: Initial values to be placed into `self.values`.
            reduce: The name of the reduce method to be used. Allowed are "mean", "min",
                "max", and "sum". Use None to apply no reduction method (leave
                `self.values` as-is when reducing, except for shortening it to
                `window`). Note that if both `reduce` and `window` are None, the user of
                this Stats object needs to apply some caution over the values list not
                growing infinitely.
            window: Window size to reduce over.
                If `window` is not None, then the reduction operation is only applied to
                the most recent `windows` items, and - after reduction - the values list
                is shortened to hold at most `window` items (the most recent ones).
                Must be None if `ema_coeff` is not None.
                If `window` is None (and `ema_coeff` is None), reduction must not be
                "mean".
                TODO (sven): Allow window=float("inf"), iff clear_on_reduce=True.
                This would enable cases where we want to accumulate n data points (w/o
                limitation, then average over these, then reset the data pool on reduce,
                e.g. for evaluation env_runner stats, which should NOT use any window,
                just like in the old API stack).
            ema_coeff: EMA coefficient to use if reduce is "mean"
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
            throughput_ema_coeff: The EMA coefficient to use for throughput tracking.
                Only used if throughput=True.
        """
        # Thus far, we only support mean, max, min, and sum.
        if reduce not in [None, "mean", "min", "max", "sum"]:
            raise ValueError("`reduce` must be one of `mean|min|max|sum` or None!")
        # One or both window and ema_coeff must be None.
        if window is not None and ema_coeff is not None:
            raise ValueError("Only one of `window` or `ema_coeff` can be specified!")
        # If `ema_coeff` is provided, `reduce` must be "mean".
        if ema_coeff is not None and reduce != "mean":
            raise ValueError(
                "`ema_coeff` arg only allowed (not None) when `reduce=mean`!"
            )
        # If `window` is explicitly set to inf, `clear_on_reduce` must be True.
        # Otherwise, we risk a memory leak.
        if window == float("inf") and not clear_on_reduce:
            raise ValueError(
                "When using an infinite window (float('inf'), `clear_on_reduce` must "
                "be set to True!"
            )

        # If reduce=mean AND window=ema_coeff=None, we use EMA by default with a coeff
        # of 0.01 (we do NOT support infinite window sizes for mean as that would mean
        # to keep data in the cache forever).
        if reduce == "mean" and window is None and ema_coeff is None:
            ema_coeff = 0.01

        self._reduce_method = reduce
        self._window = window
        self._inf_window = self._window in [None, float("inf")]
        self._ema_coeff = ema_coeff

        # Timing functionality (keep start times per thread).
        self._start_times = defaultdict(lambda: None)

        # Simply store ths flag for the user of this class.
        self._clear_on_reduce = clear_on_reduce

        # On each `.reduce()` call, we store the result of this call in reduce_history[0] and the
        # previous `reduce()` result in reduce_history[1].
        self._reduce_history = deque([[np.nan], [np.nan], [np.nan]], maxlen=3)

        self._throughput_ema_coeff = throughput_ema_coeff
        self._throughput_stats = None
        if throughput is not False:
            if self._reduce_method != "sum" or not self._inf_window:
                raise ValueError(
                    "Can't track throughput for a Stats that a) doesn't have "
                    "reduce='sum' and/or b) has a finite window! Set `Stats("
                    "reduce='sum', window=None)`."
                )
            self._throughput_stats = Stats(
                # We have to check for bool here because in Python, bool is a subclass of int
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
                self._last_push_time = time.perf_counter()
            else:
                self._last_push_time = (
                    -1
                )  # Track last push time for throughput calculation

        # The actual, underlying data in this Stats object.
        self.values: List = None
        self._set_values(force_list(init_values))

        # Track if new values were pushed since last reduce
        if init_values is not None:
            self._has_new_values = True
        else:
            self._has_new_values = False

    def push(self, value: Any) -> None:
        """Pushes a value into this Stats object.

        Args:
            value: The value to be pushed. Can be of any type.
        """
        current_time = time.time()

        # If throughput tracking is enabled, calculate it based on time between pushes
        if self._throughput_stats is not None:
            if self._last_push_time >= 0:
                time_diff = current_time - self._last_push_time
                if time_diff > 0:  # Avoid division by zero
                    current_throughput = value / time_diff
                    self._throughput_stats.push(current_throughput)
            self._last_push_time = current_time

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
        Thus, users can call this method to get an accurate look at the reduced values
        given the current internal values list.

        Args:
            compile: If True, the result is compiled into a single value if possible.

        Returns:
            The result of reducing the internal values list.
        """
        if self._has_new_values:
            ret = self._reduced_values()[0]
        else:
            ret = self._reduce_history[-1]

        if compile and self._reduce_method:
            return ret[0]
        else:
            return ret

    def get_reduce_history(self) -> List[Any]:
        """Returns the history of reduced values as a list.

        The history contains the most recent reduced values, with the most recent value
        at the end of the list. The length of the history is limited by the maxlen of
        the internal history deque.

        Returns:
            A list containing the history of reduced values.
        """
        # Make a 1 level deep copy of the reduce history to avoid mutating the original reduce history's elements
        return [sublist.copy() for sublist in list(self._reduce_history)]

    @property
    def throughput(self) -> float:
        """Returns the current throughput estimate per second.

        Raises:
            ValueError: If throughput tracking is not enabled for this Stats object.
        """
        if self._throughput_stats is None:
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

        Returns:
            The reduced value (can be of any type, depending on the input values and
            reduction method).
        """
        if self._has_new_values:
            # Only calculate and update history if there were new values pushed since last reduce
            reduced, _ = self._reduced_values()
            # `clear_on_reduce` -> Clear the values list.
            if self._clear_on_reduce:
                self._set_values([])
                # If we clear on reduce, following reduce calls should not return the old values.
                self._has_new_values = True
            else:
                self._has_new_values = False
                self._set_values(reduced)

            # Shift historic reduced valued by one in our reduce_history-tuple.
            if self._reduce_method is not None:
                # It only makes sense to extend the history if we are reducing to a single value, otherwise we duplicate values into the history and the history can blow up.
                # We need to extend it with a copy because reduced values are (possibly) references to the internal values list and will be mutated by following reduce calls.
                self._reduce_history.append(reduced.copy())

            ret = reduced
        else:
            ret = self._reduce_history[-1]

        if compile and self._reduce_method is not None:
            return ret[0]
        else:
            return ret

    def merge_on_time_axis(self, other: "Stats") -> None:
        """Merges another Stats object's values into this one along the time axis.

        Args:
            other: The other Stats object to merge values from.
        """
        # Make sure `others` have same reduction settings.
        assert self._reduce_method == other._reduce_method, (
            self._reduce_method,
            other._reduce_method,
        )
        assert self._ema_coeff == other._ema_coeff, (self._ema_coeff, other._ema_coeff)
        if self._window != other._window:
            self._window = other._window

        # Extend `self`'s values by `other`'s.
        self.values.extend(other.values)

        # Adopt `other`'s current throughput estimate (it's the newer one).
        if self._throughput_stats is not None:
            self._throughput_stats.merge_on_time_axis(other._throughput_stats)

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

        .. testcode::
            from ray.rllib.utils.metrics.stats import Stats
            from ray.rllib.utils.test_utils import check

            # Parallel-merge two (reduce=mean) stats with window=3.
            stats = Stats(reduce="mean", window=3)
            stats1 = Stats(reduce="mean", window=3)
            stats1.push(0)
            stats1.push(1)
            stats1.push(2)
            stats1.push(3)
            stats2 = Stats(reduce="mean", window=3)
            stats2.push(4000)
            stats2.push(4)
            stats2.push(5)
            stats2.push(6)
            stats.merge_in_parallel(stats1, stats2)
            # Fill new merged-values list:
            # - Start with index -1, move towards the start.
            # - Thereby always reduce across the different Stats objects' at the
            #   current index.
            # - The resulting reduced value (across Stats at current index) is then
            #   repeated AND added to the new merged-values list n times (where n is
            #   the number of Stats, across which we merge).
            # - The merged-values list is reversed.
            # Here:
            # index -1: [3, 6] -> [4.5, 4.5]
            # index -2: [2, 5] -> [4.5, 4.5, 3.5, 3.5]
            # STOP after merged list contains >= 3 items (window size)
            # reverse: [3.5, 3.5, 4.5, 4.5]
            # deque w/ maxlen=3: [3.5, 4.5, 4.5]
            check(stats.values, [3.5, 4.5, 4.5])
            check(stats.peek(), (3.5 + 4.5 + 4.5) / 3)  # mean the 3 items (window)

            # Parallel-merge two (reduce=max) stats with window=3.
            stats = Stats(reduce="max", window=3)
            stats1 = Stats(reduce="max", window=3)
            stats1.push(1)
            stats1.push(2)
            stats1.push(3)
            stats2 = Stats(reduce="max", window=3)
            stats2.push(4)
            stats2.push(5)
            stats2.push(6)
            stats.merge_in_parallel(stats1, stats2)
            # Same here: Fill new merged-values list:
            # - Start with index -1, moving to the start.
            # - Thereby always reduce across the different Stats objects' at the
            #   current index.
            # - The resulting reduced value (across Stats at current index) is then
            #   repeated AND added to the new merged-values list n times (where n is the
            #   number of Stats, across which we merge).
            # - The merged-values list is reversed.
            # Here:
            # index -1: [3, 6] -> [6, 6]
            # index -2: [2, 5] -> [6, 6, 5, 5]
            # STOP after merged list contains >= 3 items (window size)
            # reverse: [5, 5, 6, 6]
            # deque w/ maxlen=3: [5, 6, 6]
            check(stats.values, [5, 6, 6])
            check(stats.peek(), 6)  # max is 6

            # Parallel-merge two (reduce=min) stats with window=4.
            stats = Stats(reduce="min", window=4)
            stats1 = Stats(reduce="min", window=4)
            stats1.push(1)
            stats1.push(2)
            stats1.push(1)
            stats1.push(4)
            stats2 = Stats(reduce="min", window=4)
            stats2.push(5)
            stats2.push(0.5)
            stats2.push(7)
            stats2.push(8)
            stats.merge_in_parallel(stats1, stats2)
            # Same procedure:
            # index -1: [4, 8] -> [4, 4]
            # index -2: [1, 7] -> [4, 4, 1, 1]
            # STOP after merged list contains >= 4 items (window size)
            # reverse: [1, 1, 4, 4]
            check(stats.values, [1, 1, 4, 4])
            check(stats.peek(), 1)  # min is 1

            # Parallel-merge two (reduce=sum) stats with no window.
            # Note that when reduce="sum", we do NOT reduce across the indices of the
            # parallel values.
            stats = Stats(reduce="sum")
            stats1 = Stats(reduce="sum")
            stats1.push(1)
            stats1.push(2)
            stats1.push(0)
            stats1.push(3)
            stats2 = Stats(reduce="sum")
            stats2.push(4)
            stats2.push(5)
            stats2.push(6)
            # index -1: [3, 6] -> [3, 6] (no reduction, leave values as-is)
            # index -2: [0, 5] -> [3, 6, 0, 5]
            # index -3: [2, 4] -> [3, 6, 0, 5, 2, 4]
            # index -4: [1] -> [3, 6, 0, 5, 2, 4, 1]
            # reverse: [1, 4, 2, 5, 0, 6, 3]
            stats.merge_in_parallel(stats1, stats2)
            check(stats.values, [15, 6])  # 6 from `stats1` and 15 from `stats2`
            check(stats.peek(), 21)

            # Parallel-merge two "concat" (reduce=None) stats with no window.
            # Note that when reduce=None, we do NOT reduce across the indices of the
            # parallel values.
            stats = Stats(reduce=None, window=float("inf"), clear_on_reduce=True)
            stats1 = Stats(reduce=None, window=float("inf"), clear_on_reduce=True)
            stats1.push(1)
            stats2 = Stats(reduce=None, window=float("inf"), clear_on_reduce=True)
            stats2.push(2)
            # index -1: [1, 2] -> [1, 2] (no reduction, leave values as-is)
            # reverse: [2, 1]
            stats.merge_in_parallel(stats1, stats2)
            check(stats.values, [2, 1])
            check(stats.peek(), [2, 1])

        Args:
            others: One or more other Stats objects that need to be parallely merged
                into `self, meaning with equal weighting as the existing values in
                `self`.
        """
        # Make sure `others` have same reduction settings.
        assert all(
            self._reduce_method == o._reduce_method
            and self._window == o._window
            and self._ema_coeff == o._ema_coeff
            for o in others
        )
        win = self._window or float("inf")

        # Take turns stepping through `self` and `*others` values, thereby moving
        # backwards from last index to beginning and will up the resulting values list.
        # Stop as soon as we reach the window size.
        new_values = []
        tmp_values = []
        # Loop from index=-1 backward to index=start until our new_values list has
        # at least a len of `win`.
        for i in range(1, max(map(len, [self, *others])) + 1):
            # Per index, loop through all involved stats, including `self` and add
            # to `tmp_values`.
            for stats in [self, *others]:
                if len(stats) < i:
                    continue
                tmp_values.append(stats.values[-i])

            # Now reduce across `tmp_values` based on the reduce-settings of this Stats.
            # TODO (sven) : explain why all this
            if self._ema_coeff is not None:
                new_values.extend([np.nanmean(tmp_values)] * len(tmp_values))
            elif self._reduce_method in [None, "sum"]:
                new_values.extend(tmp_values)
            else:
                new_values.extend(
                    [self._reduced_values(values=tmp_values)[0]] * len(tmp_values)
                )
            tmp_values.clear()
            if len(new_values) >= win:
                break

        self._set_values(list(reversed(new_values)))

        # Adopt `other`'s current throughput estimate (it's the newer one).
        if self._throughput_stats is not None:
            for other in others:
                if other._throughput_stats is not None:
                    self._throughput_stats.merge_in_parallel(other._throughput_stats)

        # Mark that we have new values since we modified the values list
        self._has_new_values = True

    def set_to_numpy_values(self, values) -> None:
        """Converts `self.values` from tensors to actual numpy values.

        Args:
            values: The (numpy) values to set `self.values` to.
        """
        numpy_values = convert_to_numpy(values)
        if self._reduce_method is None:
            assert isinstance(values, list) and len(self.values) >= len(values)
            self.values = numpy_values
        else:
            assert len(self.values) > 0
            self._set_values(force_list(numpy_values))

        # Mark that we have new values since we modified the values list
        self._has_new_values = True

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
                "Cannot convert Stats object with reduce method `None` to int because it can not be reduced to a single value."
            )
        else:
            return int(self.peek())

    def __float__(self):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot convert Stats object with reduce method `None` to float because it can not be reduced to a single value."
            )
        else:
            return float(self.peek())

    def __eq__(self, other):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot compare Stats object with reduce method `None` to other because it can not be reduced to a single value."
            )
        else:
            return float(self) == float(other)

    def __le__(self, other):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot compare Stats object with reduce method `None` to other because it can not be reduced to a single value."
            )
        else:
            return float(self) <= float(other)

    def __ge__(self, other):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot compare Stats object with reduce method `None` to other because it can not be reduced to a single value."
            )
        else:
            return float(self) >= float(other)

    def __lt__(self, other):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot compare Stats object with reduce method `None` to other because it can not be reduced to a single value."
            )
        else:
            return float(self) < float(other)

    def __gt__(self, other):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot compare Stats object with reduce method `None` to other because it can not be reduced to a single value."
            )
        else:
            return float(self) > float(other)

    def __add__(self, other):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot add Stats object with reduce method `None` to other because it can not be reduced to a single value."
            )
        else:
            return float(self) + float(other)

    def __sub__(self, other):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot subtract Stats object with reduce method `None` from other because it can not be reduced to a single value."
            )
        else:
            return float(self) - float(other)

    def __mul__(self, other):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot multiply Stats object with reduce method `None` with other because it can not be reduced to a single value."
            )
        else:
            return float(self) * float(other)

    def __format__(self, fmt):
        if self._reduce_method is None:
            raise ValueError(
                "Cannot format Stats object with reduce method `None` because it can not be reduced to a single value."
            )
        else:
            return f"{float(self):{fmt}}"

    def get_state(self) -> Dict[str, Any]:
        state = {
            "values": self.values,
            "reduce": self._reduce_method,
            "window": self._window,
            "ema_coeff": self._ema_coeff,
            "clear_on_reduce": self._clear_on_reduce,
            "_hist": list(self._reduce_history),
        }
        if self._throughput_stats is not None:
            state["throughput_stats"] = self._throughput_stats.get_state()
        return state

    @staticmethod
    def from_state(state: Dict[str, Any], throughputs=False) -> "Stats":
        if "throughput_stats" in state:
            throughput_stats = Stats.from_state(state["throughput_stats"])
            stats = Stats(
                state["values"],
                reduce=state["reduce"],
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
                state["values"],
                reduce=state["reduce"],
                window=state["window"],
                ema_coeff=state["ema_coeff"],
                clear_on_reduce=state["clear_on_reduce"],
                throughput=state["_throughput"],
                throughput_ema_coeff=0.05,
            )
        else:
            stats = Stats(
                state["values"],
                reduce=state["reduce"],
                window=state["window"],
                ema_coeff=state["ema_coeff"],
                clear_on_reduce=state["clear_on_reduce"],
                throughput=False,
                throughput_ema_coeff=None,
            )
        stats._reduce_history = deque(
            state["_hist"], maxlen=stats._reduce_history.maxlen
        )
        return stats

    @staticmethod
    def similar_to(
        other: "Stats",
        init_values: Any = None,
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
            window=other._window,
            ema_coeff=other._ema_coeff,
            clear_on_reduce=other._clear_on_reduce,
            throughput=other._throughput_stats.peek()
            if other._throughput_stats is not None
            else False,
            throughput_ema_coeff=other._throughput_ema_coeff,
        )
        stats._reduce_history = other._reduce_history
        return stats

    def _set_values(self, new_values):
        # For stats with window, use a deque with maxlen=window.
        # This way, we never store more values than absolutely necessary.
        if not self._inf_window:
            self.values = deque(new_values, maxlen=self._window)
        # For infinite windows, use `new_values` as-is (a list).
        else:
            self.values = new_values

    def _reduced_values(self, values=None) -> Tuple[Any, Any]:
        """Runs a non-commited reduction procedure on given values (or `self.values`).

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
            return values, values

        # Special case: Internal values list is empty -> return NaN
        # This makes sure that all metrics are allways logged.
        elif len(values) == 0:
            return [float("nan")], []

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
        # Do non-EMA reduction (possibly using a window).
        else:
            # Use the numpy/torch "nan"-prefix to ignore NaN's in our value lists.
            if torch and torch.is_tensor(values[0]):
                assert all(torch.is_tensor(v) for v in values), values
                # TODO (sven) If the shape is (), do NOT even use the reduce method.
                #  Using `tf.reduce_mean()` here actually lead to a completely broken
                #  DreamerV3 (for a still unknown exact reason).
                if len(values[0].shape) == 0:
                    reduced = values[0]
                else:
                    reduce_meth = getattr(torch, "nan" + self._reduce_method)
                    reduce_in = torch.stack(list(values))
                    if self._reduce_method == "mean":
                        reduce_in = reduce_in.float()
                    reduced = reduce_meth(reduce_in)
            elif tf and tf.is_tensor(values[0]):
                # TODO (sven): Currently, tensor metrics only work with window=1.
                #  We might want o enforce it more formally, b/c it's probably not a
                #  good idea to have MetricsLogger or Stats tinker with the actual
                #  computation graph that users are trying to build in their loss
                #  functions.
                assert len(values) == 1
                # TODO (sven) If the shape is (), do NOT even use the reduce method.
                #  Using `tf.reduce_mean()` here actually lead to a completely broken
                #  DreamerV3 (for a still unknown exact reason).
                if len(values[0].shape) == 0:
                    reduced = values[0]
                else:
                    reduce_meth = getattr(tf, "reduce_" + self._reduce_method)
                    reduced = reduce_meth(values)

            else:
                reduce_meth = getattr(np, "nan" + self._reduce_method)
                try:
                    np.all(np.isnan(values))
                except Exception:
                    import pdb

                    pdb.set_trace()

                if np.all(np.isnan(values)):
                    # This avoids taking a mean of an empty array.
                    reduced = np.nan
                else:
                    reduced = reduce_meth(values)

            def safe_isnan(value):
                if torch and isinstance(value, torch.Tensor):
                    return torch.isnan(value)
                if tf and tf.is_tensor(value):
                    return tf.math.is_nan(value)
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
