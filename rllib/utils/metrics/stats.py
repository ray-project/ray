import random
import time
from typing import Any, Callable, Dict, Optional, Tuple

import numpy as np

from ray.rllib.utils import force_list


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
        # So far, we have stored all values (1, 2, and 3).
        check(stats.values, [1, 2, 3])
        # Let's call the `reduce()` method to actually reduce these values
        # to a single item of value=6:
        stats = stats.reduce()
        check(stats.peek(), 6)
        check(stats.values, [6])

        # "min" and "max" work analogous to "sum". But let's try with a `window` now:
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
        # So far, we have stored all values (2, 3, 1, and -1).
        check(stats.values, [2, 3, 1, -1])
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
        # We have not reduced yet (all values are still stored):
        check(stats.values, [-5, -4, -3, -2])
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
        check(len(stats.values), 2)  # still both deltas in the values list
        stats = stats.reduce()
        check(len(stats.values), 1)  # got reduced to one value (the sum)
        assert 2.2 > stats.values[0] > 1.8
    """

    def __init__(
        self,
        init_value: Optional[Any] = None,
        reduce: Optional[str] = "mean",
        window: Optional[int] = None,
        ema_coeff: Optional[float] = None,
        reset_on_reduce: bool = False,
        on_exit: Optional[Callable] = None,
    ):
        """Initializes a Stats instance.

        Args:
            init_value: Optional initial value to be placed into `self.values`. If None,
                `self.values` will start empty.
            reduce: The name of the reduce method to be used. Allowed are "mean", "min",
                "max", and "sum". Use None to apply no reduction method (leave
                `self.values` as-is when reducing, except for shortening it to
                `window`). Note that if both `reduce` and `window` are None, the user of
                this Stats object needs to apply some caution over the values list not
                growing infinitely.
            window: An optional window size to reduce over.
                If `window` is not None, then the reduction operation is only applied to
                the most recent `windows` items, and - after reduction - the values list
                is shortened to hold at most `window` items (the most recent ones).
                Must be None if `ema_coeff` is not None.
                If `window` is None (and `ema_coeff` is None), reduction must not be
                "mean".
                TODO (sven): Allow window=float("inf"), iff reset_on_reduce=True.
                This would enable cases where we want to accumulate n data points (w/o
                limitation, then average over these, then reset the data pool on reduce,
                e.g. for evaluation env_runner stats, which should NOT use any window,
                just like in the old API stack).
            ema_coeff: An optional EMA coefficient to use if reduce is "mean"
                and no `window` is provided. Note that if both `window` and `ema_coeff`
                are provided, an error is thrown. Also, if `ema_coeff` is provided,
                `reduce` must be "mean".
                The reduction formula for EMA performed by Stats is:
                EMA(t1) = (1.0 - ema_coeff) * EMA(t0) + ema_coeff * new_value
            reset_on_reduce: If True, the Stats object will reset its entire values list
                to an empty one after `self.reduce()` is called. However, it will then
                return from the `self.reduce()` call a new Stats object with the
                properly reduced (not completely emptied) new values. Setting this
                to True is useful for cases, in which the internal values list would
                otherwise grow indefinitely, for example if reduce is None and there
                is no `window` provided.
        """
        # Thus far, we only support mean, max, min, and sum.
        assert reduce in [None, "mean", "min", "max", "sum"]
        # One or both window and ema_coeff must be None.
        assert (
            window is None or ema_coeff is None
        ), "Only one of `window` or `ema_coeff` can be specified!"
        if ema_coeff is not None:
            assert (
                reduce == "mean"
            ), "`ema_coeff` arg only allowed (not None) when `reduce=mean`!"

        # If reduce=mean AND window=ema_coeff=None, we use EMA by default with a coeff
        # of 0.01 (we do NOT support infinite window sizes for mean as that would mean
        # to keep data in the cache forever).
        if reduce == "mean" and window is None and ema_coeff is None:
            ema_coeff = 0.01

        # The actual data in this Stats object.
        self.values = force_list(init_value)

        self._reduce_method = reduce
        self._window = window
        self._ema_coeff = ema_coeff

        # Timing functionality.
        self._start_time = None

        # Simply store ths flag for the user of this class.
        self._reset_on_reduce = reset_on_reduce

        # Code to execute when exiting a with-context.
        self._on_exit = on_exit

    def push(self, value) -> None:
        """Appends a new value into the internal values list.

        Args:
            value: The value item to be appended to the internal values list
                (`self.values`).
        """
        self.values.append(value)

    def __enter__(self) -> "Stats":
        """Called when entering a context (with which users can measure a time delta).

        Returns:
            This Stats instance (self).
        """
        assert self._start_time is None, "Concurrent updates not supported!"
        self._start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        """Called when exiting a context (with which users can measure a time delta)."""
        assert self._start_time is not None
        time_delta = time.time() - self._start_time
        self.push(time_delta)

        # Call the on_exit handler.
        if self._on_exit:
            self._on_exit(time_delta)

        self._start_time = None

    def peek(self) -> Any:
        """Returns the result of reducing the internal values list.

        Note that this method does NOT alter the internal values list in this process.
        Thus, users can call this method to get an accurate look at the reduced value
        given the current internal values list.

        Returns:
            The result of reducing the internal values list.
        """
        return self._reduced_values()[0]

    def reduce(self) -> "Stats":
        """Reduces the internal values list according to the constructor settings.

        Thereby, the internal values list is changed (note that this is different from
        `peek()`, where the internal list is NOT changed). See the docstring of this
        class for details on the reduction logic applied to the values list, based on
        the constructor settings, such as `window`, `reduce`, etc..

        Returns:
            Returns `self` (now reduced) if self._reduced_values is False.
            Returns a new `Stats` object with an empty internal values list, but
            otherwise the same constructor settings (window, reduce, etc..) as `self`.
        """
        # Reduce everything to a single (init) value.
        self.values = self._reduced_values()[1]
        # `reset_on_reduce` -> Return an empty new Stats object with the same option as
        # `self`.
        if self._reset_on_reduce:
            return Stats.similar_to(self)
        # No reset required upon `reduce()` -> Return `self`.
        else:
            return self

    def merge(self, *others: "Stats", shuffle: bool = True) -> None:
        """Merges all internal values of `others` into `self`'s internal values list.

        Args:
            others: One or more other Stats objects that need to be merged into `self.
            shuffle: Whether to shuffle the merged internal values list after the
                merging (extending). Set to True, if `self` and `*others` are all equal
                components in a parallel setup (each of their values should
                matter equally and without any time-axis bias). Set to False, if
                `*others` is only one component AND its values should be given priority
                (because they are newer).
        """
        # Make sure `other` has same reduction settings.
        assert all(self._reduce_method == o._reduce_method for o in others)
        assert all(self._window == o._window for o in others)
        assert all(self._ema_coeff == o._ema_coeff for o in others)
        # No reduction, combine self's and other's values.
        if self._reduce_method is None:
            for o in others:
                self.values.extend(o.values)
        # Combine values, then maybe shuffle to not give the values of `self` OR `other`
        # any specific weight (over the other).
        elif self._ema_coeff is not None:
            for o in others:
                self.values.extend(o.values)
            if shuffle:
                random.shuffle(self.values)
        # If we have to reduce by a window:
        # Slice self's and other's values using window, combine them, then maybe shuffle
        # values (to make sure none gets a specific weight over the other when it
        # comes to the actual reduction step).
        else:
            # Slice, then merge.
            if self._window is not None:
                self.values = self.values[-self._window :]
                for o in others:
                    self.values += o.values[-self._window :]
            # Merge all.
            else:
                for o in others:
                    self.values.extend(o.values)
            if shuffle:
                random.shuffle(self.values)

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
        return int(self.peek())

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

    def get_state(self) -> Dict[str, Any]:
        return {
            "values": self.values,
            "reduce": self._reduce_method,
            "window": self._window,
            "ema_coeff": self._ema_coeff,
            "reset_on_reduce": self._reset_on_reduce,
        }

    @staticmethod
    def from_state(state: Dict[str, Any]) -> "Stats":
        return Stats(
            state["values"],
            reduce=state["reduce"],
            window=state["window"],
            ema_coeff=state["ema_coeff"],
            reset_on_reduce=state["reset_on_reduce"],
        )

    @staticmethod
    def similar_to(other: "Stats", init_value: Optional[Any] = None):
        return Stats(
            init_value=init_value,
            reduce=other._reduce_method,
            window=other._window,
            ema_coeff=other._ema_coeff,
            reset_on_reduce=other._reset_on_reduce,
        )

    def _reduced_values(self) -> Tuple[Any, Any]:
        """Runs a non-commited reduction procedure on the internal values list.

        Returns:
            A tuple containing 1) the reduced value and 2) the new internal values list
            to be used.
        """
        # No reduction method. Return list as-is OR reduce list to len=window.
        if self._reduce_method is None:
            # No window -> return all internal values.
            if self._window is None:
                return self.values, self.values
            # Window -> return shortened internal values list.
            else:
                return self.values[-self._window :], self.values[-self._window :]
        # Do EMA (always a "mean" reduction).
        elif self._ema_coeff is not None:
            # Special case: Internal values list is empty -> return NaN.
            if len(self.values) == 0:
                return float("nan"), []

            # Perform EMA reduction over all values in internal values list.
            mean_value = self.values[0]
            for v in self.values[1:]:
                mean_value = self._ema_coeff * v + (1.0 - self._ema_coeff) * mean_value
            return mean_value, [mean_value]
        # Do non-EMA reduction (possibly using a window).
        else:
            reduce_meth = getattr(np, self._reduce_method)
            values = (
                self.values if self._window is None else self.values[-self._window :]
            )
            reduced = reduce_meth(values)
            # Convert from numpy to primitive python types.
            if reduced.shape == ():
                if reduced.dtype in [np.int32, np.int64, np.int8, np.int16]:
                    reduced = int(reduced)
                else:
                    reduced = float(reduced)

            # For window=None (infinite window) and reduce != mean, we don't have to
            # keep any values, except the last (reduced) one.
            if self._window is None and self._reduce_method != "mean":
                new_values = [reduced]
            # In all other cases, keep the values that were also used for the reduce
            # operation.
            else:
                new_values = values
            return reduced, new_values
