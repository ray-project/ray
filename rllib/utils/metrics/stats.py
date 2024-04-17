import random
import time
from typing import Any, Dict

import numpy as np

from ray.rllib.utils import force_list


class Stats:
    def __init__(
        self,
        init_value=None,
        reduce="mean",
        window=None,
        ema_coeff=None,
    ):
        # Thus far, we only support mean, max, min, and sum.
        assert reduce in [None, "mean", "min", "max", "sum"]
        # One or both window and ema must be None.
        assert window is None or ema_coeff is None, (
            "Only one of `window` or `ema_coeff` can be specified!"
        )
        if ema_coeff is not None:
            assert reduce == "mean", (
                "`ema_coeff` arg only allowed to be not None when `reduce=mean`!"
            )

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

    def push(self, value):
        self.values.append(value)

    def __enter__(self):
        assert self._start_time is None, "Concurrent updates not supported!"
        self._start_time = time.time()

    def __exit__(self, exc_type, exc_value, tb):
        assert self._start_time is not None
        time_delta = time.time() - self._start_time
        self.push(time_delta)
        self._start_time = None

    def peek(self):
        if len(self.values) == 1:
            return self.values[0]
        else:
            return self._reduced_values()[0]

    def reduce(self):
        # Reduce everything to a single (init) value.
        #if self._reduce_method is not None:
        #    self.values = [self._reduced_values()]
        ret, self.values = self._reduced_values()
        return ret

    def merge(self, *others: "Stats"):
        # Make sure `other` has same reduction settings.
        assert all(self._reduce_method is o._reduce_method for o in others)
        assert all(self._window == o._window for o in others)
        assert all(self._ema_coeff == o._ema_coeff for o in others)
        # No reduction, combine self's and other's values.
        if self._reduce_method is None:
            for o in others:
                self.values.extend(o.values)
        # Combine values, then shuffle to not give the values of `self` OR `other` any
        # specific weight (over the other).
        elif self._ema_coeff is not None:
            for o in others:
                self.values.extend(o.values)
            random.shuffle(self.values)
        # If we have to reduce by a window:
        # Slice self's and other's values using window, combine them, then shuffle
        # values (to make sure none gets a specific weight over the other when it
        # comes to the actual reduction step).
        else:
            # Slice, then merge.
            if self._window is not None:
                self.values = self.values[-self._window:]
                for o in others:
                    self.values += o.values[-self._window:]
            # Merge all.
            else:
                for o in others:
                    self.values.extend(o.values)
            random.shuffle(self.values)

    def __len__(self):
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

    def __float__(self):
        return float(self.peek())

    def __int__(self):
        return int(self.peek())

    def get_state(self) -> Dict[str, Any]:
        return {
            "values": self.values,
            "reduce": self._reduce_method,
            "window": self._window,
            "ema_coeff": self._ema_coeff,
        }
    
    @staticmethod
    def from_state(self, state: Dict[str, Any]) -> "Stats":
        return Stats(
            state["values"],
            reduce=state["reduce"],
            window=state["window"],
            ema_coeff=state["ema_coeff"],
        )

    def _reduced_values(self):
        # No reduction. Return list as-is.
        if self._reduce_method is None:
            return self.values, self.values
        # Do EMA (always a "mean" reduction).
        elif self._ema_coeff is not None:
            mean_value = self.values[0]
            for v in self.values[1:]:
                mean_value = self._ema_coeff * v + (1.0 - self._ema_coeff) * mean_value
            return mean_value, [mean_value]
        # Do some other reduction (possibly using a window).
        else:
            reduce_meth = getattr(np, self._reduce_method)
            values = self.values if self._window is None else self.values[-self._window:]
            reduced = reduce_meth(values)

            # For window=None (infinite window) and reduce != mean, we don't have to
            # keep any values, except the last (reduced) one.
            if self._window is None and self._reduce_method != "mean":
                new_values = [reduced]
            # In all other cases, keep the values that were also used for the reduce
            # operation.
            else:
                new_values = values
            return reduced, new_values
