import logging
import threading

import numpy as np
import tree  # pip install dm_tree

from ray._common.deprecation import Deprecated, deprecation_warning
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.numpy import (
    SMALL_NUMBER,
)  # Assuming SMALL_NUMBER is a small float like 1e-8
from ray.rllib.utils.serialization import _deserialize_ndarray, _serialize_ndarray
from ray.rllib.utils.typing import TensorStructType

logger = logging.getLogger(__name__)


@OldAPIStack
class Filter:
    """Processes input, possibly statefully."""

    def apply_changes(self, other: "Filter", *args, **kwargs) -> None:
        """Updates self with "new state" from other filter."""
        raise NotImplementedError

    def copy(self) -> "Filter":
        """Creates a new object with same state as self.

        Returns:
            A copy of self.
        """
        raise NotImplementedError

    def sync(self, other: "Filter") -> None:
        """Copies all state from other filter to self."""
        raise NotImplementedError

    def reset_buffer(self) -> None:
        """Creates copy of current state and resets accumulated state"""
        raise NotImplementedError

    def as_serializable(self) -> "Filter":
        raise NotImplementedError

    @Deprecated(new="Filter.reset_buffer()", error=True)
    def clear_buffer(self):
        pass


@OldAPIStack
class NoFilter(Filter):
    is_concurrent = True

    def __call__(self, x: TensorStructType, update=True):
        # Process no further if already np.ndarray, dict, or tuple.
        if isinstance(x, (np.ndarray, dict, tuple)):
            return x

        try:
            return np.asarray(x)
        except Exception:
            raise ValueError("Failed to convert to array", x)

    def apply_changes(self, other: "NoFilter", *args, **kwargs) -> None:
        pass

    def copy(self) -> "NoFilter":
        return self

    def sync(self, other: "NoFilter") -> None:
        pass

    def reset_buffer(self) -> None:
        pass

    def as_serializable(self) -> "NoFilter":
        return self


# Based on Welford's algorithm for numerical stability
# http://www.johndcook.com/blog/standard_deviation/ [4]
@OldAPIStack
class RunningStat:
    def __init__(self, shape=()):
        """Initializes a `RunningStat` instance."""
        # Keep always a state and a delta from all attributes. Note,
        # we use the state for filtering and the delta for updates.
        # All deltas will be zero(s) after a state synchronization
        # across different actors.
        self.num_pushes = 0
        self.num_pushes_delta = 0
        # Stores the mean.
        self.mean_array = np.zeros(shape)
        self.mean_delta_array = np.zeros(shape)
        # Stores the sum of squared demeaned observations. Note, this
        # follows Wellington's algorithm.
        self.sum_sq_diff_array = np.zeros(shape)
        self.sum_sq_diff_delta_array = np.zeros(shape)

    def copy(self):
        """Copies a `RunningStat`."""
        # Copy all attributes by creating a new `RunningStat` instance.
        other = RunningStat(self.shape)
        other.num_pushes = self.num_pushes
        other.num_pushes_delta = self.num_pushes_delta
        other.mean_array = np.copy(self.mean_array)
        other.mean_delta_array = np.copy(self.mean_delta_array)
        other.sum_sq_diff_array = np.copy(self.sum_sq_diff_array)
        other.sum_sq_diff_delta_array = np.copy(self.sum_sq_diff_delta_array)
        return other

    def push(self, x):
        """Updates a `RunningStat` instance by a new value.

        Args:
            x: A new value to update mean and sum of squares by. Must have the
                same shape like the mean.

        Raises:
            `ValueError` in case of a shape mismatch.
        """
        x = np.asarray(x)
        if x.shape != self.mean_array.shape:
            raise ValueError(
                "Unexpected input shape {}, expected {}, value = {}".format(
                    x.shape, self.mean_array.shape, x
                )
            )

        # Store old mean for Welford's sum of squares update.
        old_mean = np.copy(self.mean_array)
        self.num_pushes += 1
        # Also increase the delta counter since the last merge.
        self.num_pushes_delta += 1

        if self.num_pushes == 1:
            self.mean_array[...] = x
            self.mean_delta_array[...] = x
            # sum_sq_diff_array remains 0 for the first element
        else:
            # Welford's update for mean
            delta = x - old_mean
            self.mean_array[...] += delta / self.num_pushes
            # Update the mean delta.
            self.mean_delta_array[...] += delta / self.num_pushes

            # Welford's update for sum of squared differences (S)
            # S_k = S_{k-1} + (x_k - M_k)(x_k - M_{k-1}).
            self.sum_sq_diff_array[...] += delta * (x - self.mean_array)
            # Update the mean sum of squares.
            self.sum_sq_diff_delta_array[...] += delta * (x - self.mean_array)

    def update(self, other):
        """Update this `RunningStat` instance by another one.

        Args:
            other: Another `RunningStat` instance whose state should me
                merged with `self`.
        """
        # Make this explicitly for future changes to avoid ever turning `num_pushes` into
        # a float (this was a problem in earlier versions).
        n1_int = self.num_pushes
        # Note, we use only the delta for the updates, this reduces the risk of numerical
        # instabilities significantly.
        n2_int = other.num_pushes_delta
        # For higher precision use float versions of the counters.
        n1_flt = float(self.num_pushes)
        n2_flt = float(other.num_pushes_delta)
        n_flt = n1_flt + n2_flt

        # If none of the two `RunningStat`s has seen values, yet, return.
        if n1_int + n2_int == 0:
            # Avoid divide by zero, which creates nans
            return

        # Numerically stable formula for combining means
        # M_combined = (n1*M1 + n2*M2) / (n1+n2)
        # This is equivalent to M1 + delta * n2 / n
        delta_mean = other.mean_delta_array - self.mean_array
        self.mean_array += delta_mean * n2_flt / n_flt

        # Numerically stable formula for combining sums of squared differences (S)
        # S_combined = S1 + S2 + (n1*n2 / (n1+n2)) * (M1 - M2)^2 [6]
        delta_mean_sq = delta_mean * delta_mean
        self.sum_sq_diff_array += other.sum_sq_diff_delta_array + delta_mean_sq * (
            n1_flt * n2_flt / n_flt
        )

        # Update the counter with the interger versions of the two counters.
        self.num_pushes = n1_int + n2_int

    def __repr__(self):
        """Represents a `RunningStat` instance.

        Note, a `RunningStat` is represented by its mean, its standard deviation
        and the number `n` of values used to compute the two statistics.
        """
        return "(n={}, mean_mean={}, mean_std={})".format(
            self.n, np.mean(self.mean), np.mean(self.std)
        )

    @property
    def n(self):
        """Returns the number of values seen by a `RunningStat` instance."""
        return self.num_pushes

    @property
    def mean(self):
        """Returns the (vector) mean estimate of a `RunningStat` instance."""
        return self.mean_array

    @property
    def var(self):
        """Returns the (unbiased vector) variance estimate of a `RunningStat` instance."""
        # For n=0 or n=1, variance is typically undefined or 0.
        # Returning 0 for n <= 1 is a common convention for running variance.
        if self.num_pushes <= 1:
            return np.zeros_like(self.mean_array).astype(np.float32)
        # Variance = S / (n-1) for sample variance
        return (self.sum_sq_diff_array / (float(self.num_pushes) - 1)).astype(
            np.float32
        )

    @property
    def std(self):
        """Returns the (unbiased vector) std estimate of a `RunningStat` instance.ance."""
        # Ensure variance is non-negative before sqrt
        return np.sqrt(np.maximum(0, self.var))

    @property
    def shape(self):
        """Returns the shape of the `RunningStat` instance."""
        return self.mean_array.shape

    def to_state(self):
        """Returns the pickable state of a `RunningStat` instance."""
        return {
            "num_pushes": self.num_pushes,
            "num_pushes_delta": self.num_pushes_delta,
            "mean_array": _serialize_ndarray(self.mean_array),
            "mean_delta_array": _serialize_ndarray(self.mean_delta_array),
            "sum_sq_diff_array": _serialize_ndarray(self.sum_sq_diff_array),
            "sum_sq_diff_delta_array": _serialize_ndarray(self.sum_sq_diff_delta_array),
        }

    @staticmethod
    def from_state(state):
        """Builds a `RunningStat` instance from a pickable state."""
        # Need to pass shape to constructor for proper initialization
        # Assuming shape can be inferred from mean_array in state
        shape = _deserialize_ndarray(state["mean_array"]).shape
        running_stats = RunningStat(shape)
        running_stats.num_pushes = state["num_pushes"]
        running_stats.num_pushes_delta = state["num_pushes_delta"]
        running_stats.mean_array = _deserialize_ndarray(state["mean_array"])
        running_stats.mean_delta_array = _deserialize_ndarray(state["mean_delta_array"])
        running_stats.sum_sq_diff_array = _deserialize_ndarray(
            state["sum_sq_diff_array"]
        )
        running_stats.sum_sq_diff_delta_array = _deserialize_ndarray(
            state["sum_sq_diff_delta_array"]
        )
        return running_stats


@OldAPIStack
class MeanStdFilter(Filter):
    """Keeps track of a running mean for seen states"""

    is_concurrent = False

    def __init__(self, shape, demean=True, destd=True, clip=10.0):
        self.shape = shape
        # We don't have a preprocessor, if shape is None (Discrete) or
        # flat_shape is Tuple[np.ndarray] or Dict[str, np.ndarray]
        # (complex inputs).
        flat_shape = tree.flatten(self.shape)
        self.no_preprocessor = shape is None or (
            isinstance(self.shape, (dict, tuple))
            and len(flat_shape) > 0
            and isinstance(flat_shape, np.ndarray)
        )
        # If preprocessing (flattening dicts/tuples), make sure shape
        # is an np.ndarray, so we don't confuse it with a complex Tuple
        # space's shape structure (which is a Tuple[np.ndarray, ...]).
        if not self.no_preprocessor:
            self.shape = np.array(self.shape)
        self.demean = demean
        self.destd = destd
        self.clip = clip
        # Running stats.
        self.running_stats = tree.map_structure(lambda s: RunningStat(s), self.shape)

        # In distributed rollouts, each worker sees different states.
        # The buffer is used to keep track of deltas amongst all the
        # observation filters.
        self.buffer = None
        self.reset_buffer()

    def reset_buffer(self) -> None:
        self.buffer = tree.map_structure(lambda s: RunningStat(s), self.shape)

    def apply_changes(
        self, other: "MeanStdFilter", with_buffer: bool = False, *args, **kwargs
    ) -> None:
        """Applies updates from the buffer of another filter.

        Args:
            other: Other filter to apply info from
            with_buffer: Flag for specifying if the buffer should be
                copied from other.

        .. testcode::
            :skipif: True

            a = MeanStdFilter(())
            a(1)
            a(2)
            print([a.running_stats.n, a.running_stats.mean, a.buffer.n])

        .. testoutput::

            [2, 1.5, 2]

        .. testcode::
            :skipif: True

            b = MeanStdFilter(())
            b(10)
            a.apply_changes(b, with_buffer=False)
            print([a.running_stats.n, a.running_stats.mean, a.buffer.n])

        .. testoutput::

            [3, 4.333333333333333, 2]

        .. testcode::
            :skipif: True

            a.apply_changes(b, with_buffer=True)
            print([a.running_stats.n, a.running_stats.mean, a.buffer.n])

        .. testoutput::

            [4, 5.75, 1]
        """
        tree.map_structure(
            lambda rs, other_rs: rs.update(other_rs), self.running_stats, other.buffer
        )
        if with_buffer:
            self.buffer = tree.map_structure(lambda b: b.copy(), other.buffer)

    def copy(self) -> "MeanStdFilter":
        """Returns a copy of `self`."""
        other = MeanStdFilter(self.shape)
        other.sync(self)
        return other

    def as_serializable(self) -> "MeanStdFilter":
        return self.copy()

    def sync(self, other: "MeanStdFilter") -> None:
        """Syncs all fields together from other filter.

        .. testcode::
            :skipif: True

            a = MeanStdFilter(())
            a(1)
            a(2)
            print([a.running_stats.n, a.running_stats.mean, a.buffer.n])

        .. testoutput::

            [2, array(1.5), 2]

        .. testcode::
            :skipif: True

            b = MeanStdFilter(())
            b(10)
            print([b.running_stats.n, b.running_stats.mean, b.buffer.n])

        .. testoutput::

            [1, array(10.0), 1]

        .. testcode::
            :skipif: True

            a.sync(b)
            print([a.running_stats.n, a.running_stats.mean, a.buffer.n])

        .. testoutput::

            [1, array(10.0), 1]
        """
        self.demean = other.demean
        self.destd = other.destd
        self.clip = other.clip
        self.running_stats = tree.map_structure(
            lambda rs: rs.copy(), other.running_stats
        )
        self.buffer = tree.map_structure(lambda b: b.copy(), other.buffer)

    def __call__(self, x: TensorStructType, update: bool = True) -> TensorStructType:
        if self.no_preprocessor:
            x = tree.map_structure(lambda x_: np.asarray(x_), x)
        else:
            x = np.asarray(x)

        def _helper(x, rs, buffer, shape):
            # Discrete|MultiDiscrete spaces -> No normalization.
            if shape is None:
                return x

            # Keep dtype as is througout this filter.
            orig_dtype = x.dtype

            if update:
                if len(x.shape) == len(rs.shape) + 1:
                    # The vectorized case.
                    for i in range(x.shape):
                        rs.push(x[i])
                        buffer.push(x[i])
                else:
                    # The unvectorized case.
                    rs.push(x)
                    buffer.push(x)
            if self.demean:
                x = x - rs.mean
            if self.destd:
                x = x / (rs.std + SMALL_NUMBER)
            if self.clip:
                x = np.clip(x, -self.clip, self.clip)
            return x.astype(orig_dtype)

        if self.no_preprocessor:
            return tree.map_structure_up_to(
                x, _helper, x, self.running_stats, self.buffer, self.shape
            )
        else:
            return _helper(x, self.running_stats, self.buffer, self.shape)


@OldAPIStack
class ConcurrentMeanStdFilter(MeanStdFilter):
    is_concurrent = True

    def __init__(self, *args, **kwargs):
        super(ConcurrentMeanStdFilter, self).__init__(*args, **kwargs)
        deprecation_warning(
            old="ConcurrentMeanStdFilter",
            error=False,
            help="ConcurrentMeanStd filters are only used for testing and will "
            "therefore be deprecated in the course of moving to the "
            "Connetors API, where testing of filters will be done by other "
            "means.",
        )

        self._lock = threading.RLock()

        def lock_wrap(func):
            def wrapper(*args, **kwargs):
                with self._lock:
                    return func(*args, **kwargs)

            return wrapper

        self.__getattribute__ = lock_wrap(self.__getattribute__)

    def as_serializable(self) -> "MeanStdFilter":
        """Returns non-concurrent version of current class"""
        other = MeanStdFilter(self.shape)
        other.sync(self)
        return other

    def copy(self) -> "ConcurrentMeanStdFilter":
        """Returns a copy of Filter."""
        other = ConcurrentMeanStdFilter(self.shape)
        other.sync(self)
        return other

    def __repr__(self) -> str:
        return "ConcurrentMeanStdFilter({}, {}, {}, {}, {}, {})".format(
            self.shape,
            self.demean,
            self.destd,
            self.clip,
            self.running_stats,
            self.buffer,
        )


@OldAPIStack
def get_filter(filter_config, shape):
    if filter_config == "MeanStdFilter":
        return MeanStdFilter(shape, clip=None)
    elif filter_config == "ConcurrentMeanStdFilter":
        return ConcurrentMeanStdFilter(shape, clip=None)
    elif filter_config == "NoFilter":
        return NoFilter()
    elif callable(filter_config):
        return filter_config(shape)
    else:
        raise Exception("Unknown observation_filter: " + str(filter_config))
