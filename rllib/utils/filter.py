import logging
import threading

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.numpy import SMALL_NUMBER
from ray.rllib.utils.typing import TensorStructType
from ray.rllib.utils.serialization import _serialize_ndarray, _deserialize_ndarray
from ray.rllib.utils.deprecation import deprecation_warning

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


# http://www.johndcook.com/blog/standard_deviation/
@OldAPIStack
class RunningStat:
    def __init__(self, shape=()):
        self.num_pushes = 0
        self.mean_array = np.zeros(shape)
        self.std_array = np.zeros(shape)

    def copy(self):
        other = RunningStat()
        # TODO: Remove these safe-guards if not needed anymore.
        other.num_pushes = self.num_pushes if hasattr(self, "num_pushes") else self._n
        other.mean_array = (
            np.copy(self.mean_array)
            if hasattr(self, "mean_array")
            else np.copy(self._M)
        )
        other.std_array = (
            np.copy(self.std_array) if hasattr(self, "std_array") else np.copy(self._S)
        )
        return other

    def push(self, x):
        x = np.asarray(x)
        # Unvectorized update of the running statistics.
        if x.shape != self.mean_array.shape:
            raise ValueError(
                "Unexpected input shape {}, expected {}, value = {}".format(
                    x.shape, self.mean_array.shape, x
                )
            )
        self.num_pushes += 1
        if self.num_pushes == 1:
            self.mean_array[...] = x
        else:
            delta = x - self.mean_array
            self.mean_array[...] += delta / self.num_pushes
            self.std_array[...] += (
                (delta / self.num_pushes) * delta * (self.num_pushes - 1)
            )

    def update(self, other):
        n1 = float(self.num_pushes)
        n2 = float(other.num_pushes)
        n = n1 + n2
        if n == 0:
            # Avoid divide by zero, which creates nans
            return
        delta = self.mean_array - other.mean_array
        delta2 = delta * delta
        m = (n1 * self.mean_array + n2 * other.mean_array) / n
        s = self.std_array + other.std_array + (delta2 / n) * n1 * n2
        self.num_pushes = n
        self.mean_array = m
        self.std_array = s

    def __repr__(self):
        return "(n={}, mean_mean={}, mean_std={})".format(
            self.n, np.mean(self.mean), np.mean(self.std)
        )

    @property
    def n(self):
        return self.num_pushes

    @property
    def mean(self):
        return self.mean_array

    @property
    def var(self):
        return (
            self.std_array / (self.num_pushes - 1)
            if self.num_pushes > 1
            else np.square(self.mean_array)
        ).astype(np.float32)

    @property
    def std(self):
        return np.sqrt(self.var)

    @property
    def shape(self):
        return self.mean_array.shape

    def to_state(self):
        return {
            "num_pushes": self.num_pushes,
            "mean_array": _serialize_ndarray(self.mean_array),
            "std_array": _serialize_ndarray(self.std_array),
        }

    @staticmethod
    def from_state(state):
        running_stats = RunningStat()
        running_stats.num_pushes = state["num_pushes"]
        running_stats.mean_array = _deserialize_ndarray(state["mean_array"])
        running_stats.std_array = _deserialize_ndarray(state["std_array"])
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
            and isinstance(flat_shape[0], np.ndarray)
        )
        # If preprocessing (flattening dicts/tuples), make sure shape
        # is an np.ndarray, so we don't confuse it with a complex Tuple
        # space's shape structure (which is a Tuple[np.ndarray]).
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
                    for i in range(x.shape[0]):
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
