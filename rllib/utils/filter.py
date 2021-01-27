import logging
import numpy as np
import threading

logger = logging.getLogger(__name__)


class Filter:
    """Processes input, possibly statefully."""

    def apply_changes(self, other, *args, **kwargs):
        """Updates self with "new state" from other filter."""
        raise NotImplementedError

    def copy(self):
        """Creates a new object with same state as self.

        Returns:
            A copy of self.
        """
        raise NotImplementedError

    def sync(self, other):
        """Copies all state from other filter to self."""
        raise NotImplementedError

    def clear_buffer(self):
        """Creates copy of current state and clears accumulated state"""
        raise NotImplementedError

    def as_serializable(self):
        raise NotImplementedError


class NoFilter(Filter):
    is_concurrent = True

    def __init__(self, *args):
        pass

    def __call__(self, x, update=True):
        try:
            return np.asarray(x)
        except Exception:
            raise ValueError("Failed to convert to array", x)

    def apply_changes(self, other, *args, **kwargs):
        pass

    def copy(self):
        return self

    def sync(self, other):
        pass

    def clear_buffer(self):
        pass

    def as_serializable(self):
        return self


# http://www.johndcook.com/blog/standard_deviation/
class RunningStat:
    def __init__(self, shape=None):
        self._n = 0
        self._M = np.zeros(shape)
        self._S = np.zeros(shape)

    def copy(self):
        other = RunningStat()
        other._n = self._n
        other._M = np.copy(self._M)
        other._S = np.copy(self._S)
        return other

    def push(self, x):
        x = np.asarray(x)
        # Unvectorized update of the running statistics.
        if x.shape != self._M.shape:
            raise ValueError(
                "Unexpected input shape {}, expected {}, value = {}".format(
                    x.shape, self._M.shape, x))
        n1 = self._n
        self._n += 1
        if self._n == 1:
            self._M[...] = x
        else:
            delta = x - self._M
            self._M[...] += delta / self._n
            self._S[...] += delta * delta * n1 / self._n

    def update(self, other):
        n1 = self._n
        n2 = other._n
        n = n1 + n2
        if n == 0:
            # Avoid divide by zero, which creates nans
            return
        delta = self._M - other._M
        delta2 = delta * delta
        M = (n1 * self._M + n2 * other._M) / n
        S = self._S + other._S + delta2 * n1 * n2 / n
        self._n = n
        self._M = M
        self._S = S

    def __repr__(self):
        return "(n={}, mean_mean={}, mean_std={})".format(
            self.n, np.mean(self.mean), np.mean(self.std))

    @property
    def n(self):
        return self._n

    @property
    def mean(self):
        return self._M

    @property
    def var(self):
        return self._S / (self._n - 1) if self._n > 1 else np.square(self._M)

    @property
    def std(self):
        return np.sqrt(self.var)

    @property
    def shape(self):
        return self._M.shape


class MeanStdFilter(Filter):
    """Keeps track of a running mean for seen states"""
    is_concurrent = False

    def __init__(self, shape, demean=True, destd=True, clip=10.0):
        self.shape = shape
        self.demean = demean
        self.destd = destd
        self.clip = clip
        self.rs = RunningStat(shape)
        # In distributed rollouts, each worker sees different states.
        # The buffer is used to keep track of deltas amongst all the
        # observation filters.

        self.buffer = RunningStat(shape)

    def clear_buffer(self):
        self.buffer = RunningStat(self.shape)

    def apply_changes(self, other, with_buffer=False):
        """Applies updates from the buffer of another filter.

        Params:
            other (MeanStdFilter): Other filter to apply info from
            with_buffer (bool): Flag for specifying if the buffer should be
                copied from other.

        Examples:
            >>> a = MeanStdFilter(())
            >>> a(1)
            >>> a(2)
            >>> print([a.rs.n, a.rs.mean, a.buffer.n])
            [2, 1.5, 2]
            >>> b = MeanStdFilter(())
            >>> b(10)
            >>> a.apply_changes(b, with_buffer=False)
            >>> print([a.rs.n, a.rs.mean, a.buffer.n])
            [3, 4.333333333333333, 2]
            >>> a.apply_changes(b, with_buffer=True)
            >>> print([a.rs.n, a.rs.mean, a.buffer.n])
            [4, 5.75, 1]
        """
        self.rs.update(other.buffer)
        if with_buffer:
            self.buffer = other.buffer.copy()

    def copy(self):
        """Returns a copy of Filter."""
        other = MeanStdFilter(self.shape)
        other.sync(self)
        return other

    def as_serializable(self):
        return self.copy()

    def sync(self, other):
        """Syncs all fields together from other filter.

        Examples:
            >>> a = MeanStdFilter(())
            >>> a(1)
            >>> a(2)
            >>> print([a.rs.n, a.rs.mean, a.buffer.n])
            [2, array(1.5), 2]
            >>> b = MeanStdFilter(())
            >>> b(10)
            >>> print([b.rs.n, b.rs.mean, b.buffer.n])
            [1, array(10.0), 1]
            >>> a.sync(b)
            >>> print([a.rs.n, a.rs.mean, a.buffer.n])
            [1, array(10.0), 1]
        """
        assert other.shape == self.shape, "Shapes don't match!"
        self.demean = other.demean
        self.destd = other.destd
        self.clip = other.clip
        self.rs = other.rs.copy()
        self.buffer = other.buffer.copy()

    def __call__(self, x, update=True):
        x = np.asarray(x)
        if update:
            if len(x.shape) == len(self.rs.shape) + 1:
                # The vectorized case.
                for i in range(x.shape[0]):
                    self.rs.push(x[i])
                    self.buffer.push(x[i])
            else:
                # The unvectorized case.
                self.rs.push(x)
                self.buffer.push(x)
        if self.demean:
            x = x - self.rs.mean
        if self.destd:
            x = x / (self.rs.std + 1e-8)
        if self.clip:
            x = np.clip(x, -self.clip, self.clip)
        return x

    def __repr__(self):
        return "MeanStdFilter({}, {}, {}, {}, {}, {})".format(
            self.shape, self.demean, self.destd, self.clip, self.rs,
            self.buffer)


class ConcurrentMeanStdFilter(MeanStdFilter):
    is_concurrent = True

    def __init__(self, *args, **kwargs):
        super(ConcurrentMeanStdFilter, self).__init__(*args, **kwargs)
        self._lock = threading.RLock()

        def lock_wrap(func):
            def wrapper(*args, **kwargs):
                with self._lock:
                    return func(*args, **kwargs)

            return wrapper

        self.__getattribute__ = lock_wrap(self.__getattribute__)

    def as_serializable(self):
        """Returns non-concurrent version of current class"""
        other = MeanStdFilter(self.shape)
        other.sync(self)
        return other

    def copy(self):
        """Returns a copy of Filter."""
        other = ConcurrentMeanStdFilter(self.shape)
        other.sync(self)
        return other

    def __repr__(self):
        return "ConcurrentMeanStdFilter({}, {}, {}, {}, {}, {})".format(
            self.shape, self.demean, self.destd, self.clip, self.rs,
            self.buffer)


def get_filter(filter_config, shape):
    # TODO(rliaw): move this into filter manager
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
