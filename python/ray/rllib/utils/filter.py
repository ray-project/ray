from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np


class BaseFilter(object):

    def update(self, other, *args, **kwargs):
        """Given other filter, update self with "new state" from
        other filter. This may be in form of a buffer.
        """
        raise NotImplementedError

    def copy(self):
        """Creates a new object with same state as self. Returns copy."""
        raise NotImplementedError

    def sync(self, other):
        """Copies all state from other filter to self."""
        raise NotImplementedError


class NoFilter(BaseFilter):
    def __init__(self, *args):
        pass

    def __call__(self, x, update=True):
        return np.asarray(x)

    def update(self, other, *args, **kwargs):
        pass

    def copy(self):
        return self

    def sync(self, other):
        pass


# http://www.johndcook.com/blog/standard_deviation/
class RunningStat(object):

    def __init__(self, shape=None):
        """If single int, then return an 'array-shaped int'. This is needed
        in supporting usage as a reward filter."""
        self._n = 0
        self._M = np.asarray(0.0) if shape == 1 else np.zeros(shape)
        self._S = np.asarray(0.0) if shape == 1 else np.zeros(shape)

    def copy(self):
        other = RunningStat()
        other._n = self._n
        other._M = np.copy(self._M)
        other._S = np.copy(self._S)
        return other

    def push(self, x):
        x = np.asarray(x)
        # Unvectorized update of the running statistics.
        assert x.shape == self._M.shape, ("x.shape = {}, self.shape = {}"
                                          .format(x.shape, self._M.shape))
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
        delta = self._M - other._M
        delta2 = delta * delta
        M = (n1 * self._M + n2 * other._M) / n
        S = self._S + other._S + delta2 * n1 * n2 / n
        self._n = n
        self._M = M
        self._S = S

    def __repr__(self):
        return '(n={}, mean_mean={}, mean_std={})'.format(
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


class MeanStdFilter(object):
    """Keeps track of a running mean for seen states"""

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

    def update(self, other, copy_buffer=False):
        """Takes another filter and only applies the information from the
        buffer.

        Using notation F(state, buffer)
        Given Filter1(x1, y1) and Filter2(x2, yt),
        update modifies Filter1 to Filter1(x1 + yt, y1)
        """
        self.rs.update(other.buffer)
        if copy_buffer:
            self.buffer = other.buffer.copy()

    def copy(self):
        """Returns a copy of Filter."""
        other = MeanStdFilter(self.shape)
        other.demean = self.demean
        other.destd = self.destd
        other.clip = self.clip
        other.rs = self.rs.copy()
        other.buffer = self.buffer.copy()
        return other

    def sync(self, other):
        """Syncs all fields together from other filter.

        Using notation F(state, buffer)
        Given Filter1(x1, y1) and Filter2(x2, yt),
        update modifies Filter1 to Filter1(x2, yt)
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
        return 'MeanStdFilter({}, {}, {}, {}, {}, {})'.format(
            self.shape, self.demean, self.destd,
            self.clip, self.rs, self.buffer)


def get_filter(filter_config, shape):
    if filter_config == "MeanStdFilter":
        return MeanStdFilter(shape, clip=None)
    elif filter_config == "NoFilter":
        return NoFilter()
    else:
        raise Exception("Unknown observation_filter: " +
                        str(filter_config))


def test_running_stat():
    for shp in ((), (3,), (3, 4)):
        li = []
        rs = RunningStat(shp)
        for _ in range(5):
            val = np.random.randn(*shp)
            rs.push(val)
            li.append(val)
            m = np.mean(li, axis=0)
            assert np.allclose(rs.mean, m)
            v = np.square(m) if (len(li) == 1) else np.var(li, ddof=1, axis=0)
            assert np.allclose(rs.var, v)


def test_combining_stat():
    for shape in [(), (3,), (3, 4)]:
        li = []
        rs1 = RunningStat(shape)
        rs2 = RunningStat(shape)
        rs = RunningStat(shape)
        for _ in range(5):
            val = np.random.randn(*shape)
            rs1.push(val)
            rs.push(val)
            li.append(val)
        for _ in range(9):
            rs2.push(val)
            rs.push(val)
            li.append(val)
        rs1.update(rs2)
        assert np.allclose(rs.mean, rs1.mean)
        assert np.allclose(rs.std, rs1.std)


test_running_stat()
test_combining_stat()
