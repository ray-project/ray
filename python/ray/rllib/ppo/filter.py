from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np


class NoFilter(object):
    def __init__(self):
        pass

    def __call__(self, x, update=True):
        return np.asarray(x)

    def update(self, other):
        pass

    def copy(self):
        return self


# http://www.johndcook.com/blog/standard_deviation/
class RunningStat(object):

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

    def update(self, other):
        # `update` takes another filter and
        # only applies the information from the buffer.
        self.rs.update(other.buffer)

    def copy(self):
        other = MeanStdFilter(self.shape)
        other.demean = self.demean
        other.destd = self.destd
        other.clip = self.clip
        other.rs = self.rs.copy()
        other.buffer = self.buffer.copy()
        return other

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
        return 'MeanStdFilter({}, {}, {}, {}, {})'.format(
            self.shape, self.demean, self.destd, self.clip, self.rs)


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
