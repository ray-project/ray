import warnings
import numpy as np

class NoFilter(object):

  def __init__(self):
    pass

  def __call__(self, x, update=True):
    return np.asarray(x)

# http://www.johndcook.com/blog/standard_deviation/
class RunningStat(object):

  def __init__(self, shape=None):
    self._n = 0
    self._M = np.zeros(shape)
    self._S = np.zeros(shape)

  def push(self, x):
    x = np.asarray(x)
    # Unvectorized update of the running statistics.
    assert x.shape == self._M.shape, "x.shape = {}, self.shape = {}".format(x.shape, self._M.shape)
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

  @property
  def n(self):
    return self._n

  @property
  def mean(self):
    return self._M

  @property
  def var(self):
    return self._S/(self._n - 1) if self._n > 1 else np.square(self._M)

  @property
  def std(self):
    return np.sqrt(self.var)

  @property
  def shape(self):
    return self._M.shape

class MeanStdFilter(object):
  """
  y = (x-mean)/std
  using running estimates of mean,std
  """

  def __init__(self, shape, demean=True, destd=True, clip=10.0):
    self.demean = demean
    self.destd = destd
    self.clip = clip

    self.rs = RunningStat(shape)

  def __call__(self, x, update=True):
    x = np.asarray(x)
    if update:
      if len(x.shape) == len(self.rs.shape) + 1:
        # The vectorized case.
        for i in range(x.shape[0]):
          self.rs.push(x[i])
      else:
        # The unvectorized case.
        self.rs.push(x)
    if self.demean:
      x = x - self.rs.mean
    if self.destd:
      x = x / (self.rs.std+1e-8)
    if self.clip:
      if np.amin(x) < -self.clip or np.amax(x) > self.clip:
        print("Clipping value to " + str(self.clip))
      x = np.clip(x, -self.clip, self.clip)
    return x


def test_running_stat():
  for shp in ((), (3,), (3,4)):
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
  for shape in [(), (3,), (3,4)]:
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
