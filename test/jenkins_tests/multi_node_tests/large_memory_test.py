from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
from numpy.testing import assert_almost_equal

import ray


if __name__ == "__main__":
  ray.init()

  A = np.zeros(2 ** 31 + 1, dtype="int8")
  a = ray.put(A)
  assert_almost_equal(ray.get(a), A)
  del A
  del a

  B = {"hello": np.zeros(2 ** 30 + 1),
       "world": np.ones(2 ** 30 + 1)}
  b = ray.put(B)
  assert_almost_equal(ray.get(b)["hello"], B["hello"])
  assert_almost_equal(ray.get(b)["world"], B["world"])
  del B
  del b

  C = [np.ones(2 ** 30 + 1), 42.0 * np.ones(2 ** 30 + 1)]
  c = ray.put(C)
  assert_almost_equal(ray.get(c)[0], C[0])
  assert_almost_equal(ray.get(c)[1], C[1])
  del C
  del c

  D = (2 ** 30 + 1) * ["h"]
  d = ray.put(D)
  assert ray.get(d) == D
  del D
  del d

  E = (2 ** 30 + 1) * ("i",)
  e = ray.put(E)
  assert ray.get(e) == E
  del E
  del e
