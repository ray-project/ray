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

  C = {"hello": np.zeros(2 ** 30 + 1),
       "world": np.ones(2 ** 30 + 1)}
  c = ray.put(C)
  assert_almost_equal(ray.get(c)["hello"], C["hello"])
  assert_almost_equal(ray.get(c)["world"], C["world"])
  del C
  del c

  D = [np.ones(2 ** 30 + 1), 42.0 * np.ones(2 ** 30 + 1)]
  d = ray.put(D)
  assert_almost_equal(ray.get(d)[0], D[0])
  assert_almost_equal(ray.get(d)[1], D[1])
  del D
  del d

  E = (2 ** 30 + 1) * ["h"]
  e = ray.put(E)
  assert ray.get(e) == E
  del E
  del e

  F = (2 ** 30 + 1) * ("i",)
  f = ray.put(F)
  assert ray.get(f) == F
  del F
  del f
