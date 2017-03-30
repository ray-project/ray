import numpy as np
from numpy.testing import assert_equal, assert_almost_equal
import os
import time

import ray


if __name__ == "__main__":
  ray.init()

  A = np.zeros(2 ** 31, dtype="int8")
  a = ray.put(A)
  assert_almost_equal(ray.get(a), A)

  # B = np.zeros(2 ** 33, dtype="int8")
  # b = ray.put(B)
  # assert assert_almost_equal(ray.get(b), B)

  # C = {"hello": np.zeros(2 ** 32 + 1),
  #      "world": np.ones(2 ** 32 + 1)}
  # c = ray.put(C)
  # assert assert_almost_equal(ray.get(c)["hello"], C["hello"])
  # assert assert_almost_equal(ray.get(c)["world"], C["world"])

  # D = [np.ones(2 ** 32 + 1), 42.0 * np.ones(2 ** 32 + 1)]
  # d = ray.put(D)
  # assert assert_almost_equal(ray.get(d)[0], D[0])
  # assert assert_almost_equal(ray.get(d)[1], D[1])

  # E = (2 ** 32 + 1) * ["h"]
  # e = ray.put(E)
  # assert ray.get(e) == E

  # F = (2 ** 32 + 1) * ("i",)
  # f = ray.put(F)
  # assert ray.get(f) == F
