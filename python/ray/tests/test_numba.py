import unittest

from numba import njit
import numpy as np

import ray


@njit(fastmath=True)
def centroid(x, y):
    return ((x / x.sum()) * y).sum()


# Define a wrapper to call centroid function
@ray.remote
def centroid_wrapper(x, y):
    return centroid(x, y)


class NumbaTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1)

    def tearDown(self):
        ray.shutdown()

    def test_numba_njit(self):
        x = np.random.random(10)
        y = np.random.random(1)
        result = ray.get(centroid_wrapper.remote(x, y))
        assert result == centroid(x, y)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
