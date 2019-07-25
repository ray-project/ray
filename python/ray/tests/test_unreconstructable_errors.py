from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import unittest

import ray


class TestUnreconstructableErrors(unittest.TestCase):
    def setUp(self):
        ray.init(object_store_memory=10000000, redis_max_memory=10000000)

    def tearDown(self):
        ray.shutdown()

    def testDriverPutEvictedCannotReconstruct(self):
        x_id = ray.put(np.zeros(1 * 1024 * 1024))
        ray.get(x_id)
        for _ in range(10):
            ray.put(np.zeros(1 * 1024 * 1024))
        self.assertRaises(ray.exceptions.UnreconstructableError,
                          lambda: ray.get(x_id))

    def testLineageEvictedReconstructionFails(self):
        @ray.remote
        def f(data):
            return 0

        x_id = f.remote(None)
        ray.get(x_id)
        for _ in range(400):
            ray.get([f.remote(np.zeros(10000)) for _ in range(50)])
        self.assertRaises(ray.exceptions.UnreconstructableError,
                          lambda: ray.get(x_id))


if __name__ == "__main__":
    unittest.main(verbosity=2)
