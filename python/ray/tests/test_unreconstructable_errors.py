import numpy as np
import unittest

import ray


class TestObjectLostErrors(unittest.TestCase):
    def setUp(self):
        ray.init(
            num_cpus=1,
            object_store_memory=150 * 1024 * 1024,
            _redis_max_memory=10000000)

    def tearDown(self):
        ray.shutdown()

    def testDriverPutEvictedCannotReconstruct(self):
        x_id = ray.worker.global_worker.put_object(
            np.zeros(1 * 1024 * 1024), pin_object=False)
        ray.get(x_id)
        for _ in range(20):
            ray.put(np.zeros(10 * 1024 * 1024))
        self.assertRaises(ray.exceptions.ObjectLostError,
                          lambda: ray.get(x_id))


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
