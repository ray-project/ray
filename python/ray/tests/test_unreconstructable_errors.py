import numpy as np
import unittest

import ray
from ray import ray_constants


class TestUnreconstructableErrors(unittest.TestCase):
    def setUp(self):
        ray.init(
            num_cpus=1,
            object_store_memory=150 * 1024 * 1024,
            redis_max_memory=10000000)

    def tearDown(self):
        ray.shutdown()

    def testDriverPutEvictedCannotReconstruct(self):
        x_id = ray.put(np.zeros(1 * 1024 * 1024), weakref=True)
        ray.get(x_id)
        for _ in range(20):
            ray.put(np.zeros(10 * 1024 * 1024))
        self.assertRaises(ray.exceptions.UnreconstructableError,
                          lambda: ray.get(x_id))

    def testLineageEvictedReconstructionFails(self):
        if ray_constants.direct_call_enabled():
            return  # not relevant

        @ray.remote
        def f(data):
            return 0

        x_id = f.remote(None)
        ray.get(x_id)
        # Hold references to the ray.put objects so they aren't LRU'd.
        oids = []
        for _ in range(400):
            new_oids = [f.remote(np.zeros(10000)) for _ in range(50)]
            oids.extend(new_oids)
            ray.get(new_oids)
        self.assertRaises(ray.exceptions.UnreconstructableError,
                          lambda: ray.get(x_id))


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
