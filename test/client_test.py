from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray
import sys
import pytest


# NOTE: These client tests run a gateway locally, which I don't think
# makes any sense in any real-world scenario. A true Client test would
# require multiple machines with a network configured more realistically.
@pytest.fixture
class ClientTest():
    def tearDown(self):
        ray.worker.cleanup()

    def testBasicClient(self):
        ray.init(driver_mode=ray.CLIENT_MODE)

        data = 1
        a = ray.put(data)
        self.assertEqual(ray.get(a), data)

    def testArrayClient(self):
        ray.init(driver_mode=ray.CLIENT_MODE)

        data = [1, 2, 3]
        a = ray.put(data)
        self.assertSequenceEqual(ray.get(a), data)

    def testNumpyClient(self):
        ray.init(driver_mode=ray.CLIENT_MODE)

        data = np.random.rand(4, 4)
        a = ray.put(data)
        self.assertEqual(ray.get(a), data)
