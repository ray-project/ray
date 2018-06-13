from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray
import sys
import unittest

import ray.test.test_functions as test_functions

if sys.version_info >= (3, 0):
    from importlib import reload


# NOTE: These client tests run a gateway locally, which I don't think
# makes any sense in any real-world scenario. A true Client test would
# require multiple machines with a network configured more realistically.
class ClientTest(unittest.TestCase):
    def tearDown(self):
        ray.worker.cleanup()

    def testBasicClient(self):
        ray.init(driver_mode=ray.CLIENT_MODE)

        a = ray.put(1)
        self.assertEqual(ray.get(a), 1)

    def testNumpyClient(self):
        ray.init(driver_mode=ray.CLIENT_MODE)

        data = np.random.rand(4, 4)
        a = ray.put(data)
        self.assertEqual(ray.get(a), data)


if __name__ == "__main__":
    unittest.main(verbosity=2)
