import unittest

import ray
from ray.rllib.algorithms.registry import ALGORITHMS


class TestAlgorithmImport(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def test_algo_import(self):
        for name, func in ALGORITHMS.items():
            func()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
