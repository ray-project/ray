import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


class TestAlgorithmConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=6)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_from_legacy_dict(self):
        config_dict = {
            "evaluation_config": {
                "lr": 0.1,
            },
            "lr": 0.2,
        }
        config = AlgorithmConfig.from_dict(config_dict)
        self.assertFalse(config.in_evaluation)
        self.assertTrue(config.evaluation_config.in_evaluation)
        self.assertTrue(config.lr == 0.2)
        self.assertTrue(config.evaluation_config.lr == 0.1)
        print(config)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
