"""Testing for trainer class"""
import copy
import unittest
from ray.rllib.agents.trainer import Trainer, COMMON_CONFIG


class TestTrainer(unittest.TestCase):
    def test_validate_config_idempotent(self):
        """
        Asserts that validate_config run multiple
        times on COMMON_CONFIG will be idempotent
        """
        # Given
        standard_config = copy.deepcopy(COMMON_CONFIG)

        # When (we validate config 2 times)
        Trainer._validate_config(standard_config)
        config_v1 = copy.deepcopy(standard_config)
        Trainer._validate_config(standard_config)
        config_v2 = copy.deepcopy(standard_config)

        # Then
        self.assertEqual(config_v1, config_v2)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
