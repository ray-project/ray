import unittest

from ray.rllib.algorithms.registry import POLICIES, get_policy_class


class TestPolicies(unittest.TestCase):
    def test_load_policies(self):
        for name in POLICIES.keys():
            self.assertIsNotNone(get_policy_class(name))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
