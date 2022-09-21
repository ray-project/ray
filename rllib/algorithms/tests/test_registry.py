import unittest

from ray.rllib.algorithms.registry import (
    POLICIES,
    get_policy_class,
    get_policy_class_name,
)


class TestPolicies(unittest.TestCase):
    def test_load_policies(self):
        for name in POLICIES.keys():
            self.assertIsNotNone(get_policy_class(name))

    def test_get_eager_traced_class_name(self):
        from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF2Policy

        traced = PPOTF2Policy.with_tracing()
        self.assertEqual(get_policy_class_name(traced), "PPOTF2Policy")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
