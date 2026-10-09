import unittest

from ray.rllib.algorithms.registry import (
    ALGORITHMS,
    ALGORITHMS_CLASS_TO_NAME,
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

    def test_registered_algorithm_names(self):
        """All RLlib registered algorithms should have their name listed in the
        registry dictionary."""

        for class_name in ALGORITHMS_CLASS_TO_NAME.keys():
            registered_name = ALGORITHMS_CLASS_TO_NAME[class_name]
            algo_class, _ = ALGORITHMS[registered_name]()
            self.assertEqual(class_name.upper(), algo_class.__name__.upper())


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
