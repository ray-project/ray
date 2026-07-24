import unittest

from ray.rllib.algorithms.iql import IQLConfig
from ray.rllib.algorithms.marwil import MARWILConfig


class TestIQLConfig(unittest.TestCase):
    def test_beta_validation(self):
        for beta in (0.1, 1.0, 3.0):
            with self.subTest(beta=beta):
                (
                    IQLConfig()
                    .framework(None)
                    .training(beta=beta)
                    .offline_data(dataset_num_iters_per_learner=1)
                    .validate()
                )

        for beta in (0.0, -0.1):
            with self.subTest(beta=beta), self.assertRaisesRegex(ValueError, "`beta`"):
                (
                    IQLConfig()
                    .framework(None)
                    .training(beta=beta)
                    .offline_data(dataset_num_iters_per_learner=1)
                    .validate()
                )

    def test_marwil_beta_validation_is_unchanged(self):
        for beta in (0.0, 1.0):
            with self.subTest(beta=beta):
                (
                    MARWILConfig()
                    .framework(None)
                    .training(beta=beta)
                    .offline_data(dataset_num_iters_per_learner=1)
                    .validate()
                )

        for beta in (-0.1, 1.1):
            with self.subTest(beta=beta), self.assertRaisesRegex(ValueError, "`beta`"):
                (
                    MARWILConfig()
                    .framework(None)
                    .training(beta=beta)
                    .offline_data(dataset_num_iters_per_learner=1)
                    .validate()
                )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
