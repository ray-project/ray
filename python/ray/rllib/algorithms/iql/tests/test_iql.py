import unittest

from ray.rllib.algorithms.iql import IQLConfig
from ray.rllib.algorithms.marwil import MARWILConfig


class TestIQL(unittest.TestCase):
    def test_iql_beta_validation(self):
        # IQL's `beta` is an inverse temperature, so values > 1.0 are valid.
        config = (
            IQLConfig().offline_data(dataset_num_iters_per_learner=1).training(beta=3.0)
        )
        config.validate()

        config.training(beta=0.0)
        with self.assertRaisesRegex(ValueError, "`beta`"):
            config.validate()

        # MARWIL still requires `beta` to be within [0.0, 1.0].
        config = (
            MARWILConfig()
            .offline_data(dataset_num_iters_per_learner=1)
            .training(beta=3.0)
        )
        with self.assertRaisesRegex(ValueError, "`beta` must be within"):
            config.validate()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
