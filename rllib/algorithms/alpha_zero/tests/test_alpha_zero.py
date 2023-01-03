import os
import unittest

import ray
import ray.rllib.algorithms.alpha_zero as az
from ray.rllib.algorithms.alpha_zero.models.custom_torch_models import DenseModel
from ray.rllib.examples.env.cartpole_sparse_rewards import CartPoleSparseRewards
from ray.rllib.utils.test_utils import (
    check_train_results,
    framework_iterator,
)


class TestAlphaZero(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_alpha_zero_compilation(self):
        """Test whether AlphaZero can be built with all frameworks."""
        config = (
            az.AlphaZeroConfig()
            .resources(
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                num_gpus=float(os.environ.get("RLLIB_NUM_GPUS", "0"))
            )
            .environment(env=CartPoleSparseRewards)
            .training(model={"custom_model": DenseModel})
        )
        num_iterations = 1

        # Only working for torch right now.
        for _ in framework_iterator(config, frameworks="torch"):
            algo = config.build()
            for i in range(num_iterations):
                results = algo.train()
                check_train_results(results)
                print(results)
            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
