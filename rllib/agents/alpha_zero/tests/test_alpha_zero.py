import unittest

import ray
import ray.rllib.agents.alpha_zero as az
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
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
        """Test whether an AlphaZeroTrainer can be built with all frameworks."""
        config = az.DEFAULT_CONFIG.copy()
        num_iterations = 1

        # Only working for torch right now.
        for _ in framework_iterator(config, frameworks="torch"):
            trainer = az.AlphaZeroTrainer(env="CartPole-v0")
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)

            check_compute_single_action(trainer, include_prev_action_reward=True)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
