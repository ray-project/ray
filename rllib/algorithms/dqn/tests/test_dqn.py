import unittest

import ray
import ray.rllib.algorithms.dqn as dqn
from ray.rllib.utils.test_utils import check_train_results_new_api_stack


class TestDQN(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_dqn_compilation(self):
        """Test whether DQN can be built and trained."""
        num_iterations = 2
        config = (
            dqn.dqn.DQNConfig()
            .environment("CartPole-v1")
            .env_runners(num_env_runners=2)
            .training(num_steps_sampled_before_learning_starts=0)
        )

        # Double-dueling DQN.
        print("Double-dueling")
        algo = config.build()
        for i in range(num_iterations):
            results = algo.train()
            check_train_results_new_api_stack(results)
            print(results)

        algo.stop()

        # Rainbow.
        print("Rainbow")
        config.training(num_atoms=10, double_q=True, dueling=True, n_step=5)
        algo = config.build()
        for i in range(num_iterations):
            results = algo.train()
            check_train_results_new_api_stack(results)
            print(results)

        algo.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
