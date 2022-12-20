from gym.wrappers import TimeLimit
import unittest

import ray
import ray.rllib.algorithms.mbmpo as mbmpo
from ray.rllib.examples.env.mbmpo_env import CartPoleWrapper
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)
from ray.tune.registry import register_env


class TestMBMPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()
        register_env(
            "cartpole-mbmpo",
            lambda env_ctx: TimeLimit(CartPoleWrapper(), max_episode_steps=200),
        )

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_mbmpo_compilation(self):
        """Test whether MBMPO can be built with all frameworks."""
        config = (
            mbmpo.MBMPOConfig()
            .environment("cartpole-mbmpo")
            .rollouts(num_rollout_workers=2)
            .training(dynamics_model={"ensemble_size": 2})
        )
        num_iterations = 1

        # Test for torch framework (tf not implemented yet).
        for _ in framework_iterator(config, frameworks="torch"):
            algo = config.build()

            for i in range(num_iterations):
                results = algo.train()
                check_train_results(results)
                print(results)

            check_compute_single_action(algo, include_prev_action_reward=False)
            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
