import gymnasium as gym
import pytest
import unittest

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.examples.env.deterministic_envs import (
    create_cartpole_deterministic,
    create_pendulum_deterministic,
)
from ray.rllib.utils.test_utils import check_reproducibilty


class TestReproPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_reproducibility_ppo_cartpole(self):
        """Tests whether the algorithm is reproducible within 3 iterations
        on discrete env cartpole."""

        gym.register(
            "DeterministicCartPole",
            create_cartpole_deterministic,
            kwargs={"seed": 42},
        )
        configs = (
            ppo.PPOConfig()
            .environment("DeterministicCartPole")
            .rollouts(rollout_fragment_length=8)
            .training(train_batch_size=64, sgd_minibatch_size=32, num_sgd_iter=2)
        )
        check_reproducibilty(
            algo_class=ppo.PPO,
            algo_config=configs,
            fw_kwargs={"frameworks": ("tf", "torch")},
            training_iteration=3,
        )

    def test_reproducibility_ppo_pendulum(self):
        """Tests whether the algorithm is reproducible within 3 iterations
        on continuous env pendulum."""

        gym.register(
            "DeterministicPendulum", create_pendulum_deterministic, kwargs={"seed": 42}
        )
        configs = (
            ppo.PPOConfig()
            .environment("DeterministicPendulum")
            .rollouts(rollout_fragment_length=8)
            .training(train_batch_size=64, sgd_minibatch_size=32, num_sgd_iter=2)
        )
        check_reproducibilty(
            algo_class=ppo.PPO,
            algo_config=configs,
            fw_kwargs={"frameworks": ("tf", "torch")},
            training_iteration=3,
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
