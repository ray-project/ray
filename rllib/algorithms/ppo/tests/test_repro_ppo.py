import unittest
import pytest

import ray
from ray.tune import register_env
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.examples.envs.classes.deterministic_envs import (
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

        register_env("DeterministicCartPole-v1", create_cartpole_deterministic)
        configs = (
            ppo.PPOConfig()
            .environment(env="DeterministicCartPole-v1", env_config={"seed": 42})
            .env_runners(rollout_fragment_length=8)
            .training(train_batch_size=64, minibatch_size=32, num_epochs=2)
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

        register_env("DeterministicPendulum-v1", create_pendulum_deterministic)
        configs = (
            ppo.PPOConfig()
            .environment(env="DeterministicPendulum-v1", env_config={"seed": 42})
            .env_runners(rollout_fragment_length=8)
            .training(train_batch_size=64, minibatch_size=32, num_epochs=2)
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
