import unittest

import ray
from ray.tune import register_env
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.examples.env.deterministic_envs import (
    DeterministicCartPole, DeterministicPendulum
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

        register_env(
            "DeterministicCartPole-v0", 
            lambda _: DeterministicCartPole(seed=42)
        )
        check_reproducibilty(
            algo_class=ppo.PPO,
            algo_config=ppo.PPOConfig().environment(env="DeterministicCartPole-v0"),
            fw_kwargs={"frameworks": ("tf", "torch")},
            training_iteration=3,
        )

    def test_reproducibility_ppo_pendulum(self):
        """Tests whether the algorithm is reproducible within 3 iterations 
        on continuous env pendulum."""
        register_env(
            "DeterministicPendulum-v1", 
            lambda _: DeterministicPendulum(seed=42)
        )

        check_reproducibilty(
            algo_class=ppo.PPO,
            algo_config=ppo.PPOConfig().environment(env="DeterministicPendulum-v1"),
            fw_kwargs={"frameworks": ("tf", "torch")},
            training_iteration=3,
        )

if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))