import unittest

import ray
from ray.tune import register_env
import ray.rllib.algorithms.simple_q as simple_q
from ray.rllib.examples.env.deterministic_envs import create_cartpole_deterministic
from ray.rllib.utils.test_utils import check_reproducibilty


class TestReproSimpleQ(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_reproducibility_dqn_cartpole(self):
        """Tests whether the algorithm is reproducible within 3 iterations
        on discrete env cartpole."""

        register_env("DeterministicCartPole-v0", create_cartpole_deterministic)
        config = simple_q.SimpleQConfig().environment(
            env="DeterministicCartPole-v0", env_config={"seed": 42}
        )
        check_reproducibilty(
            algo_class=simple_q.SimpleQ,
            algo_config=config,
            fw_kwargs={"frameworks": ("tf", "torch")},
            training_iteration=3,
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
