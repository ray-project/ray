import unittest

import ray
from ray.tune import register_env
import ray.rllib.algorithms.simple_q as simple_q
from ray.rllib.examples.env.deterministic_envs import DeterministicCartPole
from ray.rllib.utils.test_utils import check_reproducibilty


class TestReproPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_reproducibility_dqn_cartpole(self):
        """Tests whether the algorithm is reproducible within 3 iterations
        on discrete env cartpole."""

        register_env(
            "DeterministicCartPole-v0", lambda _: DeterministicCartPole(seed=42)
        )
        config = simple_q.SimpleQConfig().environment(env="DeterministicCartPole-v0")
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
