import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPO


class TestAlgorithmConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=6)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_running_specific_algo_with_generic_config(self):
        """Tests, whether some algo can be run with the generic AlgorithmConfig."""
        config = (
            AlgorithmConfig(algo_class=PPO)
            .environment("CartPole-v0")
            .training(lr=0.12345, train_batch_size=3000)
        )
        algo = config.build()
        self.assertTrue(algo.config.lr == 0.12345)
        self.assertTrue(algo.config.train_batch_size == 3000)
        algo.train()
        algo.stop()

    def test_freezing_of_algo_config(self):
        """Tests, whether freezing an AlgorithmConfig actually works as expected."""
        config = (
            AlgorithmConfig()
            .environment("CartPole-v0")
            .training(lr=0.12345, train_batch_size=3000)
            .multi_agent(
                policies={"pol1": (None, None, None, {"lr": 0.001})},
                policy_mapping_fn=lambda aid, episode, worker, **kw: "pol1",
            )
        )
        config.freeze()

        def set_lr(config):
            config.lr = 0.01

        self.assertRaisesRegex(
            AttributeError,
            "Cannot set attribute.+of an already frozen AlgorithmConfig",
            lambda: set_lr(config),
        )

        def set_one_policy(config):
            config.policies["pol1"] = (None, None, None, {"lr": 0.123})

        self.assertRaisesRegex(
            AttributeError,
            "Cannot set attribute.+of an already frozen AlgorithmConfig",
            lambda: set_one_policy(config),
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
