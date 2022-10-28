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

    def test_creating_algo_config_from_legacy_dict(self):
        """Tests, whether translation from dict to AlgorithmConfig works as expected."""
        config_dict = {
            "evaluation_config": {
                "lr": 0.1,
            },
            "lr": 0.2,
            # Old-style multi-agent dict.
            "multiagent": {
                "policies": {"pol1", "pol2"},
                "policies_to_train": ["pol1"],
                "policy_mapping_fn": lambda aid, episode, worker, **kwargs: "pol1",
            },
        }
        config = AlgorithmConfig.from_dict(config_dict)
        self.assertFalse(config.in_evaluation)
        self.assertTrue(config.lr == 0.2)
        self.assertTrue(config.policies == {"pol1", "pol2"})
        self.assertTrue(config.policy_mapping_fn(1, 2, 3) == "pol1")
        eval_config = config.get_evaluation_config_object()
        self.assertTrue(eval_config.in_evaluation)
        self.assertTrue(eval_config.lr == 0.1)

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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
