import unittest

import ray
import ray.rllib.agents.dqn as dqn
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


class TestDQN(unittest.TestCase):
    def test_dqn_compilation(self):
        """Test whether a DQNTrainer can be built with both frameworks."""
        config = dqn.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["eager"] = True

        # tf.
        trainer = dqn.DQNTrainer(config=config, env="CartPole-v0")

        num_iterations = 2
        for i in range(num_iterations):
            results = trainer.train()
            print(results)

    def test_soft_q_config(self):
        """Tests, whether a soft-DQN Agent outputs actions stochastically."""
        ray.init()

        config = dqn.DEFAULT_CONFIG.copy()
        config["eager"] = True
        config["num_workers"] = 0  # Run locally.
        #config["evaluation_num_workers"] = 1  # Evaluate locally.
        config["soft_q"] = True
        config["softmax_temperature"] = 0.0   # almost argmax
        config["evaluation_num_episodes"] = 100
        config["evaluation_interval"] = 1
        config["env_config"] = {"render": True, "is_slippery": False, "map_name": "4x4"}

        # tf.
        trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
        metrics = trainer._evaluate()

        num_iterations = 2
        for i in range(num_iterations):
            results = trainer.train()
            print(results)
