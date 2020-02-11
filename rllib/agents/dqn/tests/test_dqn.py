import numpy as np
import unittest

import ray
import ray.rllib.agents.dqn as dqn
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check

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
        config["soft_q"] = True  # This will automatically disable exploration.
        config["evaluation_num_episodes"] = 100
        config["evaluation_interval"] = 1
        config["env_config"] = {"is_slippery": False, "map_name": "4x4"}

        # Low softmax temperature. Behaves like argmax
        # (but no epsilon exploration).
        config["softmax_temperature"] = 0.0
        trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
        # Assert that exploration has been switched off.
        check(trainer.config["exploration"], False)
        # Due to the low temp, always expect the same action.
        a_ = trainer.compute_action(observation=np.array(0))
        for _ in range(50):
            a = trainer.compute_action(observation=np.array(0))
            check(a, a_)

        # Higher softmax temperature.
        config["softmax_temperature"] = 1.0
        trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
        # Due to the higher temp, expect different actions avg'ing around 1.5.
        actions = []
        for _ in range(300):
            actions.append(trainer.compute_action(observation=np.array(0)))
        check(np.mean(actions), 1.5, atol=0.2)

        # Test n train runs.
        num_iterations = 2
        for i in range(num_iterations):
            results = trainer.train()
            print(results)
