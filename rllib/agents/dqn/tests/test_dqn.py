import numpy as np
import unittest

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

    def test_dqn_exploration_and_soft_q_config(self):
        """Tests, whether a DQN Agent outputs exploration/softmaxed actions."""
        config = dqn.DEFAULT_CONFIG.copy()
        config["eager"] = True
        config["num_workers"] = 0  # Run locally.
        config["env_config"] = {"is_slippery": False, "map_name": "4x4"}

        # Default EpsilonGreedy setup.
        trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
        # Setting exploit=True should always return the same action.
        a_ = trainer.compute_action(observation=np.array(0), exploit=True)
        for _ in range(50):
            a = trainer.compute_action(observation=np.array(0), exploit=True)
            check(a, a_)
        # exploit=False should return different (random) actions.
        actions = []
        for _ in range(50):
            actions.append(trainer.compute_action(observation=np.array(0)))
        check(np.mean(actions), 1.5, atol=0.2)

        # Low softmax temperature. Behaves like argmax
        # (but no epsilon exploration).
        config["exploration"] = {"type": "SoftQ", "temperature": 0.0}
        trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
        # Due to the low temp, always expect the same action.
        a_ = trainer.compute_action(observation=np.array(0))
        for _ in range(50):
            a = trainer.compute_action(observation=np.array(0))
            check(a, a_)

        # Higher softmax temperature.
        config["exploration"]["temperature"] = 1.0
        trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")

        # Even with the higher temperature, if we set exploit=True, we should
        # expect the same actions always.
        a_ = trainer.compute_action(observation=np.array(0), exploit=True)
        for _ in range(50):
            a = trainer.compute_action(observation=np.array(0), exploit=True)
            check(a, a_)

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
