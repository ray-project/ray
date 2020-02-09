import numpy as np
import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.utils import check, try_import_tf

tf = try_import_tf()


class TestExplorations(unittest.TestCase):
    """
    Tests all Exploration components and the deterministic flag for
    compute_action calls.
    """

    ray.init()

    def test_deterministic_flag(self):
        # Test calling PPO's compute_actions method with different exploration
        # options.
        config = ppo.DEFAULT_CONFIG.copy()
        config["eager"] = True
        config["num_workers"] = 0
        trainer = ppo.PPOTrainer(env="CartPole-v0")
        dummy_obs = np.array([0.0, 0.1, 0.0, 0.0])

        # Make sure all actions drawn are the same, given same observations.
        actions = []
        for _ in range(100):
            actions.append(
                trainer.compute_action(
                    observation=dummy_obs, deterministic=True))
            check(actions[-1], actions[0])

        # Make sure actions drawn are different, given constant observations.
        actions = []
        for _ in range(100):
            actions.append(
                trainer.compute_action(
                    observation=dummy_obs, deterministic=False))
        check(np.mean(actions), 0.5, atol=0.15)

    def test_epsilon_greedy(self):
        pass
