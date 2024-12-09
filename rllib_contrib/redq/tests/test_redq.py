import unittest

import gymnasium as gym
import numpy as np
from gymnasium.spaces import Box
from redq import REDQConfig

import ray
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.test_utils import check_compute_single_action, check_train_results

torch, _ = try_import_torch()


class SimpleEnv(gym.Env):
    def __init__(self, config):
        self._skip_env_checking = True
        if config.get("simplex_actions", False):
            self.action_space = Simplex((2,))
        else:
            self.action_space = Box(0.0, 1.0, (1,))
        self.observation_space = Box(0.0, 1.0, (1,))
        self.max_steps = config.get("max_steps", 100)
        self.state = None
        self.steps = None

    def reset(self, *, seed=None, options=None):
        self.state = self.observation_space.sample()
        self.steps = 0
        return self.state, {}

    def step(self, action):
        self.steps += 1
        # Reward is 1.0 - (max(actions) - state).
        [rew] = 1.0 - np.abs(np.max(action) - self.state)
        terminated = False
        truncated = self.steps >= self.max_steps
        self.state = self.observation_space.sample()
        return self.state, rew, terminated, truncated, {}


class TestREDQ(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_redq_compilation(self):
        """Test whether REDQ can be built with torch."""
        config = (
            REDQConfig()
            .training(num_steps_sampled_before_learning_starts=0)
            .rollouts(num_rollout_workers=0, num_envs_per_worker=2)
            .exploration(exploration_config={"random_timesteps": 100})
            .framework("torch")
        )

        num_iterations = 1

        # Test on torch
        algo = config.build(env="Pendulum-v1")
        for _ in range(num_iterations):
            results = algo.train()
            check_train_results(results)
            print(results)
        check_compute_single_action(algo)

        algo.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
