import logging

import gymnasium as gym
import numpy as np
from gymnasium.spaces import Box, Discrete

logger = logging.getLogger("ray.rllib")


class SimpleCorridor(gym.Env):
    """Example of a custom env in which you have to walk down a corridor.

    You can configure the length of the corridor via the env config."""

    def __init__(self, config=None):
        config = config or {}

        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, 999.0, shape=(1,), dtype=np.float32)

        self.set_corridor_length(config.get("corridor_length", 10))

        self._cur_pos = 0

    def set_corridor_length(self, length):
        self.end_pos = length
        logger.info(f"Set corridor length to {self.end_pos}")
        assert self.end_pos <= 999, "The maximum `corridor_length` allowed is 999!"

    def reset(self, *, seed=None, options=None):
        self._cur_pos = 0.0
        return self._get_obs(), {}

    def step(self, action):
        assert action in [0, 1], action
        if action == 0 and self._cur_pos > 0:
            self._cur_pos -= 1.0
        elif action == 1:
            self._cur_pos += 1.0
        terminated = self._cur_pos >= self.end_pos
        truncated = False
        reward = 1.0 if terminated else -0.01
        return self._get_obs(), reward, terminated, truncated, {}

    def _get_obs(self):
        return np.array([self._cur_pos], np.float32)
