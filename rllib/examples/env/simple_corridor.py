import gymnasium as gym
from gymnasium.spaces import Box, Discrete
import numpy as np


class SimpleCorridor(gym.Env):
    """Example of a custom env in which you have to walk down a corridor.

    You can configure the length of the corridor via the env config."""

    def __init__(self, config=None):
        config = config or {}
        self.end_pos = config.get("corridor_length", 10)
        self.cur_pos = 0
        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, 999.0, shape=(1,), dtype=np.float32)

    def set_corridor_length(self, length):
        self.end_pos = length
        print("Updated corridor length to {}".format(length))

    def reset(self, *, seed=None, options=None):
        self.cur_pos = 0.0
        return [self.cur_pos], {}

    def step(self, action):
        assert action in [0, 1], action
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1.0
        elif action == 1:
            self.cur_pos += 1.0
        done = truncated = self.cur_pos >= self.end_pos
        return [self.cur_pos], 1 if done else 0, done, truncated, {}
