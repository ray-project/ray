import gymnasium as gym
import numpy as np
from gymnasium.spaces import Box, Discrete


class FastImageEnv(gym.Env):
    def __init__(self, config):
        self.zeros = np.zeros((84, 84, 4))
        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, 1.0, shape=(84, 84, 4), dtype=np.float32)
        self.i = 0

    def reset(self, *, seed=None, options=None):
        self.i = 0
        return self.zeros, {}

    def step(self, action):
        self.i += 1
        done = truncated = self.i > 1000
        return self.zeros, 1, done, truncated, {}
