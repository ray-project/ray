import gym
from gym.spaces import Box, Discrete
import numpy as np


class FastImageEnv(gym.Env):
    def __init__(self, config):
        self.zeros = np.zeros((84, 84, 4))
        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, 1.0, shape=(84, 84, 4), dtype=np.float32)
        self.i = 0

    def reset(self):
        self.i = 0
        return self.zeros

    def step(self, action):
        self.i += 1
        return self.zeros, 1, self.i > 1000, {}
