import gymnasium as gym
from gymnasium.spaces import Box, Discrete, Tuple
import numpy as np
import random


class CorrelatedActionsEnv(gym.Env):
    """
    Simple env in which the policy has to emit a tuple of equal actions.

    In each step, the agent observes a random number (0 or 1) and has to choose
    two actions a1 and a2.
    It gets +5 reward for matching a1 to the random obs and +5 for matching a2
    to a1. I.e., +10 at most per step.

    One way to effectively learn this is through correlated action
    distributions, e.g., in examples/rl_modules/autoregressive_action_rlm.py

    There are 20 steps. Hence, the best score would be ~200 reward.
    """

    def __init__(self, _=None):
        self.observation_space = Box(0, 1, shape=(1,), dtype=np.float32)
        self.action_space = Tuple([Discrete(2), Discrete(2)])
        self.last_observation = None

    def reset(self, *, seed=None, options=None):
        self.t = 0
        self.last_observation = np.array([random.choice([0, 1])], dtype=np.float32)
        return self.last_observation, {}

    def step(self, action):
        self.t += 1
        a1, a2 = action
        reward = 0
        # Encourage correlation between most recent observation and a1.
        if a1 == self.last_observation:
            reward += 5
        # Encourage correlation between a1 and a2.
        if a1 == a2:
            reward += 5
        done = truncated = self.t > 20
        self.last_observation = np.array([random.choice([0, 1])], dtype=np.float32)
        return self.last_observation, reward, done, truncated, {}
