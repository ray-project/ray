import gym
from gym.spaces import Discrete, Tuple
import random


class CorrelatedActionsEnv(gym.Env):
    """Simple env in which the policy has to emit a tuple of equal actions.

    The best score would be ~200 reward."""

    def __init__(self, _):
        self.observation_space = Discrete(2)
        self.action_space = Tuple([Discrete(2), Discrete(2)])

    def reset(self):
        self.t = 0
        self.last = random.choice([0, 1])
        return self.last

    def step(self, action):
        self.t += 1
        a1, a2 = action
        reward = 0
        if a1 == self.last:
            reward += 5
        # encourage correlation between a1 and a2
        if a1 == a2:
            reward += 5
        done = self.t > 20
        self.last = random.choice([0, 1])
        return self.last, reward, done, {}
