import gym
from gym.spaces import Discrete
import random


class RepeatAfterMeEnv(gym.Env):
    """Env in which the observation at timestep minus n must be repeated."""

    def __init__(self, config):
        self.observation_space = Discrete(2)
        self.action_space = Discrete(2)
        self.delay = config.get("repeat_delay", 1)
        assert self.delay >= 1, "`repeat_delay` must be at least 1!"
        self.history = []

    def reset(self):
        self.history = [0] * self.delay
        return self._next_obs()

    def step(self, action):
        if action == self.history[-(1 + self.delay)]:
            reward = 1
        else:
            reward = -1
        done = len(self.history) > 100
        return self._next_obs(), reward, done, {}

    def _next_obs(self):
        token = random.choice([0, 1])
        self.history.append(token)
        return token
