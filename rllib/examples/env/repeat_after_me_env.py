import gym
from gym.spaces import Box, Discrete
import numpy as np


class RepeatAfterMeEnv(gym.Env):
    """Env in which the observation at timestep minus n must be repeated."""

    def __init__(self, config=None):
        config = config or {}
        if config.get("continuous"):
            self.observation_space = Box(-1.0, 1.0, (2,))
        else:
            self.observation_space = Discrete(2)

        self.action_space = self.observation_space
        # Note: Set `repeat_delay` to 0 for simply repeating the seen
        # observation (no delay).
        self.delay = config.get("repeat_delay", 1)
        self.episode_len = config.get("episode_len", 100)
        self.history = []

    def reset(self):
        self.history = [0] * self.delay
        return self._next_obs()

    def step(self, action):
        obs = self.history[-(1 + self.delay)]

        # Box: -abs(diff).
        if isinstance(self.action_space, Box):
            reward = -np.sum(np.abs(action - obs))
        # Discrete: +1.0 if exact match, -1.0 otherwise.
        if isinstance(self.action_space, Discrete):
            reward = 1.0 if action == obs else -1.0

        done = len(self.history) > self.episode_len
        return self._next_obs(), reward, done, {}

    def _next_obs(self):
        if isinstance(self.observation_space, Box):
            token = np.random.random(size=(2,))
        else:
            token = np.random.choice([0, 1])
        self.history.append(token)
        return token
