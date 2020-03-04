import gym

import numpy as np


class LookAndPush(gym.Env):
    def __init__(self, env_config):
        del env_config
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Discrete(5)
        self._state = None
        self._case = None

    def reset(self):
        self._state = np.array([2])
        self._case = np.random.choice(2)
        return self._state

    def step(self, action):
        assert self.action_space.contains(action)

        if self._state[0] == 4:
            if action and self._case:
                return self._state, 10., True, {}
            else:
                return self._state, -10, False, {}
        else:
            if action:
                if self._state[0] == 0:
                    self._state += 2
                else:
                    self._state += 1
            elif self._state[0] == 2:
                self._state[0] = self._case

        return self._state, -1, False, {}



