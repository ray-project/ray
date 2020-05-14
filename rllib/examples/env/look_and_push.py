import gym
import numpy as np


class OneHot(gym.Wrapper):
    def __init__(self, env):
        super(OneHot, self).__init__(env)
        self.observation_space = gym.spaces.Box(0., 1.,
                                                (env.observation_space.n, ))

    def reset(self, **kwargs):
        obs = self.env.reset(**kwargs)
        return self._encode_obs(obs)

    def step(self, action):
        obs, reward, done, info = self.env.step(action)
        return self._encode_obs(obs), reward, done, info

    def _encode_obs(self, obs):
        new_obs = np.ones(self.env.observation_space.n)
        new_obs[obs] = 1.0
        return new_obs


class LookAndPush(gym.Env):
    def __init__(self):
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Discrete(5)
        self._state = None
        self._case = None

    def reset(self):
        self._state = 2
        self._case = np.random.choice(2)
        return self._state

    def step(self, action):
        assert self.action_space.contains(action)

        if self._state == 4:
            if action and self._case:
                return self._state, 10., True, {}
            else:
                return self._state, -10, True, {}
        else:
            if action:
                if self._state == 0:
                    self._state = 2
                else:
                    self._state += 1
            elif self._state == 2:
                self._state = self._case

        return self._state, -1, False, {}
