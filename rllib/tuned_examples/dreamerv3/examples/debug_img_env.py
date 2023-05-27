import gymnasium as gym
import numpy as np


class DebugImgEnv(gym.Env):
    def __init__(
        self,
        repeat_action_probability=0.0,
        full_action_space=False,
        frameskip=0,
        render_mode=None,
    ):
        super().__init__()
        self.observation_space = gym.spaces.Box(0, 255, (64, 64, 3), dtype=np.uint8)
        self.action_space = gym.spaces.Discrete(6)
        self._ts = 0
        self.current_obs = None

    def reset(self, *, seed=None, options=None):
        self._ts = 0
        return self._obs(0), {}

    def step(self, action):
        self._ts += 1
        terminated = self._ts >= 10
        return self._obs(action), self._ts * 0.1, terminated, False, {}

    def _obs(self, prev_action):
        obs = np.full(
            shape=self.observation_space.shape,
            fill_value=self._ts,
            dtype=self.observation_space.dtype,
        )
        obs[0][0][0] = self._ts
        obs[0][0][1] = prev_action
        self.current_obs = obs
        return obs

    def render(self):
        return self.current_obs
