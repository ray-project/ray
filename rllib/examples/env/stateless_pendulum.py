from gymnasium.spaces import Box
import numpy as np

from gymnasium.envs.classic_control import PendulumEnv


class StatelessPendulum(PendulumEnv):
    """Partially observable variant of the Pendulum gym environment.

    https://github.com/openai/gym/blob/master/gym/envs/classic_control/
    pendulum.py

    We delete the angular velocity component of the state, so that it
    can only be solved by a memory enhanced model (policy).
    """

    def __init__(self, config=None):
        config = config or {}
        g = config.get("g", 10.0)

        super().__init__(g=g)

        # Fix our observation-space (remove angular velocity component).
        high = np.array([1.0, 1.0], dtype=np.float32)
        self.observation_space = Box(low=-high, high=high, dtype=np.float32)

    def step(self, action):
        next_obs, reward, done, truncated, info = super().step(action)
        # next_obs is [cos(theta), sin(theta), theta-dot (angular velocity)]
        return next_obs[:-1], reward, done, truncated, info

    def reset(self, *, seed=None, options=None):
        init_obs, init_info = super().reset(seed=seed, options=options)
        # init_obs is [cos(theta), sin(theta), theta-dot (angular velocity)]
        return init_obs[:-1], init_info
