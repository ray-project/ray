from gym.spaces import Box
import numpy as np

from gym.envs.classic_control import CartPoleEnv


class StatelessCartPole(CartPoleEnv):
    """Partially observable variant of the CartPole gym environment.

    https://github.com/openai/gym/blob/master/gym/envs/classic_control/
    cartpole.py

    We delete the x- and angular velocity components of the state, so that it
    can only be solved by a memory enhanced model (policy).
    """

    def __init__(self, config=None):
        super().__init__()

        # Fix our observation-space (remove 2 velocity components).
        high = np.array(
            [
                self.x_threshold * 2,
                self.theta_threshold_radians * 2,
            ],
            dtype=np.float32,
        )

        self.observation_space = Box(low=-high, high=high, dtype=np.float32)

    def step(self, action):
        next_obs, reward, done, info = super().step(action)
        # next_obs is [x-pos, x-veloc, angle, angle-veloc]
        return np.array([next_obs[0], next_obs[2]]), reward, done, info

    def reset(self):
        init_obs = super().reset()
        # init_obs is [x-pos, x-veloc, angle, angle-veloc]
        return np.array([init_obs[0], init_obs[2]])
