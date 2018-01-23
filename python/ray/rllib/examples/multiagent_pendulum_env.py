from gym.spaces import Box, Tuple
from gym.utils import seeding
from gym.envs.classic_control.pendulum import PendulumEnv
import numpy as np

"""
 Multiagent pendulum that sums its torques to generate an action
"""


class MultiAgentPendulumEnv(PendulumEnv):
    metadata = {
      'render.modes': ['human', 'rgb_array'],
      'video.frames_per_second': 30
    }

    def __init__(self):
        self.max_speed = 8
        self.max_torque = 2.
        self.dt = .05
        self.viewer = None

        high = np.array([1., 1., self.max_speed])
        self.action_space = [Box(low=-self.max_torque / 2,
                                 high=self.max_torque / 2, shape=(1,))
                             for _ in range(2)]
        self.observation_space = Tuple(tuple(Box(low=-high, high=high)
                                             for _ in range(2)))

        self._seed()

    def _seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def _step(self, u):
        th, thdot = self.state  # th := theta

        summed_u = np.sum(u)
        g = 10.
        m = 1.
        length = 1.
        dt = self.dt

        summed_u = np.clip(summed_u, -self.max_torque, self.max_torque)
        self.last_u = summed_u  # for rendering
        costs = self.angle_normalize(th) ** 2 + .1 * thdot ** 2 + \
            .001 * (summed_u ** 2)

        newthdot = thdot + (-3 * g / (2 * length) * np.sin(th + np.pi) +
                            3. / (m * length ** 2) * summed_u) * dt
        newth = th + newthdot * dt
        newthdot = np.clip(newthdot, -self.max_speed, self.max_speed)

        self.state = np.array([newth, newthdot])
        return self._get_obs(), -costs, False, {}

    def _reset(self):
        high = np.array([np.pi, 1])
        self.state = self.np_random.uniform(low=-high, high=high)
        self.last_u = None
        return self._get_obs()

    def _get_obs(self):
        theta, thetadot = self.state
        return [np.array([np.cos(theta), np.sin(theta), thetadot])
                for _ in range(2)]

    def angle_normalize(self, x):
        return (((x + np.pi) % (2 * np.pi)) - np.pi)
