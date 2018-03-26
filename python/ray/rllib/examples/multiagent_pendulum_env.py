from gym.spaces import Tuple
from gym.envs.classic_control.pendulum import PendulumEnv
import numpy as np

"""
 Multiagent pendulum that has two identical pendulums being operated
 by separate agents. Their rewards are summed as we currently only
 support shared rewards.
"""


class DoubleMultiAgentPendulumEnv(PendulumEnv):
    metadata = {
      'render.modes': ['human', 'rgb_array'],
      'video.frames_per_second': 30
    }

    def __init__(self):
        super(DoubleMultiAgentPendulumEnv, self).__init__()
        self.action_space = [self.action_space for _ in range(2)]
        self.observation_space = Tuple([self.observation_space for
                                        _ in range(2)])

    def step(self, u):
        state = []
        costs = 0
        for i in range(2):
            th, thdot = self.state[i]  # th := theta

            summed_u = u[i][0]
            g = 10.
            m = 1.
            length = 1.
            dt = self.dt

            summed_u = np.clip(summed_u, -self.max_torque, self.max_torque)
            self.last_u = summed_u  # for rendering
            costs += self.angle_normalize(th) ** 2 + .1 * thdot ** 2 + \
                .001 * (summed_u ** 2)

            newthdot = thdot + (-3 * g / (2 * length) * np.sin(th + np.pi) +
                                3. / (m * length ** 2) * summed_u) * dt
            newth = th + newthdot * dt
            newthdot = np.clip(newthdot, -self.max_speed, self.max_speed)

            state += [np.array([newth[0], newthdot[0]])]

        self.state = state
        return self._get_obs(), -costs[0]/2, False, {}

    def reset(self):
        high = np.array([np.pi, 1])
        self.state = [self.np_random.uniform(low=-high, high=high)
                      for _ in range(2)]
        self.last_u = None
        return self._get_obs()

    def _get_obs(self):
        obs_arr = []
        for i in range(2):
            theta, thetadot = self.state[i]
            obs_arr += [np.array([np.cos(theta), np.sin(theta), thetadot])]
        return obs_arr

    def angle_normalize(self, x):
        return ((x + np.pi) % (2 * np.pi)) - np.pi
