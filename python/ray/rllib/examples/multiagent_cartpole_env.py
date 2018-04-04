"""
Classic cart-pole system implemented by Rich Sutton et al.
Copied from http://incompleteideas.net/sutton/book/code/pole.c
permalink: https://perma.cc/C9ZM-652R
"""

import math
from gym import spaces, logger
from gym.envs.classic_control.cartpole import CartPoleEnv
from gym.spaces import Tuple
import numpy as np


class MultiAgentCartPoleEnv(CartPoleEnv):
    metadata = {
        'render.modes': ['human', 'rgb_array'],
        'video.frames_per_second': 50
    }

    def __init__(self):
        super().__init__()
        high = np.array([
            self.x_threshold * 2,
            np.finfo(np.float32).max,
            self.theta_threshold_radians * 2,
            np.finfo(np.float32).max])
        self.action_space = Tuple([spaces.Discrete(2) for _ in range(2)])
        self.observation_space = Tuple(
            [spaces.Box(-high, high, dtype=np.float32) for _ in range(2)])

    def step(self, action):
        summed_action = int(np.sum(np.asarray(action) / 2))
        state = self.state
        x, x_dot, theta, theta_dot = state
        force = self.force_mag if summed_action == 1 else -self.force_mag
        costheta = math.cos(theta)
        sintheta = math.sin(theta)
        temp = (force + self.polemass_length * theta_dot *
                theta_dot * sintheta) / self.total_mass
        thetaacc = (self.gravity * sintheta - costheta * temp) / (
                self.length * (4.0 / 3.0 - self.masspole *
                               costheta * costheta / self.total_mass))
        xacc = temp - self.polemass_length * thetaacc \
            * costheta / self.total_mass
        x = x + self.tau * x_dot
        x_dot = x_dot + self.tau * xacc
        theta = theta + self.tau * theta_dot
        theta_dot = theta_dot + self.tau * thetaacc
        self.state = (x, x_dot, theta, theta_dot)
        done = x < -self.x_threshold \
            or x > self.x_threshold \
            or theta < -self.theta_threshold_radians \
            or theta > self.theta_threshold_radians
        done = bool(done)

        if not done:
            reward = 1.0
        elif self.steps_beyond_done is None:
            # Pole just fell!
            self.steps_beyond_done = 0
            reward = 1.0
        else:
            if self.steps_beyond_done == 0:
                logger.warn(
                    "You are calling 'step()' even though this "
                    "environment has already returned done = True. "
                    "You should always call 'reset()' once you receive "
                    "'done = True' -- any further steps are "
                    "undefined behavior.")
            self.steps_beyond_done += 1
            reward = 0.0

        return [np.array(self.state) for _ in range(2)], reward, done, {}

    def reset(self):
        self.state = self.np_random.uniform(low=-0.05, high=0.05, size=(4,))
        self.steps_beyond_done = None
        return [np.array(self.state) for _ in range(2)]
