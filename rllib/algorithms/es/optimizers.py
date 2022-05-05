# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

import numpy as np


class Optimizer:
    def __init__(self, policy):
        self.policy = policy
        self.dim = policy.num_params
        self.t = 0

    def update(self, globalg):
        self.t += 1
        step = self._compute_step(globalg)
        theta = self.policy.get_flat_weights()
        ratio = np.linalg.norm(step) / np.linalg.norm(theta)
        return theta + step, ratio

    def _compute_step(self, globalg):
        raise NotImplementedError


class SGD(Optimizer):
    def __init__(self, policy, stepsize, momentum=0.0):
        Optimizer.__init__(self, policy)
        self.v = np.zeros(self.dim, dtype=np.float32)
        self.stepsize, self.momentum = stepsize, momentum

    def _compute_step(self, globalg):
        self.v = self.momentum * self.v + (1.0 - self.momentum) * globalg
        step = -self.stepsize * self.v
        return step


class Adam(Optimizer):
    def __init__(self, policy, stepsize, beta1=0.9, beta2=0.999, epsilon=1e-08):
        Optimizer.__init__(self, policy)
        self.stepsize = stepsize
        self.beta1 = beta1
        self.beta2 = beta2
        self.epsilon = epsilon
        self.m = np.zeros(self.dim, dtype=np.float32)
        self.v = np.zeros(self.dim, dtype=np.float32)

    def _compute_step(self, globalg):
        a = self.stepsize * (
            np.sqrt(1 - self.beta2 ** self.t) / (1 - self.beta1 ** self.t)
        )
        self.m = self.beta1 * self.m + (1 - self.beta1) * globalg
        self.v = self.beta2 * self.v + (1 - self.beta2) * (globalg * globalg)
        step = -a * self.m / (np.sqrt(self.v) + self.epsilon)
        return step
