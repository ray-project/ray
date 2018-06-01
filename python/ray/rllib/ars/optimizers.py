# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

# OPTIMIZERS FOR MINIMIZING OBJECTIVES
class Optimizer(object):
    def __init__(self, w_policy):
        self.w_policy = w_policy.flatten()
        self.dim = w_policy.size
        self.t = 0

    def update(self, globalg):
        self.t += 1
        step = self._compute_step(globalg)
        ratio = np.linalg.norm(step) / (np.linalg.norm(self.w_policy) + 1e-5)
        return self.w_policy + step, ratio

    def _compute_step(self, globalg):
        raise NotImplementedError


class SGD(Optimizer):
    def __init__(self, pi, stepsize):
        Optimizer.__init__(self, pi)
        self.stepsize = stepsize

    def _compute_step(self, globalg):
        step = -self.stepsize * globalg
        return step