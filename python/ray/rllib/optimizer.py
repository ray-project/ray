from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Optimizer(object):
    """RLlib optimizers provide a variety of SGD strategies for RL.

    For example, AsyncOptimizer is used for A3C, and LocalMultiGpuOptimizer is
    used for PPO. These optimizers are all pluggable however, it is possible
    to mix as match as needed.

    In order for an algorithm to use an RLlib optimizer, it must implement
    the Evaluator interface and pass a number of remote Evaluators to the
    Optimizer. The Optimizer uses these Evaluators to sample from the
    environment and compute model gradient updates.
    """

    def __init__(self, local_evaluator, remote_evaluators):
        self.local_evaluator = local_evaluator
        self.remote_evaluators = remote_evaluators

    def step(self):


# TODO(ekl) does this have to be provided by the evaluator
def _concat(samples):
    result = []
    for s in samples:
        result.extend(s)
    return result


class SyncLocalOptimizer(Optimizer):
    def step(self):
        samples = _concat(
            ray.get([e.sample.remote() for e in self.remote_evaluators]))
        grad = self.local_evaluator.gradients(samples)
        self.local_evaluator.apply(grad)
        weights = ray.put(self.local_evaluator.get_weights())
        for e in self.remote_evalutors:
            e.set_weights.remote(weights)
