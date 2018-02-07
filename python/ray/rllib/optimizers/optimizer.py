from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Optimizer(object):
    """RLlib optimizers encapsulate distributed RL optimization strategies.

    For example, AsyncOptimizer is used for A3C, and LocalMultiGPUOptimizer is
    used for PPO. These optimizers are all pluggable, and it is possible
    to mix and match as needed.

    In order for an algorithm to use an RLlib optimizer, it must implement
    the Evaluator interface and pass a number of Evaluators to its Optimizer
    of choice. The Optimizer uses these Evaluators to sample from the
    environment and compute model gradient updates.
    """

    def __init__(self, config, local_evaluator, remote_evaluators):
        """Create an optimizer instance.

        Args:
            config (dict): Optimizer-specific configuration data.
            local_evaluator (Evaluator): Local evaluator instance, required.
            remote_evaluators (list): A list of handles to remote evaluators.
                if empty, the optimizer should fall back to to using only the
                local evaluator.
        """
        self.config = config
        self.local_evaluator = local_evaluator
        self.remote_evaluators = remote_evaluators
        self._init()

    def _init(self):
        pass

    def step(self):
        """Takes a logical optimization step."""

        raise NotImplementedError

    def stats(self):
        """Returns a dictionary of internal performance statistics."""

        return {}
