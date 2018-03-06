from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


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

    @classmethod
    def make(
            cls, evaluator_cls, evaluator_args, num_workers, optimizer_config):
        """Create evaluators and an optimizer instance using those evaluators.

        Args:
            evaluator_cls (class): Python class of the evaluators to create.
            evaluator_args (list): List of constructor args for the evaluators.
            num_workers (int): Number of remote evaluators to create in
                addition to a local evaluator. This can be zero or greater.
            optimizer_config (dict): Keyword arguments to pass to the
                optimizer class constructor.
        """

        local_evaluator = evaluator_cls(*evaluator_args)
        remote_cls = ray.remote(num_cpus=1)(evaluator_cls)
        remote_evaluators = [
            remote_cls.remote(*evaluator_args)
            for _ in range(num_workers)]
        return cls(optimizer_config, local_evaluator, remote_evaluators)

    def __init__(self, config, local_evaluator, remote_evaluators):
        """Create an optimizer instance.

        Args:
            config (dict): Optimizer-specific arguments.
            local_evaluator (Evaluator): Local evaluator instance, required.
            remote_evaluators (list): A list of Ray actor handles to remote
                evaluators instances. If empty, the optimizer should fall back
                to using only the local evaluator.
        """
        self.config = config
        self.local_evaluator = local_evaluator
        self.remote_evaluators = remote_evaluators
        self._init(**config)

        # Counters that should be updated by sub-classes
        self.num_steps_trained = 0
        self.num_steps_sampled = 0

    def _init(self):
        pass

    def step(self):
        """Takes a logical optimization step."""

        raise NotImplementedError

    def stats(self):
        """Returns a dictionary of internal performance statistics."""

        return {
            "num_steps_trained": self.num_steps_trained,
            "num_steps_sampled": self.num_steps_sampled,
        }

    def save(self):
        return [self.num_steps_trained, self.num_steps_sampled]

    def restore(self, data):
        self.num_steps_trained = data[0]
        self.num_steps_sampled = data[1]
