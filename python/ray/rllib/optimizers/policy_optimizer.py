from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.evaluation.sample_batch import MultiAgentBatch


class PolicyOptimizer(object):
    """Policy optimizers encapsulate distributed RL optimization strategies.

    Policy optimizers serve as the "control plane" of algorithms.

    For example, AsyncOptimizer is used for A3C, and LocalMultiGPUOptimizer is
    used for PPO. These optimizers are all pluggable, and it is possible
    to mix and match as needed.

    In order for an algorithm to use an RLlib optimizer, it must implement
    the PolicyEvaluator interface and pass a PolicyEvaluator class or set of
    PolicyEvaluators to its PolicyOptimizer of choice. The PolicyOptimizer
    uses these Evaluators to sample from the environment and compute model
    gradient updates.

    Attributes:
        config (dict): The JSON configuration passed to this optimizer.
        local_evaluator (PolicyEvaluator): The embedded evaluator instance.
        remote_evaluators (list): List of remote evaluator replicas, or [].
        num_steps_trained (int): Number of timesteps trained on so far.
        num_steps_sampled (int): Number of timesteps sampled so far.
        evaluator_resources (dict): Optional resource requests to set for
            evaluators created by this optimizer.
    """

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
        """Subclasses should prefer overriding this instead of __init__."""

        pass

    def step(self):
        """Takes a logical optimization step.

        This should run for long enough to minimize call overheads (i.e., at
        least a couple seconds), but short enough to return control
        periodically to callers (i.e., at most a few tens of seconds).
        """

        raise NotImplementedError

    def stats(self):
        """Returns a dictionary of internal performance statistics."""

        return {
            "num_steps_trained": self.num_steps_trained,
            "num_steps_sampled": self.num_steps_sampled,
        }

    def save(self):
        """Returns a serializable object representing the optimizer state."""

        return [self.num_steps_trained, self.num_steps_sampled]

    def restore(self, data):
        """Restores optimizer state from the given data object."""

        self.num_steps_trained = data[0]
        self.num_steps_sampled = data[1]

    def foreach_evaluator(self, func):
        """Apply the given function to each evaluator instance."""

        local_result = [func(self.local_evaluator)]
        remote_results = ray.get(
            [ev.apply.remote(func) for ev in self.remote_evaluators])
        return local_result + remote_results

    def foreach_evaluator_with_index(self, func):
        """Apply the given function to each evaluator instance.

        The index will be passed as the second arg to the given function.
        """

        local_result = [func(self.local_evaluator, 0)]
        remote_results = ray.get(
            [ev.apply.remote(func, i + 1)
             for i, ev in enumerate(self.remote_evaluators)])
        return local_result + remote_results

    def _check_not_multiagent(self, sample_batch):
        if isinstance(sample_batch, MultiAgentBatch):
            raise NotImplementedError(
                "This optimizer does not support multi-agent yet.")
