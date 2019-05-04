from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.evaluation.metrics import collect_episodes, summarize_episodes
from ray.rllib.utils.memory import ray_get_and_free

logger = logging.getLogger(__name__)


@DeveloperAPI
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

    @DeveloperAPI
    def __init__(self, local_evaluator, remote_evaluators=None):
        """Create an optimizer instance.

        Args:
            local_evaluator (Evaluator): Local evaluator instance, required.
            remote_evaluators (list): A list of Ray actor handles to remote
                evaluators instances. If empty, the optimizer should fall back
                to using only the local evaluator.
        """
        self.local_evaluator = local_evaluator
        self.remote_evaluators = remote_evaluators or []
        self.episode_history = []

        # Counters that should be updated by sub-classes
        self.num_steps_trained = 0
        self.num_steps_sampled = 0

    @DeveloperAPI
    def step(self):
        """Takes a logical optimization step.

        This should run for long enough to minimize call overheads (i.e., at
        least a couple seconds), but short enough to return control
        periodically to callers (i.e., at most a few tens of seconds).

        Returns:
            fetches (dict|None): Optional fetches from compute grads calls.
        """

        raise NotImplementedError

    @DeveloperAPI
    def stats(self):
        """Returns a dictionary of internal performance statistics."""

        return {
            "num_steps_trained": self.num_steps_trained,
            "num_steps_sampled": self.num_steps_sampled,
        }

    @DeveloperAPI
    def save(self):
        """Returns a serializable object representing the optimizer state."""

        return [self.num_steps_trained, self.num_steps_sampled]

    @DeveloperAPI
    def restore(self, data):
        """Restores optimizer state from the given data object."""

        self.num_steps_trained = data[0]
        self.num_steps_sampled = data[1]

    @DeveloperAPI
    def stop(self):
        """Release any resources used by this optimizer."""
        pass

    @DeveloperAPI
    def collect_metrics(self,
                        timeout_seconds,
                        min_history=100,
                        selected_evaluators=None):
        """Returns evaluator and optimizer stats.

        Arguments:
            timeout_seconds (int): Max wait time for a evaluator before
                dropping its results. This usually indicates a hung evaluator.
            min_history (int): Min history length to smooth results over.
            selected_evaluators (list): Override the list of remote evaluators
                to collect metrics from.

        Returns:
            res (dict): A training result dict from evaluator metrics with
                `info` replaced with stats from self.
        """
        episodes, num_dropped = collect_episodes(
            self.local_evaluator,
            selected_evaluators or self.remote_evaluators,
            timeout_seconds=timeout_seconds)
        orig_episodes = list(episodes)
        missing = min_history - len(episodes)
        if missing > 0:
            episodes.extend(self.episode_history[-missing:])
            assert len(episodes) <= min_history
        self.episode_history.extend(orig_episodes)
        self.episode_history = self.episode_history[-min_history:]
        res = summarize_episodes(episodes, orig_episodes, num_dropped)
        res.update(info=self.stats())
        return res

    @DeveloperAPI
    def reset(self, remote_evaluators):
        """Called to change the set of remote evaluators being used."""

        self.remote_evaluators = remote_evaluators

    @DeveloperAPI
    def foreach_evaluator(self, func):
        """Apply the given function to each evaluator instance."""

        local_result = [func(self.local_evaluator)]
        remote_results = ray_get_and_free(
            [ev.apply.remote(func) for ev in self.remote_evaluators])
        return local_result + remote_results

    @DeveloperAPI
    def foreach_evaluator_with_index(self, func):
        """Apply the given function to each evaluator instance.

        The index will be passed as the second arg to the given function.
        """

        local_result = [func(self.local_evaluator, 0)]
        remote_results = ray_get_and_free([
            ev.apply.remote(func, i + 1)
            for i, ev in enumerate(self.remote_evaluators)
        ])
        return local_result + remote_results
