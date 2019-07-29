from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.evaluation.metrics import collect_episodes, summarize_episodes

logger = logging.getLogger(__name__)


@DeveloperAPI
class PolicyOptimizer(object):
    """Policy optimizers encapsulate distributed RL optimization strategies.

    Policy optimizers serve as the "control plane" of algorithms.

    For example, AsyncOptimizer is used for A3C, and LocalMultiGPUOptimizer is
    used for PPO. These optimizers are all pluggable, and it is possible
    to mix and match as needed.

    Attributes:
        config (dict): The JSON configuration passed to this optimizer.
        workers (WorkerSet): The set of rollout workers to use.
        num_steps_trained (int): Number of timesteps trained on so far.
        num_steps_sampled (int): Number of timesteps sampled so far.
    """

    @DeveloperAPI
    def __init__(self, workers):
        """Create an optimizer instance.

        Args:
            workers (WorkerSet): The set of rollout workers to use.
        """
        self.workers = workers
        self.episode_history = []
        self.to_be_collected = []

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
                        selected_workers=None):
        """Returns worker and optimizer stats.

        Arguments:
            timeout_seconds (int): Max wait time for a worker before
                dropping its results. This usually indicates a hung worker.
            min_history (int): Min history length to smooth results over.
            selected_workers (list): Override the list of remote workers
                to collect metrics from.

        Returns:
            res (dict): A training result dict from worker metrics with
                `info` replaced with stats from self.
        """
        episodes, self.to_be_collected = collect_episodes(
            self.workers.local_worker(),
            selected_workers or self.workers.remote_workers(),
            self.to_be_collected,
            timeout_seconds=timeout_seconds)
        orig_episodes = list(episodes)
        missing = min_history - len(episodes)
        if missing > 0:
            episodes.extend(self.episode_history[-missing:])
            assert len(episodes) <= min_history
        self.episode_history.extend(orig_episodes)
        self.episode_history = self.episode_history[-min_history:]
        res = summarize_episodes(episodes, orig_episodes)
        res.update(info=self.stats())
        return res

    @DeveloperAPI
    def reset(self, remote_workers):
        """Called to change the set of remote workers being used."""
        self.workers.reset(remote_workers)

    @DeveloperAPI
    def foreach_worker(self, func):
        """Apply the given function to each worker instance."""
        return self.workers.foreach_worker(func)

    @DeveloperAPI
    def foreach_worker_with_index(self, func):
        """Apply the given function to each worker instance.

        The index will be passed as the second arg to the given function.
        """
        return self.workers.foreach_worker_with_index(func)
