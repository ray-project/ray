from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import ray
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.evaluation.policy_evaluator import PolicyEvaluator
from ray.rllib.evaluation.metrics import collect_episodes, summarize_episodes

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
    def __init__(self, local_evaluator, remote_evaluators=None, config=None):
        """Create an optimizer instance.

        Args:
            config (dict): Optimizer-specific arguments.
            local_evaluator (Evaluator): Local evaluator instance, required.
            remote_evaluators (list): A list of Ray actor handles to remote
                evaluators instances. If empty, the optimizer should fall back
                to using only the local evaluator.
        """
        self.local_evaluator = local_evaluator
        self.remote_evaluators = remote_evaluators or []
        self.episode_history = []
        self.config = config or {}
        self._init(**self.config)

        # Counters that should be updated by sub-classes
        self.num_steps_trained = 0
        self.num_steps_sampled = 0

        logger.debug("Created policy optimizer with {}: {}".format(
            config, self))

    @DeveloperAPI
    def _init(self):
        """Subclasses should prefer overriding this instead of __init__."""

        raise NotImplementedError

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
        remote_results = ray.get(
            [ev.apply.remote(func) for ev in self.remote_evaluators])
        return local_result + remote_results

    @DeveloperAPI
    def foreach_evaluator_with_index(self, func):
        """Apply the given function to each evaluator instance.

        The index will be passed as the second arg to the given function.
        """

        local_result = [func(self.local_evaluator, 0)]
        remote_results = ray.get([
            ev.apply.remote(func, i + 1)
            for i, ev in enumerate(self.remote_evaluators)
        ])
        return local_result + remote_results

    @classmethod
    def make(cls,
             env_creator,
             policy_graph,
             optimizer_batch_size=None,
             num_workers=0,
             num_envs_per_worker=None,
             optimizer_config=None,
             remote_num_cpus=None,
             remote_num_gpus=None,
             **eval_kwargs):
        """Creates an Optimizer with local and remote evaluators.

        Args:
            env_creator(func): Function that returns a gym.Env given an
                EnvContext wrapped configuration.
            policy_graph (class|dict): Either a class implementing
                PolicyGraph, or a dictionary of policy id strings to
                (PolicyGraph, obs_space, action_space, config) tuples.
                See PolicyEvaluator documentation.
            optimizer_batch_size (int): Batch size summed across all workers.
                Will override worker `batch_steps`.
            num_workers (int): Number of remote evaluators
            num_envs_per_worker (int): (Optional) Sets the number
                environments per evaluator for vectorization.
                If set, overrides `num_envs` in kwargs
                for PolicyEvaluator.__init__.
            optimizer_config (dict): Config passed to the optimizer.
            remote_num_cpus (int): CPU specification for remote evaluator.
            remote_num_gpus (int): GPU specification for remote evaluator.
            **eval_kwargs: PolicyEvaluator Class non-positional args.

        Returns:
            (Optimizer) Instance of `cls` with evaluators configured
                accordingly.
        """
        optimizer_config = optimizer_config or {}
        if num_envs_per_worker:
            assert num_envs_per_worker > 0, "Improper num_envs_per_worker!"
            eval_kwargs["num_envs"] = int(num_envs_per_worker)
        if optimizer_batch_size:
            assert optimizer_batch_size > 0
            if num_workers > 1:
                eval_kwargs["batch_steps"] = \
                    optimizer_batch_size // num_workers
            else:
                eval_kwargs["batch_steps"] = optimizer_batch_size
        evaluator = PolicyEvaluator(env_creator, policy_graph, **eval_kwargs)
        remote_cls = PolicyEvaluator.as_remote(remote_num_cpus,
                                               remote_num_gpus)
        remote_evaluators = [
            remote_cls.remote(env_creator, policy_graph, **eval_kwargs)
            for i in range(num_workers)
        ]

        return cls(evaluator, remote_evaluators, optimizer_config)
