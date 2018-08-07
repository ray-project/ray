from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.evaluation.policy_evaluator import PolicyEvaluator
from ray.rllib.evaluation.metrics import collect_metrics
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
        self.config = config or {}
        self._init(**self.config)

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

        Returns:
            fetches (dict|None): Optional fetches from compute grads calls.
        """

        raise NotImplementedError

    def stats(self):
        """Returns a dictionary of internal performance statistics."""

        return {
            "num_steps_trained": self.num_steps_trained,
            "num_steps_sampled": self.num_steps_sampled,
        }

    def collect_metrics(self):
        """Returns evaluator and optimizer stats.

        Returns:
            res (dict): A training result dict from evaluator metrics with
                `info` replaced with stats from self.
        """
        res = collect_metrics(self.local_evaluator, self.remote_evaluators)
        res.update(info=self.stats())
        return res

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
        remote_results = ray.get([
            ev.apply.remote(func, i + 1)
            for i, ev in enumerate(self.remote_evaluators)
        ])
        return local_result + remote_results

    @staticmethod
    def _check_not_multiagent(sample_batch):
        if isinstance(sample_batch, MultiAgentBatch):
            raise NotImplementedError(
                "This optimizer does not support multi-agent yet.")

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
