from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.rllib import optimizers
from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.agents.marwil.marwil_policy_graph import MARWILPolicyGraph
from ray.rllib.utils.annotations import override
from ray.rllib.utils.schedules import ConstantSchedule, LinearSchedule

OPTIMIZER_SHARED_CONFIGS = [
    "learning_starts", "buffer_size", "prioritized_replay",
    "prioritized_replay_alpha", "prioritized_replay_beta",
    "schedule_max_timesteps", "beta_annealing_fraction",
    "final_prioritized_replay_beta", "prioritized_replay_eps",
    "train_batch_size", "sample_batch_size"
]

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": False,
    # GAE(lambda) parameter
    "lambda": 1.0,

    # === Model ===
    "actor_hiddens": [64],
    "critic_hiddens": [64],
    "beta": 0.1,
    "c": 3.0,

    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    "buffer_size": 50000,
    # If True prioritized replay buffer will be used.
    "prioritized_replay": False,
    # Alpha parameter for prioritized replay buffer.
    "prioritized_replay_alpha": 0.6,
    # Beta parameter for sampling from prioritized replay buffer.
    "prioritized_replay_beta": 0.4,
    # Fraction of entire training period over which the beta parameter is
    # annealed
    "beta_annealing_fraction": 0.2,
    # Final value of beta
    "final_prioritized_replay_beta": 0.4,
    # Epsilon to add to the TD errors when updating priorities.
    "prioritized_replay_eps": 1e-6,
    # Whether to LZ4 compress observations
    "compress_observations": True,

    # === Optimization ===
    # Learning rate for adam optimizer
    "lr": 1e-4,
    # If not None, clip gradients during optimization at this value
    "grad_norm_clipping": 40,
    # Update the replay buffer with this many samples at once. Note that
    # this setting applies per-worker if num_workers > 1.
    "sample_batch_size": 4,
    # Size of a batched sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 64,
    "timesteps_per_iteration": 1000,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 50000,

    # === Parallelism ===
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you"re using the Async or Ape-X optimizers.
    "num_workers": 0,
    # Optimizer class to use.
    "optimizer_class": "SyncReplayOptimizer",
    # Prevent iterations from going lower than this time span
    "min_iter_time_s": 1,
})
# __sphinx_doc_end__
# yapf: enable


class MARWILAgent(Agent):
    """MARWIL implementation in TensorFlow."""

    _agent_name = "MARWIL"
    _default_config = DEFAULT_CONFIG
    _policy_graph = MARWILPolicyGraph
    _optimizer_shared_configs = OPTIMIZER_SHARED_CONFIGS

    @override(Agent)
    def _init(self):
        for k in self._optimizer_shared_configs:
            if k not in self.config["optimizer"]:
                self.config["optimizer"][k] = self.config[k]

        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, self._policy_graph)

        def create_remote_evaluators():
            return self.make_remote_evaluators(self.env_creator,
                                               self._policy_graph,
                                               self.config["num_workers"])

        if self.config["optimizer_class"] != "AsyncReplayOptimizer":
            self.remote_evaluators = create_remote_evaluators()
        else:
            # Hack to workaround https://github.com/ray-project/ray/issues/2541
            self.remote_evaluators = None

        self.optimizer = getattr(optimizers, self.config["optimizer_class"])(
            self.local_evaluator, self.remote_evaluators,
            self.config["optimizer"])
        # Create the remote evaluators *after* the replay actors
        if self.remote_evaluators is None:
            self.remote_evaluators = create_remote_evaluators()
            self.optimizer._set_evaluators(self.remote_evaluators)

    @override(Agent)
    def _train(self):
        start_sampled_timestep = self.optimizer.num_steps_sampled
        start_trained_timestep = self.optimizer.num_steps_trained

        # Do optimization steps
        start = time.time()
        while (self.optimizer.num_steps_sampled - start_sampled_timestep <
               self.config["timesteps_per_iteration"] and \
               self.optimizer.num_steps_trained - start_trained_timestep <
               self.config["timesteps_per_iteration"]
               ) or time.time() - start < self.config["min_iter_time_s"]:
            self.optimizer.step()

        result = self.optimizer.collect_metrics(
            timeout_seconds=self.config["collect_metrics_timeout"])

        result.update(
            timesteps_this_iter=max(self.optimizer.num_steps_sampled-start_sampled_timestep, self.optimizer.num_steps_trained-start_trained_timestep),
            info=dict(**self.optimizer.stats()))
        return result
