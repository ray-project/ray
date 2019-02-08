from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.rllib import optimizers
from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.agents.dqn import DQNAgent
from ray.rllib.agents.sac.sac_policy_graph import SACPolicyGraph
from ray.rllib.utils.annotations import override


OPTIMIZER_SHARED_CONFIGS = [
    "buffer_size", "prioritized_replay", "prioritized_replay_alpha",
    "prioritized_replay_beta", "prioritized_replay_eps", "sample_batch_size",
    "train_batch_size", "learning_starts"
]


# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Model ===
    "policy": {
        "type": "GaussianLatentSpacePolicy",
        "kwargs": {
            "hidden_layer_sizes": (256, 256),
            "activation": "relu",
            "output_activation": "linear",
        }
    },
    "Q": {
        "type": "FeedforwardQ",
        "kwargs": {
            "hidden_layer_sizes": (256, 256),
            "activation": "relu",
            "output_activation": "linear",
        }
    },

    # === Evaluation ===
    # Evaluate with epsilon=0 every `evaluation_interval` training iterations.
    # The evaluation stats will be reported under the "evaluation" metric key.
    # Note that evaluation is currently not parallelized, and that for Ape-X
    # metrics are already only reported for the lowest epsilon workers.
    "evaluation_interval": None,
    # Number of episodes to run per evaluation period.
    "evaluation_num_episodes": 1,

    # === Exploration ===
    # Max num timesteps for annealing schedules. Exploration is annealed from
    # 1.0 to exploration_fraction over this number of timesteps scaled by
    # exploration_fraction
    "schedule_max_timesteps": 100000,
    # Number of env steps to optimize for before returning
    "timesteps_per_iteration": 1000,
    # Fraction of entire training period over which the exploration rate is
    # annealed
    "exploration_fraction": 0.0,
    # Final value of random action probability
    "exploration_final_eps": 0.00,
    # Whether to use a distribution of epsilons across workers for exploration.
    "per_worker_exploration": False,

    # Number of env steps to optimize for before returning
    # Epochs in softlearning code
    "timesteps_per_iteration": 1000,
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 1,
    # Update the target by \tau * policy + (1-\tau) * target_policy
    "tau": 5e-3,

    # Target entropy lower bound. This is the inverse of reward scale,
    # and will be optimized automatically.
    "target_entropy": "auto",

    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    "buffer_size": int(1e6),
    # If True prioritized replay buffer will be used.
    # TODO(hartikainen): Make sure this works or remove the option.
    "prioritized_replay": False,
    # Alpha parameter for prioritized replay buffer.
    "prioritized_replay_alpha": 0.6,
    # Beta parameter for sampling from prioritized replay buffer.
    "prioritized_replay_beta": 0.4,
    # Epsilon to add to the TD errors when updating priorities.
    "prioritized_replay_eps": 1e-6,
    # Whether to LZ4 compress observations
    "compress_observations": False,

    # === Optimization ===
    "optimization": {
        # Learning rate for adam optimizer. Note: SAC currently only uses
        # a single optimizer for all the three losses (policy, Q, and entropy).
        # The "learning rate" of these are controlled with the loss weights
        # below. TODO(hartikainen): I think these should eventually use their
        # own optimizers for two reasons: 1) controlling the learning rate
        # seems a more "standard" and direct way of controlling the learning
        # rates, and 2) sharing the optimizers decouples all the three models
        # to each other.
        'learning_rate': 3e-4,

        'policy_loss_weight': 1.0,
        'Q_loss_weight': 1.0,
        'entropy_loss_weight': 1.0,
    },
    # If not None, clip gradients during optimization at this value
    # TODO(hartikainen): Make sure this works or remove the option.
    "grad_norm_clipping": None,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 1500,
    # Update the replay buffer with this many samples at once. Note that this
    # setting applies per-worker if num_workers > 1.
    "sample_batch_size": 1,
    # Size of a batched sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 256,

    # === Parallelism ===
    # Whether to use a GPU for local optimization.
    "num_gpus": 0,
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you"re using the Async or Ape-X optimizers.
    "num_workers": 0,
    # Whether to allocate GPUs for workers (if > 0).
    "num_gpus_per_worker": 0,
    # Whether to allocate CPUs for workers (if > 0).
    "num_cpus_per_worker": 1,
    # Optimizer class to use.
    "optimizer_class": "SyncReplayOptimizer",
    # Whether to compute priorities on workers.
    "worker_side_prioritization": False,
    # Prevent iterations from going lower than this time span
    "min_iter_time_s": 1,
})
# __sphinx_doc_end__
# yapf: enable


class SACAgent(DQNAgent):
    """Soft Actor-Critic implementation in TensorFlow."""
    _agent_name = "SAC"
    _default_config = DEFAULT_CONFIG
    _policy_graph = SACPolicyGraph
    _optimizer_shared_configs = OPTIMIZER_SHARED_CONFIGS

    def __getstate__(self):
        raise NotImplementedError("TODO(hartikainen): Check this.")
        state = Agent.__getstate__(self)
        state.update({
            "num_target_updates": self.num_target_updates,
            "last_target_update_ts": self.last_target_update_ts,
        })
        return state

    def __setstate__(self, state):
        raise NotImplementedError("TODO(hartikainen): Check this.")
        Agent.__setstate__(self, state)
        self.num_target_updates = state["num_target_updates"]
        self.last_target_update_ts = state["last_target_update_ts"]
