from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.rllib import optimizers
from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.agents.sac.sac_policy_graph import SACPolicyGraph
from ray.rllib.utils.annotations import override


OPTIMIZER_SHARED_CONFIGS = [
    # "buffer_size", "prioritized_replay", "prioritized_replay_alpha",
    # "prioritized_replay_beta", "prioritized_replay_eps", "sample_batch_size",
    # "train_batch_size", "learning_starts"
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

    # Number of env steps to optimize for before returning
    # Epochs in softlearning code
    "timesteps_per_iteration": 1000,
    # Update the target network every `target_update_interval` steps.
    "target_update_interval": 1,
    # Update the target by \tau * policy + (1-\tau) * target_policy
    "tau": 2e-3,

    # Target entropy lower bound. This is the inverse of reward scale,
    # and will be optimized automatically.
    "target_entropy": "auto",

    # === Replay buffer ===
    "replay_pool": {
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
    },

    # === Optimization ===
    "optimization": {
        # Learning rate for adam optimizer
        "policy_lr": 3e-4,
        "Q_lr": 3e-4,
        "entropy_lr": 3e-4,
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
    },

    # === Parallelism ===
    "parallelism": {
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
    }
})
# __sphinx_doc_end__
# yapf: enable


class SACAgent(Agent):
    """Soft Actor-Critic implementation in TensorFlow."""
    _agent_name = "SAC"
    _default_config = DEFAULT_CONFIG
    _policy_graph = SACPolicyGraph
    _optimizer_shared_configs = OPTIMIZER_SHARED_CONFIGS
