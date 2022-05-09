"""
SlateQ (Reinforcement Learning for Recommendation)
==================================================

This file defines the trainer class for the SlateQ algorithm from the
`"Reinforcement Learning for Slate-based Recommender Systems: A Tractable
Decomposition and Practical Methodology" <https://arxiv.org/abs/1905.12767>`_
paper.

See `slateq_torch_policy.py` for the definition of the policy. Currently, only
PyTorch is supported. The algorithm is written and tested for Google's RecSim
environment (https://github.com/google-research/recsim).
"""

import logging
from typing import List, Type

from ray.rllib.agents.dqn.dqn import DQNTrainer
from ray.rllib.agents.slateq.slateq_tf_policy import SlateQTFPolicy
from ray.rllib.agents.slateq.slateq_torch_policy import SlateQTorchPolicy
from ray.rllib.agents.trainer import with_common_config
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config

logger = logging.getLogger(__name__)


# fmt: off
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Model ===
    # Dense-layer setup for each the n (document) candidate Q-network stacks.
    "fcnet_hiddens_per_candidate": [256, 32],

    # === Exploration Settings ===
    "exploration_config": {
        # The Exploration class to use.
        # Must be SlateEpsilonGreedy or SlateSoftQ to handle the problem that
        # the action space of the policy is different from the space used inside
        # the exploration component.
        # E.g.: action_space=MultiDiscrete([5, 5]) <- slate-size=2, num-docs=5,
        # but action distribution is Categorical(5*4) -> all possible unique slates.
        "type": "SlateEpsilonGreedy",
        "warmup_timesteps": 20000,
        "epsilon_timesteps": 250000,
        "final_epsilon": 0.01,
    },
    # Switch to greedy actions in evaluation workers.
    "evaluation_config": {
        "explore": False,
    },

    # Minimum env sampling timesteps to accumulate within a single `train()` call. This
    # value does not affect learning, only the number of times `Trainer.step_attempt()`
    # is called by `Trauber.train()`. If - after one `step_attempt()`, the env sampling
    # timestep count has not been reached, will perform n more `step_attempt()` calls
    # until the minimum timesteps have been executed. Set to 0 for no minimum timesteps.
    "min_sample_timesteps_per_reporting": 1000,
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 3200,
    # Update the target by \tau * policy + (1-\tau) * target_policy.
    "tau": 1.0,

    # If True, use huber loss instead of squared loss for critic network
    # Conventionally, no need to clip gradients if using a huber loss
    "use_huber": False,
    # Threshold of the huber loss.
    "huber_threshold": 1.0,

    # === Replay buffer ===
    "replay_buffer_config": {
        # Enable the new ReplayBuffer API.
        "_enable_replay_buffer_api": True,
        "type": "MultiAgentPrioritizedReplayBuffer",
        "capacity": 100000,
        "prioritized_replay_alpha": 0.6,
        # Beta parameter for sampling from prioritized replay buffer.
        "prioritized_replay_beta": 0.4,
        # Epsilon to add to the TD errors when updating priorities.
        "prioritized_replay_eps": 1e-6,
        # The number of continuous environment steps to replay at once. This may
        # be set to greater than 1 to support recurrent models.
        "replay_sequence_length": 1,
    },
    # Whether to LZ4 compress observations
    "compress_observations": False,
    # If set, this will fix the ratio of replayed from a buffer and learned on
    # timesteps to sampled from an environment and stored in the replay buffer
    # timesteps. Otherwise, the replay will proceed at the native ratio
    # determined by (train_batch_size / rollout_fragment_length).
    "training_intensity": None,

    # === Optimization ===
    # Learning rate for RMSprop optimizer for the q-model.
    "lr": 0.00025,
    # Learning rate schedule.
    # In the format of [[timestep, value], [timestep, value], ...]
    # A schedule should normally start from timestep 0.
    "lr_schedule": None,
    # Learning rate for adam optimizer for the user choice model.
    "lr_choice_model": 1e-3,  # Only relevant for framework=torch.

    # RMSProp epsilon hyper parameter.
    "rmsprop_epsilon": 1e-5,
    # If not None, clip gradients during optimization at this value
    "grad_clip": None,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 20000,
    # Update the replay buffer with this many samples at once. Note that
    # this setting applies per-worker if num_workers > 1.
    "rollout_fragment_length": 4,
    # Size of a batch sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 32,
    # N-step Q learning
    "n_step": 1,

    # === Parallelism ===
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you"re using the Async or Ape-X optimizers.
    "num_workers": 0,
    # Whether to compute priorities on workers.
    "worker_side_prioritization": False,
    # Prevent reporting frequency from going lower than this time span.
    "min_time_s_per_reporting": 1,

    # Switch on no-preprocessors for easier Q-model coding.
    "_disable_preprocessor_api": True,

    # Deprecated keys:
    # Use `capacity` in `replay_buffer_config` instead.
    "buffer_size": DEPRECATED_VALUE,
    # Use `replay_sequence_length` in `replay_buffer_config` instead.
    "replay_sequence_length": DEPRECATED_VALUE,
})
# __sphinx_doc_end__
# fmt: on


def calculate_round_robin_weights(config: TrainerConfigDict) -> List[float]:
    """Calculate the round robin weights for the rollout and train steps"""
    if not config["training_intensity"]:
        return [1, 1]
    # e.g., 32 / 4 -> native ratio of 8.0
    native_ratio = config["train_batch_size"] / config["rollout_fragment_length"]
    # Training intensity is specified in terms of
    # (steps_replayed / steps_sampled), so adjust for the native ratio.
    weights = [1, config["training_intensity"] / native_ratio]
    return weights


class SlateQTrainer(DQNTrainer):
    @classmethod
    @override(DQNTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(DQNTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        super().validate_config(config)
        validate_buffer_config(config)

    @override(DQNTrainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            return SlateQTorchPolicy
        else:
            return SlateQTFPolicy
