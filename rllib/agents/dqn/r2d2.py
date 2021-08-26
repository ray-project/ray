"""
Recurrent Experience Replay in Distributed Reinforcement Learning (R2D2)
========================================================================

[1] Recurrent Experience Replay in Distributed Reinforcement Learning -
    S Kapturowski, G Ostrovski, J Quan, R Munos, W Dabney - 2019, DeepMind

This file defines the distributed Trainer class for the R2D2
algorithm. See `r2d2_[tf|torch]_policy.py` for the definition of the policies.

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#recurrent-replay-distributed-dqn-r2d2
"""  # noqa: E501

import logging
from typing import List, Optional, Type

from ray.rllib.agents import dqn
from ray.rllib.agents.dqn.dqn import execution_plan
from ray.rllib.agents.dqn.r2d2_tf_policy import R2D2TFPolicy
from ray.rllib.agents.dqn.r2d2_torch_policy import R2D2TorchPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.typing import TrainerConfigDict

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = dqn.DQNTrainer.merge_trainer_configs(
    dqn.DEFAULT_CONFIG,  # See keys in impala.py, which are also supported.
    {
        # Learning rate for adam optimizer
        "lr": 1e-4,
        # Discount factor.
        "gamma": 0.997,
        # Train batch size (in number of single timesteps).
        "train_batch_size": 64 * 20,
        # Adam epsilon hyper parameter
        "adam_epsilon": 1e-3,
        # Run in parallel by default.
        "num_workers": 2,
        # Batch mode must be complete_episodes.
        "batch_mode": "complete_episodes",
        # R2D2 does not suport n-step > 1 yet!
        "n_step": 1,

        # If True, assume a zero-initialized state input (no matter where in
        # the episode the sequence is located).
        # If False, store the initial states along with each SampleBatch, use
        # it (as initial state when running through the network for training),
        # and update that initial state during training (from the internal
        # state outputs of the immediately preceding sequence).
        "zero_init_states": True,
        # If > 0, use the `burn_in` first steps of each replay-sampled sequence
        # (starting either from all 0.0-values if `zero_init_state=True` or
        # from the already stored values) to calculate an even more accurate
        # initial states for the actual sequence (starting after this burn-in
        # window). In the burn-in case, the actual length of the sequence
        # used for loss calculation is `n - burn_in` time steps
        # (n=LSTM’s/attention net’s max_seq_len).
        "burn_in": 0,

        # Whether to use the h-function from the paper [1] to scale target
        # values in the R2D2-loss function:
        # h(x) = sign(x)(􏰅|x| + 1 − 1) + εx
        "use_h_function": True,
        # The epsilon parameter from the R2D2 loss function (only used
        # if `use_h_function`=True.
        "h_function_epsilon": 1e-3,

        # === Hyperparameters from the paper [1] ===
        # Size of the replay buffer (in sequences, not timesteps).
        "buffer_size": 100000,
        # If True prioritized replay buffer will be used.
        # Note: Not supported yet by R2D2!
        "prioritized_replay": False,
        # Set automatically: The number of contiguous environment steps to
        # replay at once. Will be calculated via
        # model->max_seq_len + burn_in.
        # Do not set this to any valid value!
        "replay_sequence_length": -1,

        # Update the target network every `target_network_update_freq` steps.
        "target_network_update_freq": 2500,
    },
    _allow_unknown_configs=True,
)
# __sphinx_doc_end__
# yapf: enable


def validate_config(config: TrainerConfigDict) -> None:
    """Checks and updates the config based on settings.

    Rewrites rollout_fragment_length to take into account n_step truncation.
    """
    if config["replay_sequence_length"] != -1:
        raise ValueError(
            "`replay_sequence_length` is calculated automatically to be "
            "model->max_seq_len + burn_in!")
    # Add the `burn_in` to the Model's max_seq_len.
    # Set the replay sequence length to the max_seq_len of the model.
    config["replay_sequence_length"] = \
        config["burn_in"] + config["model"]["max_seq_len"]

    if config.get("prioritized_replay"):
        raise ValueError("Prioritized replay is not supported for R2D2 yet!")

    if config.get("batch_mode") != "complete_episodes":
        raise ValueError("`batch_mode` must be 'complete_episodes'!")

    if config["n_step"] > 1:
        raise ValueError("`n_step` > 1 not yet supported by R2D2!")


def calculate_rr_weights(config: TrainerConfigDict) -> List[float]:
    """Calculate the round robin weights for the rollout and train steps"""
    if not config["training_intensity"]:
        return [1, 1]
    # e.g., 32 / 4 -> native ratio of 8.0
    native_ratio = (
        config["train_batch_size"] / config["rollout_fragment_length"])
    # Training intensity is specified in terms of
    # (steps_replayed / steps_sampled), so adjust for the native ratio.
    weights = [1, config["training_intensity"] / native_ratio]
    return weights


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    """Policy class picker function. Class is chosen based on DL-framework.

    Args:
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        Optional[Type[Policy]]: The Policy class to use with R2D2Trainer.
            If None, use `default_policy` provided in build_trainer().
    """
    if config["framework"] == "torch":
        return R2D2TorchPolicy


# Build an R2D2 trainer, which uses the framework specific Policy
# determined in `get_policy_class()` above.
R2D2Trainer = dqn.DQNTrainer.with_updates(
    name="R2D2",
    default_policy=R2D2TFPolicy,
    default_config=DEFAULT_CONFIG,
    validate_config=validate_config,
    get_policy_class=get_policy_class,
    execution_plan=execution_plan,
)
