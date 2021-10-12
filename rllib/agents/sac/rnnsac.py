from typing import Optional, Type

from ray.rllib.agents.sac import SACTrainer, \
                                 DEFAULT_CONFIG as SAC_DEFAULT_CONFIG
from ray.rllib.agents.sac.rnnsac_torch_policy import RNNSACTorchPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.typing import TrainerConfigDict

DEFAULT_CONFIG = SACTrainer.merge_trainer_configs(
    SAC_DEFAULT_CONFIG,
    {
        # Batch mode (see common config)
        "batch_mode": "complete_episodes",
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
        # Set automatically: The number of contiguous environment steps to
        # replay at once. Will be calculated via
        # model->max_seq_len + burn_in.
        # Do not set this to any valid value!
        "replay_sequence_length": -1,
    },
    _allow_unknown_configs=True,
)


def validate_config(config: TrainerConfigDict) -> None:
    if config["replay_sequence_length"] != -1:
        raise ValueError(
            "`replay_sequence_length` is calculated automatically to be "
            "model->max_seq_len + burn_in!")
    # Add the `burn_in` to the Model's max_seq_len.
    # Set the replay sequence length to the max_seq_len of the model.
    config["replay_sequence_length"] = \
        config["burn_in"] + config["model"]["max_seq_len"]


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    """Policy class picker function. Class is chosen based on DL-framework.

    Args:
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        Optional[Type[Policy]]: The Policy class to use with PPOTrainer.
            If None, use `default_policy` provided in build_trainer().
    """
    if config["framework"] == "torch":
        return RNNSACTorchPolicy


RNNSACTrainer = SACTrainer.with_updates(
    name="RNNSACTrainer",
    default_policy=RNNSACTorchPolicy,
    get_policy_class=get_policy_class,
    default_config=DEFAULT_CONFIG,
    validate_config=validate_config,
)

RNNSACTrainer._allow_unknown_subkeys += ["policy_model", "Q_model"]
