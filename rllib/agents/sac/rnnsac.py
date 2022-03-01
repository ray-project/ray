from typing import Type

from ray.rllib.agents.sac import SACTrainer, DEFAULT_CONFIG as SAC_DEFAULT_CONFIG
from ray.rllib.agents.sac.rnnsac_torch_policy import RNNSACTorchPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
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


class RNNSACTrainer(SACTrainer):
    @classmethod
    @override(SACTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(SACTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        if config["replay_sequence_length"] != -1:
            raise ValueError(
                "`replay_sequence_length` is calculated automatically to be "
                "model->max_seq_len + burn_in!"
            )
        # Add the `burn_in` to the Model's max_seq_len.
        # Set the replay sequence length to the max_seq_len of the model.
        config["replay_sequence_length"] = (
            config["burn_in"] + config["model"]["max_seq_len"]
        )

        if config["framework"] != "torch":
            raise ValueError(
                "Only `framework=torch` supported so far for RNNSACTrainer!"
            )

    @override(SACTrainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        return RNNSACTorchPolicy
