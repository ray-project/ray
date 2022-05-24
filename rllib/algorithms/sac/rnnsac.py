from typing import Type, Optional

from ray.rllib.algorithms.sac import (
    SACTrainer,
    SACConfig,
)
from ray.rllib.algorithms.sac.rnnsac_torch_policy import RNNSACTorchPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.utils.deprecation import DEPRECATED_VALUE, Deprecated


class RNNSACConfig(SACConfig):
    """Defines a configuration class from which an RNNSACTrainer can be built.

    Example:
        >>> config = RNNSACConfig().training(gamma=0.9, lr=0.01)\
        ...     .resources(num_gpus=0)\
        ...     .rollouts(num_rollout_workers=4)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build(env="CartPole-v1")
        >>> trainer.train()
    """

    def __init__(self, trainer_class=None):
        super().__init__(trainer_class=trainer_class or RNNSACTrainer)
        # fmt: off
        # __sphinx_doc_begin__
        self.batch_mode = "complete_episodes"
        self.zero_init_states = True
        self.replay_buffer_config = {
            # This algorithm learns on sequences. We therefore require the replay buffer
            # to slice sampled batches into sequences before replay. How sequences
            # are sliced depends on the parameters `replay_sequence_length`,
            # `replay_burn_in`, and `replay_zero_init_states`.
            "storage_unit": "sequences",
            # If > 0, use the `burn_in` first steps of each replay-sampled sequence
            # (starting either from all 0.0-values if `zero_init_state=True` or
            # from the already stored values) to calculate an even more accurate
            # initial states for the actual sequence (starting after this burn-in
            # window). In the burn-in case, the actual length of the sequence
            # used for loss calculation is `n - burn_in` time steps
            # (n=LSTM’s/attention net’s max_seq_len).
            "replay_burn_in": 0,
            # Set automatically: The number of contiguous environment steps to
            # replay at once. Will be calculated via
            # model->max_seq_len + burn_in.
            # Do not set this to any valid value!
            "replay_sequence_length": -1,
        },
        self.burn_in = DEPRECATED_VALUE

        # fmt: on
        # __sphinx_doc_end__

    @override(SACConfig)
    def training(
        self,
        *,
        zero_init_states: Optional[bool] = None,
        **kwargs,
    ) -> "RNNSACConfig":
        """Sets the training related configuration.

        Args:
            zero_init_states: If True, assume a zero-initialized state input (no matter
                where in the episode the sequence is located).
                If False, store the initial states along with each SampleBatch, use
                it (as initial state when running through the network for training),
                and update that initial state during training (from the internal
                state outputs of the immediately preceding sequence).

        Returns:
            This updated TrainerConfig object.
        """
        super().training(**kwargs)
        if zero_init_states is not None:
            self.zero_init_states = zero_init_states

        return self


class RNNSACTrainer(SACTrainer):
    @classmethod
    @override(SACTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return RNNSACConfig().to_dict()

    @override(SACTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        # Add the `burn_in` to the Model's max_seq_len.
        replay_sequence_length = (
            config["replay_buffer_config"]["replay_burn_in"]
            + config["model"]["max_seq_len"]
        )
        # Check if user tries to set replay_sequence_length (to anything
        # other than the proper value)
        if config["replay_buffer_config"].get("replay_sequence_length", None) not in [
            None,
            -1,
            replay_sequence_length,
        ]:
            raise ValueError(
                "`replay_sequence_length` is calculated automatically to be "
                "config['model']['max_seq_len'] + config['burn_in']. Leave "
                "config['replay_sequence_length'] blank to avoid this error."
            )
        # Set the replay sequence length to the max_seq_len of the model.
        config["replay_buffer_config"][
            "replay_sequence_length"
        ] = replay_sequence_length

        if config["framework"] != "torch":
            raise ValueError(
                "Only `framework=torch` supported so far for RNNSACTrainer!"
            )

    @override(SACTrainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        return RNNSACTorchPolicy


class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(RNNSACConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.sac.rnnsac.DEFAULT_CONFIG",
        new="ray.rllib.algorithms.sac.rnnsac.RNNSACConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
