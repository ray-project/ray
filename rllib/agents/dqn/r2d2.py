import logging
from typing import Type

from ray.rllib.agents.dqn import DQNTrainer, DEFAULT_CONFIG as DQN_DEFAULT_CONFIG
from ray.rllib.agents.dqn.r2d2_tf_policy import R2D2TFPolicy
from ray.rllib.agents.dqn.r2d2_torch_policy import R2D2TorchPolicy
from ray.rllib.agents.trainer import Trainer
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.utils.deprecation import DEPRECATED_VALUE

logger = logging.getLogger(__name__)

# fmt: off
# __sphinx_doc_begin__
R2D2_DEFAULT_CONFIG = Trainer.merge_trainer_configs(
    DQN_DEFAULT_CONFIG,  # See keys in dqn.py, which are also supported.
    {
        # Learning rate for adam optimizer.
        "lr": 1e-4,
        # Discount factor.
        "gamma": 0.997,
        # Train batch size (in number of single timesteps).
        "train_batch_size": 64,
        # Adam epsilon hyper parameter
        "adam_epsilon": 1e-3,
        # Run in parallel by default.
        "num_workers": 2,
        # Batch mode must be complete_episodes.
        "batch_mode": "complete_episodes",

        # === Replay buffer ===
        "replay_buffer_config": {
            "_enable_replay_buffer_api": True,
            "type": "MultiAgentReplayBuffer",
            # Size of the replay buffer (in sequences, not timesteps).
            "capacity": 100000,
            "storage_unit": "sequences",
            # Set automatically: The number
            # of contiguous environment steps to
            # replay at once. Will be calculated via
            # model->max_seq_len + burn_in.
            # Do not set this to any valid value!
            "replay_sequence_length": -1,
            # If > 0, use the `replay_burn_in` first steps of each replay-sampled
            # sequence (starting either from all 0.0-values if `zero_init_state=True` or
            # from the already stored values) to calculate an even more accurate
            # initial states for the actual sequence (starting after this burn-in
            # window). In the burn-in case, the actual length of the sequence
            # used for loss calculation is `n - replay_burn_in` time steps
            # (n=LSTM’s/attention net’s max_seq_len).
            "replay_burn_in": 0,
        },
        # If True, assume a zero-initialized state input (no matter where in
        # the episode the sequence is located).
        # If False, store the initial states along with each SampleBatch, use
        # it (as initial state when running through the network for training),
        # and update that initial state during training (from the internal
        # state outputs of the immediately preceding sequence).
        "zero_init_states": True,

        # Whether to use the h-function from the paper [1] to scale target
        # values in the R2D2-loss function:
        # h(x) = sign(x)(􏰅|x| + 1 − 1) + εx
        "use_h_function": True,
        # The epsilon parameter from the R2D2 loss function (only used
        # if `use_h_function`=True.
        "h_function_epsilon": 1e-3,

        # Update the target network every `target_network_update_freq` steps.
        "target_network_update_freq": 2500,

        # Deprecated keys:
        # Use config["replay_buffer_config"]["replay_burn_in"] instead
        "burn_in": DEPRECATED_VALUE
    },
    _allow_unknown_configs=True,
)
# __sphinx_doc_end__
# fmt: on


# Build an R2D2 trainer, which uses the framework specific Policy
# determined in `get_policy_class()` above.
class R2D2Trainer(DQNTrainer):
    """Recurrent Experience Replay in Distrib. Reinforcement Learning (R2D2).

    Trainer defining the distributed R2D2 algorithm.
    See `r2d2_[tf|torch]_policy.py` for the definition of the policies.

    [1] Recurrent Experience Replay in Distributed Reinforcement Learning -
        S Kapturowski, G Ostrovski, J Quan, R Munos, W Dabney - 2019, DeepMind


    Detailed documentation:
    https://docs.ray.io/en/master/rllib-algorithms.html#\
    recurrent-replay-distributed-dqn-r2d2
    """

    @classmethod
    @override(DQNTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return R2D2_DEFAULT_CONFIG

    @override(DQNTrainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            return R2D2TorchPolicy
        else:
            return R2D2TFPolicy

    @override(DQNTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        """Checks and updates the config based on settings.

        Rewrites rollout_fragment_length to take into account burn-in and
        max_seq_len truncation.
        """
        # Call super's validation method.
        super().validate_config(config)

        if config["replay_buffer_config"]["replay_sequence_length"] != -1:
            raise ValueError(
                "`replay_sequence_length` is calculated automatically to be "
                "model->max_seq_len + burn_in!"
            )
        # Add the `burn_in` to the Model's max_seq_len.
        # Set the replay sequence length to the max_seq_len of the model.
        config["replay_buffer_config"]["replay_sequence_length"] = (
            config["replay_buffer_config"]["replay_burn_in"]
            + config["model"]["max_seq_len"]
        )

        if config.get("batch_mode") != "complete_episodes":
            raise ValueError("`batch_mode` must be 'complete_episodes'!")
