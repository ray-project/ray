import logging
from typing import Optional, Type

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dqn import DQN, DQNConfig
from ray.rllib.algorithms.r2d2.r2d2_tf_policy import R2D2TFPolicy
from ray.rllib.algorithms.r2d2.r2d2_torch_policy import R2D2TorchPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.deprecation import DEPRECATED_VALUE

logger = logging.getLogger(__name__)


class R2D2Config(DQNConfig):
    r"""Defines a configuration class from which a R2D2 Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.r2d2.r2d2 import R2D2Config
        >>> config = R2D2Config()
        >>> print(config.h_function_epsilon)  # doctest: +SKIP
        >>> replay_config = config.replay_buffer_config.update(
        >>>     {
        >>>         "capacity": 1000000,
        >>>         "replay_burn_in": 20,
        >>>     }
        >>> )
        >>> config.training(replay_buffer_config=replay_config)\  # doctest: +SKIP
        >>>       .resources(num_gpus=1)\
        >>>       .rollouts(num_rollout_workers=30)\
        >>>       .environment("CartPole-v1")
        >>> algo = R2D2(config=config)  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.r2d2.r2d2 import R2D2Config
        >>> from ray import air
        >>> from ray import tune
        >>> config = R2D2Config()
        >>> config.training(train_batch_size=tune.grid_search([256, 64])
        >>> config.environment(env="CartPole-v1")
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "R2D2",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean":200}),
        ...     param_space=config.to_dict()
        ... ).fit()

    Example:
        >>> from ray.rllib.algorithms.r2d2.r2d2 import R2D2Config
        >>> config = R2D2Config()
        >>> print(config.exploration_config)  # doctest: +SKIP
        >>> explore_config = config.exploration_config.update(
        >>>     {
        >>>         "initial_epsilon": 1.0,
        >>>         "final_epsilon": 0.1,
        >>>         "epsilone_timesteps": 200000,
        >>>     }
        >>> )
        >>> config.training(lr_schedule=[[1, 1e-3, [500, 5e-3]])\
        >>>       .exploration(exploration_config=explore_config)

    Example:
        >>> from ray.rllib.algorithms.r2d2.r2d2 import R2D2Config
        >>> config = R2D2Config()
        >>> print(config.exploration_config)  # doctest: +SKIP
        >>> explore_config = config.exploration_config.update(
        >>>     {
        >>>         "type": "SoftQ",
        >>>         "temperature": [1.0],
        >>>     }
        >>> )
        >>> config.training(lr_schedule=[[1, 1e-3, [500, 5e-3]])\
        >>>       .exploration(exploration_config=explore_config)
    """

    def __init__(self, algo_class=None):
        """Initializes a ApexConfig instance."""
        super().__init__(algo_class=algo_class or R2D2)

        # fmt: off
        # __sphinx_doc_begin__
        # R2D2-specific settings:
        self.zero_init_states = True
        self.use_h_function = True
        self.h_function_epsilon = 1e-3

        # R2D2 settings overriding DQN ones:
        # .training()
        self.adam_epsilon = 1e-3
        self.lr = 1e-4
        self.gamma = 0.997
        self.train_batch_size = 1000
        self.target_network_update_freq = 1000
        self.training_intensity = 150
        # R2D2 is using a buffer that stores sequences.
        self.replay_buffer_config = {
            "type": "MultiAgentReplayBuffer",
            # Specify prioritized replay by supplying a buffer type that supports
            # prioritization, for example: MultiAgentPrioritizedReplayBuffer.
            "prioritized_replay": DEPRECATED_VALUE,
            # Size of the replay buffer (in sequences, not timesteps).
            "capacity": 100000,
            # This algorithm learns on sequences. We therefore require the replay buffer
            # to slice sampled batches into sequences before replay. How sequences
            # are sliced depends on the parameters `replay_sequence_length`,
            # `replay_burn_in`, and `replay_zero_init_states`.
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
        }

        # .rollouts()
        self.num_rollout_workers = 2
        self.batch_mode = "complete_episodes"

        # fmt: on
        # __sphinx_doc_end__

        self.burn_in = DEPRECATED_VALUE

    def training(
        self,
        *,
        zero_init_states: Optional[bool] = NotProvided,
        use_h_function: Optional[bool] = NotProvided,
        h_function_epsilon: Optional[float] = NotProvided,
        **kwargs,
    ) -> "R2D2Config":
        """Sets the training related configuration.

        Args:
            zero_init_states: If True, assume a zero-initialized state input (no
                matter where in the episode the sequence is located).
                If False, store the initial states along with each SampleBatch, use
                it (as initial state when running through the network for training),
                and update that initial state during training (from the internal
                state outputs of the immediately preceding sequence).
            use_h_function: Whether to use the h-function from the paper [1] to scale
                target values in the R2D2-loss function:
                h(x) = sign(x)(􏰅|x| + 1 − 1) + εx
            h_function_epsilon: The epsilon parameter from the R2D2 loss function (only
                used if `use_h_function`=True.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if zero_init_states is not NotProvided:
            self.zero_init_states = zero_init_states
        if use_h_function is not NotProvided:
            self.use_h_function = use_h_function
        if h_function_epsilon is not NotProvided:
            self.h_function_epsilon = h_function_epsilon

        return self

    @override(DQNConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if (
            not self.in_evaluation
            and self.replay_buffer_config.get("replay_sequence_length", -1) != -1
        ):
            raise ValueError(
                "`replay_sequence_length` is calculated automatically to be "
                "model->max_seq_len + burn_in!"
            )
        # Add the `burn_in` to the Model's max_seq_len.
        # Set the replay sequence length to the max_seq_len of the model.
        self.replay_buffer_config["replay_sequence_length"] = (
            self.replay_buffer_config["replay_burn_in"] + self.model["max_seq_len"]
        )

        if self.batch_mode != "complete_episodes":
            raise ValueError("`batch_mode` must be 'complete_episodes'!")


class R2D2(DQN):
    """Recurrent Experience Replay in Distrib. Reinforcement Learning (R2D2).

    Algorithm defining the distributed R2D2 algorithm.
    See `r2d2_[tf|torch]_policy.py` for the definition of the policies.

    [1] Recurrent Experience Replay in Distributed Reinforcement Learning -
        S Kapturowski, G Ostrovski, J Quan, R Munos, W Dabney - 2019, DeepMind


    Detailed documentation:
    https://docs.ray.io/en/master/rllib-algorithms.html#\
    recurrent-replay-distributed-dqn-r2d2
    """

    @classmethod
    @override(DQN)
    def get_default_config(cls) -> AlgorithmConfig:
        return R2D2Config()

    @classmethod
    @override(DQN)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return R2D2TorchPolicy
        else:
            return R2D2TFPolicy


# Deprecated: Use ray.rllib.algorithms.r2d2.r2d2.R2D2Config instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(R2D2Config().to_dict())

    @Deprecated(
        old="ray.rllib.agents.dqn.r2d2::R2D2_DEFAULT_CONFIG",
        new="ray.rllib.algorithms.r2d2.r2d2::R2D2Config(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


R2D2_DEFAULT_CONFIG = _deprecated_default_config()
