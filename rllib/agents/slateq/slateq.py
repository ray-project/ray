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
from typing import List, Optional, Type, Union

from ray.rllib.agents.dqn.dqn import DQNTrainer
from ray.rllib.agents.slateq.slateq_tf_policy import SlateQTFPolicy
from ray.rllib.agents.slateq.slateq_torch_policy import SlateQTorchPolicy
from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config

logger = logging.getLogger(__name__)


class SlateQConfig(TrainerConfig):
    """Defines a configuration class from which a SlateQTrainer can be built.

    Example:
        >>> config = SlateQConfig().training(lr=0.01).resources(num_gpus=1)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build(env="CartPole-v1")
        >>> trainer.train()

    Example:
        >>> from ray import tune
        >>> config = SlateQConfig()
        >>> # Print out some default values.
        >>> print(config.lr)
        ... 0.0004
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]))
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "SlateQ",
        ...     stop={"episode_reward_mean": 160.0},
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self):
        """Initializes a PGConfig instance."""
        super().__init__(trainer_class=SlateQTrainer)

        # fmt: off
        # __sphinx_doc_begin__

        # SlateQ specific settings:
        self.fcnet_hiddens_per_candidate = [256, 32]
        self.target_network_update_freq = 3200
        self.tau = 1.0
        self.use_huber = False
        self.huber_threshold = 1.0
        self.training_intensity = None
        self.lr_schedule = None
        self.lr_choice_model = 1e-3
        self.rmsprop_epsilon = 1e-5
        self.grad_clip = None
        self.learning_starts = 20000
        self.n_step = 1
        self.worker_side_prioritization = False

        # Override some of TrainerConfig's default values with SlateQ-specific values.
        self.exploration_config = {
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
        }
        self.num_workers = 0
        self.rollout_fragment_length = 4
        self.train_batch_size = 32
        self.lr = 0.00025
        self.min_sample_timesteps_per_reporting = 1000
        self.min_time_s_per_reporting = 1
        self.compress_observations = False
        self._disable_preprocessor_api = True
        # __sphinx_doc_end__
        # fmt: on

    @override(TrainerConfig)
    def training(
        self,
        *,
        fcnet_hiddens_per_candidate: Optional[List[int]] = None,
        target_network_update_freq: Optional[int] = None,
        tau: Optional[float] = None,
        use_huber: Optional[bool] = None,
        huber_threshold: Optional[float] = None,
        training_intensity: Optional[float] = None,
        lr_schedule: Optional[List[List[Union[int, float]]]] = None,
        lr_choice_model: Optional[bool] = None,
        rmsprop_epsilon: Optional[float] = None,
        grad_clip: Optional[float] = None,
        learning_starts: Optional[int] = None,
        n_step: Optional[int] = None,
        worker_side_prioritization: Optional[bool] = None,
        **kwargs,
    ) -> "SlateQConfig":
        """Sets the training related configuration.

        Args:
            fcnet_hiddens_per_candidate: Dense-layer setup for each the n (document)
                candidate Q-network stacks.
            target_network_update_freq: Update the target network every
                `target_network_update_freq` steps.
            tau: Update the target by \tau * policy + (1-\tau) * target_policy.
            use_huber: If True, use huber loss instead of squared loss for critic
                network. Conventionally, no need to clip gradients if using a huber
                loss.
            huber_threshold: The threshold for the Huber loss.
            training_intensity: If set, this will fix the ratio of replayed from a
                buffer and learned on timesteps to sampled from an environment and
                stored in the replay buffer timesteps. Otherwise, the replay will
                proceed at the native ratio determined by
                `(train_batch_size / rollout_fragment_length)`.
            lr_schedule: Learning rate schedule. In the format of
                [[timestep, lr-value], [timestep, lr-value], ...]
                Intermediary timesteps will be assigned to interpolated learning rate
                values. A schedule should normally start from timestep 0.
            lr_choice_model: Learning rate for adam optimizer for the user choice model.
                So far, only relevant/supported for framework=torch.
            rmsprop_epsilon: RMSProp epsilon hyperparameter.
            grad_clip: If not None, clip gradients during optimization at this value.
            learning_starts: How many steps of the model to sample before learning
                starts.
            n_step: N-step parameter for Q-learning.
            worker_side_prioritization: Whether to compute priorities for the replay
                buffer on the workers.
            **kwargs:

        Returns:
            This updated TrainerConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if fcnet_hiddens_per_candidate is None:
            self.fcnet_hiddens_per_candidate = fcnet_hiddens_per_candidate
        if target_network_update_freq is None:
            self.target_network_update_freq = target_network_update_freq
        if tau is None:
            self.tau = tau
        if use_huber is None:
            self.use_huber = use_huber
        if huber_threshold is None:
            self.huber_threshold = huber_threshold
        if training_intensity is None:
            self.training_intensity = training_intensity
        if lr_schedule is None:
            self.lr_schedule = lr_schedule
        if lr_choice_model is None:
            self.lr_choice_model = lr_choice_model
        if rmsprop_epsilon is None:
            self.rmsprop_epsilon = rmsprop_epsilon
        if grad_clip is None:
            self.grad_clip = grad_clip
        if learning_starts is None:
            self.learning_starts = learning_starts
        if n_step is None:
            self.n_step = n_step
        if worker_side_prioritization is None:
            self.worker_side_prioritization = worker_side_prioritization

        return self


# TODO: Wait for MAx's PR on how we solve Buffer configs.
DEFAULT_CONFIG = with_common_config({
    # === Exploration Settings ===
    # Switch to greedy actions in evaluation workers.
    # TODO: How do we do this?
    "evaluation_config": {
        "explore": False,
    },

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

})


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
