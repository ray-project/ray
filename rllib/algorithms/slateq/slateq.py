"""
SlateQ (Reinforcement Learning for Recommendation)
==================================================

This file defines the algorithm class for the SlateQ algorithm from the
`"Reinforcement Learning for Slate-based Recommender Systems: A Tractable
Decomposition and Practical Methodology" <https://arxiv.org/abs/1905.12767>`_
paper.

See `slateq_torch_policy.py` for the definition of the policy. Currently, only
PyTorch is supported. The algorithm is written and tested for Google's RecSim
environment (https://github.com/google-research/recsim).
"""

import logging
from typing import Any, Dict, List, Optional, Type, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dqn.dqn import DQN
from ray.rllib.algorithms.slateq.slateq_tf_policy import SlateQTFPolicy
from ray.rllib.algorithms.slateq.slateq_torch_policy import SlateQTorchPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)

logger = logging.getLogger(__name__)


class SlateQConfig(AlgorithmConfig):
    """Defines a configuration class from which a SlateQ Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.slateq import SlateQConfig
        >>> config = SlateQConfig().training(lr=0.01).resources(num_gpus=1)
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.slateq import SlateQConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = SlateQConfig()
        >>> # Print out some default values.
        >>> print(config.lr)  # doctest: +SKIP
        >>> # Update the config object.
        >>> config = config.training(  # doctest: +SKIP
        ...     lr=tune.grid_search([0.001, 0.0001]))
        >>> # Set the config object's env.
        >>> config = config.environment(env="CartPole-v1")  # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "SlateQ",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 160.0}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self):
        """Initializes a PGConfig instance."""
        super().__init__(algo_class=SlateQ)

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
        self.n_step = 1
        self.replay_buffer_config = {
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
            # Whether to compute priorities on workers.
            "worker_side_prioritization": False,
        }
        # Number of timesteps to collect from rollout workers before we start
        # sampling from replay buffers for learning. Whether we count this in agent
        # steps  or environment steps depends on config.multi_agent(count_steps_by=..).
        self.num_steps_sampled_before_learning_starts = 20000

        # Override some of AlgorithmConfig's default values with SlateQ-specific values.
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
        # Switch to greedy actions in evaluation workers.
        self.evaluation_config = {"explore": False}
        self.rollout_fragment_length = 4
        self.train_batch_size = 32
        self.lr = 0.00025
        self.min_sample_timesteps_per_iteration = 1000
        self.min_time_s_per_iteration = 1
        self.compress_observations = False
        self._disable_preprocessor_api = True
        # Switch to greedy actions in evaluation workers.
        self.evaluation(evaluation_config=AlgorithmConfig.overrides(explore=False))
        # __sphinx_doc_end__
        # fmt: on

        # Deprecated config keys.
        self.learning_starts = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        replay_buffer_config: Optional[Dict[str, Any]] = NotProvided,
        fcnet_hiddens_per_candidate: Optional[List[int]] = NotProvided,
        target_network_update_freq: Optional[int] = NotProvided,
        tau: Optional[float] = NotProvided,
        use_huber: Optional[bool] = NotProvided,
        huber_threshold: Optional[float] = NotProvided,
        training_intensity: Optional[float] = NotProvided,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        lr_choice_model: Optional[bool] = NotProvided,
        rmsprop_epsilon: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        n_step: Optional[int] = NotProvided,
        num_steps_sampled_before_learning_starts: Optional[int] = NotProvided,
        **kwargs,
    ) -> "SlateQConfig":
        """Sets the training related configuration.

        Args:
            replay_buffer_config: The config dict to specify the replay buffer used.
                May contain a `type` key (default: `MultiAgentPrioritizedReplayBuffer`)
                indicating the class being used. All other keys specify the names
                and values of kwargs passed to to this class' constructor.
            fcnet_hiddens_per_candidate: Dense-layer setup for each the n (document)
                candidate Q-network stacks.
            target_network_update_freq: Update the target network every
                `target_network_update_freq` sample steps.
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
            n_step: N-step parameter for Q-learning.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if replay_buffer_config is not NotProvided:
            self.replay_buffer_config.update(replay_buffer_config)
        if fcnet_hiddens_per_candidate is not NotProvided:
            self.fcnet_hiddens_per_candidate = fcnet_hiddens_per_candidate
        if target_network_update_freq is not NotProvided:
            self.target_network_update_freq = target_network_update_freq
        if tau is not NotProvided:
            self.tau = tau
        if use_huber is not NotProvided:
            self.use_huber = use_huber
        if huber_threshold is not NotProvided:
            self.huber_threshold = huber_threshold
        if training_intensity is not NotProvided:
            self.training_intensity = training_intensity
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if lr_choice_model is not NotProvided:
            self.lr_choice_model = lr_choice_model
        if rmsprop_epsilon is not NotProvided:
            self.rmsprop_epsilon = rmsprop_epsilon
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        if n_step is not NotProvided:
            self.n_step = n_step
        if num_steps_sampled_before_learning_starts is not NotProvided:
            self.num_steps_sampled_before_learning_starts = (
                num_steps_sampled_before_learning_starts
            )

        return self


def calculate_round_robin_weights(config: AlgorithmConfig) -> List[float]:
    """Calculate the round robin weights for the rollout and train steps"""
    if not config["training_intensity"]:
        return [1, 1]
    # e.g., 32 / 4 -> native ratio of 8.0
    native_ratio = config["train_batch_size"] / config["rollout_fragment_length"]
    # Training intensity is specified in terms of
    # (steps_replayed / steps_sampled), so adjust for the native ratio.
    weights = [1, config["training_intensity"] / native_ratio]
    return weights


@Deprecated(
    old="rllib/algorithms/slate_q/",
    new="rllib_contrib/slate_q/",
    help=ALGO_DEPRECATION_WARNING,
    error=False,
)
class SlateQ(DQN):
    @classmethod
    @override(DQN)
    def get_default_config(cls) -> AlgorithmConfig:
        return SlateQConfig()

    @classmethod
    @override(DQN)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return SlateQTorchPolicy
        else:
            return SlateQTFPolicy
