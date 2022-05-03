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
from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_ops import Replay, StoreToReplayBuffer
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.train_ops import (
    MultiGPUTrainOneStep,
    TrainOneStep,
    UpdateTargetNetwork,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.typing import TrainerConfigDict
from ray.util.iter import LocalIterator
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



DEFAULT_CONFIG = with_common_config({
    # === Model ===
    # Dense-layer setup for each the n (document) candidate Q-network stacks.
    "fcnet_hiddens_per_candidate": [256, 32],

    # === Exploration Settings ===
    # Switch to greedy actions in evaluation workers.
    # TODO: How do we do this?
    "evaluation_config": {
        "explore": False,
    },

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
    # If set, this will fix the ratio of replayed from a buffer and learned on
    # timesteps to sampled from an environment and stored in the replay buffer
    # timesteps. Otherwise, the replay will proceed at the native ratio
    # determined by (train_batch_size / rollout_fragment_length).
    "training_intensity": None,

    # === Optimization ===
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
    # N-step Q learning
    "n_step": 1,

    # === Parallelism ===
    # Whether to compute priorities on workers.
    "worker_side_prioritization": False,

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

    @staticmethod
    @override(DQNTrainer)
    def execution_plan(
        workers: WorkerSet, config: TrainerConfigDict, **kwargs
    ) -> LocalIterator[dict]:
        assert (
            "local_replay_buffer" in kwargs
        ), "SlateQ execution plan requires a local replay buffer."

        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        # We execute the following steps concurrently:
        # (1) Generate rollouts and store them in our local replay buffer.
        # Calling next() on store_op drives this.
        store_op = rollouts.for_each(
            StoreToReplayBuffer(local_buffer=kwargs["local_replay_buffer"])
        )

        if config["simple_optimizer"]:
            train_step_op = TrainOneStep(workers)
        else:
            train_step_op = MultiGPUTrainOneStep(
                workers=workers,
                sgd_minibatch_size=config["train_batch_size"],
                num_sgd_iter=1,
                num_gpus=config["num_gpus"],
                _fake_gpus=config["_fake_gpus"],
            )

        # (2) Read and train on experiences from the replay buffer. Every batch
        # returned from the LocalReplay() iterator is passed to TrainOneStep to
        # take a SGD step.
        replay_op = (
            Replay(local_buffer=kwargs["local_replay_buffer"])
            .for_each(train_step_op)
            .for_each(
                UpdateTargetNetwork(workers, config["target_network_update_freq"])
            )
        )

        # Alternate deterministically between (1) and (2). Only return the
        # output of (2) since training metrics are not available until (2)
        # runs.
        train_op = Concurrently(
            [store_op, replay_op],
            mode="round_robin",
            output_indexes=[1],
            round_robin_weights=calculate_round_robin_weights(config),
        )

        return StandardMetricsReporting(train_op, workers, config)
