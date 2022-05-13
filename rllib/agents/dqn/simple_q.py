"""
Simple Q-Learning
=================

This module provides a basic implementation of the DQN algorithm without any
optimizations.

This file defines the distributed Trainer class for the Simple Q algorithm.
See `simple_q_[tf|torch]_policy.py` for the definition of the policy loss.
"""

import logging
from typing import List, Optional, Type, Union

from ray.rllib.agents.dqn.simple_q_tf_policy import SimpleQTFPolicy
from ray.rllib.agents.dqn.simple_q_torch_policy import SimpleQTorchPolicy
from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.agents.trainer import Trainer
from ray.rllib.utils.metrics import SYNCH_WORKER_WEIGHTS_TIMER
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    train_one_step,
    multi_gpu_train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    TARGET_NET_UPDATE_TIMER,
)
from ray.rllib.utils.typing import (
    ResultDict,
    TrainerConfigDict,
)
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_TARGET_UPDATES,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.deprecation import Deprecated, DEPRECATED_VALUE

logger = logging.getLogger(__name__)


class SimpleQConfig(TrainerConfig):
    """Defines a SimpleQTrainer configuration class from which a SimpleQTrainer can
    be built.

    Example:
        >>> from ray.rllib.agents.dqn import SimpleQConfig
        >>> config = SimpleQConfig()
        >>> print(config.replay_buffer_config)
        >>> replay_config = config.replay_buffer_config.update(
        >>>     {
        >>>         "capacity":  40000,
        >>>         "replay_batch_size": 64,
        >>>     }
        >>> )
        >>> config.training(replay_buffer_config=replay_config)\
        ...       .resources(num_gpus=1)\
        ...       .rollouts(num_rollout_workers=3)

    Example:
        >>> from ray.rllib.agents.dqn import SimpleQConfig
        from ray import tune
        >>> config = SimpleQConfig()
        >>> config.training(adam_epsilon=tune.grid_search([1e-8, 5e-8, 1e-7])
        >>> config.environment(env="CartPole-v1")
        >>> tune.run(
        >>>     "SimpleQ",
        >>>     stop={"episode_reward_mean": 200},
        >>>     config=config.to_dict()
        >>> )

    Example:
        >>> from ray.rllib.agents.dqn import SimpleQConfig
        >>> config = SimpleQConfig()
        >>> print(config.exploration_config)
        >>> explore_config = config.exploration_config.update(
        >>>     {
        >>>         "initial_epsilon": 1.5,
        >>>         "final_epsilon": 0.01,
        >>>         "epsilon_timesteps": 5000,
        >>>     })
        >>> config = SimpleQConfig().rollouts(rollout_fragment_length=32)\
        >>>                         .exploration(exploration_config=explore_config)\
        >>>                         .training(learning_starts=200)

    Example:
        >>> from ray.rllib.agents.dqn import SimpleQConfig
        >>> config = SimpleQConfig()
        >>> print(config.exploration_config)
        >>> explore_config = config.exploration_config.update(
        >>>     {
        >>>         "type": "softq",
        >>>         "temperature": [1.0],
        >>>     })
        >>> config = SimpleQConfig().training(lr_schedule=[[1, 1e-3], [500, 5e-3]])\
        >>>                         .exploration(exploration_config=explore_config)
    """

    def __init__(self):
        """Initializes a SimpleQConfig instance."""
        super().__init__(trainer_class=SimpleQTrainer)

        # Simple Q specific
        # fmt: off
        # __sphinx_doc_begin__
        self.target_network_update_freq = 500
        self.replay_buffer_config = {
            "_enable_replay_buffer_api": True,
            "learning_starts": 1000,
            "type": "MultiAgentReplayBuffer",
            "capacity": 50000,
            "replay_batch_size": 32,
            "replay_sequence_length": 1,
        }
        self.store_buffer_in_checkpoints = False
        self.lr_schedule = None
        self.adam_epsilon = 1e-8
        self.grad_clip = 40
        # __sphinx_doc_end__
        # fmt: on

        # Overrides of TrainerConfig defaults
        # `rollouts()`
        self.num_workers = 0
        self.rollout_fragment_length = 4

        # `training()`
        self.lr = 5e-4
        self.train_batch_size = 32

        # `exploration()`
        self.exploration_config = {
            "type": "EpsilonGreedy",
            "initial_epsilon": 1.0,
            "final_epsilon": 0.02,
            "epsilon_timesteps": 10000,
        }

        # `evaluation()`
        self.evaluation_config = {"explore": False}

        # `reporting()`
        self.min_time_s_per_reporting = 1
        self.min_sample_timesteps_per_reporting = 1000

        # Deprecated.
        self.buffer_size = DEPRECATED_VALUE
        self.prioritized_replay = DEPRECATED_VALUE
        self.learning_starts = DEPRECATED_VALUE
        self.replay_batch_size = DEPRECATED_VALUE
        self.replay_sequence_length = DEPRECATED_VALUE
        self.prioritized_replay_alpha = DEPRECATED_VALUE
        self.prioritized_replay_beta = DEPRECATED_VALUE
        self.prioritized_replay_eps = DEPRECATED_VALUE

    @override(TrainerConfig)
    def training(
        self,
        *,
        target_network_update_freq: Optional[int] = None,
        replay_buffer_config: Optional[dict] = None,
        store_buffer_in_checkpoints: Optional[bool] = None,
        lr_schedule: Optional[List[List[Union[int, float]]]] = None,
        adam_epsilon: Optional[float] = None,
        grad_clip: Optional[int] = None,
        **kwargs,
    ) -> "SimpleQConfig":
        """Sets the training related configuration.

        Args:
            This value does not affect learning, only the length of iterations.
            target_network_update_freq: Update the target network every
                `target_network_update_freq` steps.
            replay_buffer_config: Replay buffer config.
            store_buffer_in_checkpoints: Set this to True, if you want the contents
                of your buffer(s)
            to be stored in any saved checkpoints as well.
                Warnings will be created if:
                    - This is True AND restoring from a checkpoint that contains no
                      buffer data.
                    - This is False AND restoring from a checkpoint that does contain
                      buffer data.
            lr_schedule: Learning rate schedule. In the format of
                [[timestep, value], [timestep, value], ...].
                A schedule should normally start from timestep 0.
            adam_epsilon: Adam epsilon hyper parameter
            grad_clip: If not None, clip gradients during optimization at this value

        Returns:
            This updated TrainerConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if target_network_update_freq is not None:
            self.target_network_update_freq = target_network_update_freq
        if replay_buffer_config is not None:
            self.replay_buffer_config = replay_buffer_config
        if store_buffer_in_checkpoints is not None:
            self.store_buffer_in_checkpoints = store_buffer_in_checkpoints
        if lr_schedule is not None:
            self.lr_schedule = lr_schedule
        if adam_epsilon is not None:
            self.adam_epsilon = adam_epsilon
        if grad_clip is not None:
            self.grad_clip = grad_clip

        return self


class SimpleQTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return SimpleQConfig().to_dict()

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        """Validates the Trainer's config dict.
        Args:
            config (TrainerConfigDict): The Trainer's config to check.
        Raises:
            ValueError: In case something is wrong with the config.
        """
        # Call super's validation method.
        super().validate_config(config)

        if config["exploration_config"]["type"] == "ParameterNoise":
            if config["batch_mode"] != "complete_episodes":
                logger.warning(
                    "ParameterNoise Exploration requires `batch_mode` to be "
                    "'complete_episodes'. Setting batch_mode="
                    "complete_episodes."
                )
                config["batch_mode"] = "complete_episodes"
            if config.get("noisy", False):
                raise ValueError(
                    "ParameterNoise Exploration and `noisy` network cannot be"
                    " used at the same time!"
                )

        validate_buffer_config(config)

        # Multi-agent mode and multi-GPU optimizer.
        if config["multiagent"]["policies"] and not config["simple_optimizer"]:
            logger.info(
                "In multi-agent mode, policies will be optimized sequentially"
                " by the multi-GPU optimizer. Consider setting "
                "`simple_optimizer=True` if this doesn't work for you."
            )

    @override(Trainer)
    def get_default_policy_class(
        self, config: TrainerConfigDict
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return SimpleQTorchPolicy
        else:
            return SimpleQTFPolicy

    @ExperimentalAPI
    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        """Simple Q training iteration function.

        Simple Q consists of the following steps:
        - Sample n MultiAgentBatches from n workers synchronously.
        - Store new samples in the replay buffer.
        - Sample one training MultiAgentBatch from the replay buffer.
        - Learn on the training batch.
        - Update the target network every `target_network_update_freq` steps.
        - Return all collected training metrics for the iteration.

        Returns:
            The results dict from executing the training iteration.
        """
        batch_size = self.config["train_batch_size"]
        local_worker = self.workers.local_worker()

        # Sample n MultiAgentBatches from n workers.
        new_sample_batches = synchronous_parallel_sample(
            worker_set=self.workers, concat=False
        )

        for batch in new_sample_batches:
            # Update sampling step counters.
            self._counters[NUM_ENV_STEPS_SAMPLED] += batch.env_steps()
            self._counters[NUM_AGENT_STEPS_SAMPLED] += batch.agent_steps()
            # Store new samples in the replay buffer
            # Use deprecated add_batch() to support old replay buffers for now
            self.local_replay_buffer.add(batch)

        global_vars = {
            "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
        }

        # Use deprecated replay() to support old replay buffers for now
        train_batch = self.local_replay_buffer.sample(batch_size)
        # If not yet learning, early-out here and do not perform learning, weight-
        # synching, or target net updating.
        if train_batch is None or len(train_batch) == 0:
            self.workers.local_worker().set_global_vars(global_vars)
            return {}

        # Learn on the training batch.
        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU)
        if self.config.get("simple_optimizer") is True:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # TODO: Move training steps counter update outside of `train_one_step()` method.
        # # Update train step counters.
        # self._counters[NUM_ENV_STEPS_TRAINED] += train_batch.env_steps()
        # self._counters[NUM_AGENT_STEPS_TRAINED] += train_batch.agent_steps()

        # Update target network every `target_network_update_freq` steps.
        cur_ts = self._counters[NUM_ENV_STEPS_TRAINED]
        last_update = self._counters[LAST_TARGET_UPDATE_TS]
        if cur_ts - last_update >= self.config["target_network_update_freq"]:
            with self._timers[TARGET_NET_UPDATE_TIMER]:
                to_update = local_worker.get_policies_to_train()
                local_worker.foreach_policy_to_train(
                    lambda p, pid: pid in to_update and p.update_target()
                )
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

        # Update weights and global_vars - after learning on the local worker - on all
        # remote workers.
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            self.workers.sync_weights(global_vars=global_vars)

        # Return all collected metrics for the iteration.
        return train_results


# Deprecated: Use ray.rllib.agents.dqn.simple_q.SimpleQConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(SimpleQConfig().to_dict())

    @Deprecated(
        old="ray.rllib.agents.dqn.simple_q.DEFAULT_CONFIG",
        new="ray.rllib.agents.dqn.simple_q.SimpleQConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
