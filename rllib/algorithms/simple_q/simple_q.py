"""
Simple Q-Learning
=================

This module provides a basic implementation of the DQN algorithm without any
optimizations.

This file defines the distributed Algorithm class for the Simple Q algorithm.
See `simple_q_[tf|torch]_policy.py` for the definition of the policy loss.
"""

import logging
from typing import List, Optional, Type, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.simple_q.simple_q_tf_policy import (
    SimpleQTF1Policy,
    SimpleQTF2Policy,
)
from ray.rllib.algorithms.simple_q.simple_q_torch_policy import SimpleQTorchPolicy
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.execution.train_ops import multi_gpu_train_one_step, train_one_step
from ray.rllib.policy.policy import Policy
from ray.rllib.utils import deep_update
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE, Deprecated
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_TARGET_UPDATES,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TARGET_NET_UPDATE_TIMER,
)
from ray.rllib.utils.replay_buffers.utils import (
    update_priorities_in_replay_buffer,
    validate_buffer_config,
)
from ray.rllib.utils.typing import AlgorithmConfigDict, ResultDict

logger = logging.getLogger(__name__)


class SimpleQConfig(AlgorithmConfig):
    """Defines a configuration class from which a SimpleQ Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.simple_q import SimpleQConfig
        >>> config = SimpleQConfig()
        >>> print(config.replay_buffer_config)
        >>> replay_config = config.replay_buffer_config.update(
        >>>     {
        >>>         "capacity":  40000,
        >>>     }
        >>> )
        >>> config.training(replay_buffer_config=replay_config)\
        ...       .resources(num_gpus=1)\
        ...       .rollouts(num_rollout_workers=3)

    Example:
        >>> from ray.rllib.algorithms.simple_q import SimpleQConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = SimpleQConfig()
        >>> config.training(adam_epsilon=tune.grid_search([1e-8, 5e-8, 1e-7])
        >>> config.environment(env="CartPole-v1")
        >>> tune.Tuner(
        >>>     "SimpleQ",
        >>>     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        >>>     param_space=config.to_dict()
        >>> ).fit()

    Example:
        >>> from ray.rllib.algorithms.simple_q import SimpleQConfig
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

    Example:
        >>> from ray.rllib.algorithms.simple_q import SimpleQConfig
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

    def __init__(self, algo_class=None):
        """Initializes a SimpleQConfig instance."""
        super().__init__(algo_class=algo_class or SimpleQ)

        # Simple Q specific
        # fmt: off
        # __sphinx_doc_begin__
        self.target_network_update_freq = 500
        self.replay_buffer_config = {
            "type": "MultiAgentReplayBuffer",
            "capacity": 50000,
            # The number of contiguous environment steps to replay at once. This
            # may be set to greater than 1 to support recurrent models.
            "replay_sequence_length": 1,
        }
        # Number of timesteps to collect from rollout workers before we start
        # sampling from replay buffers for learning. Whether we count this in agent
        # steps  or environment steps depends on config["multiagent"]["count_steps_by"].
        self.num_steps_sampled_before_learning_starts = 1000
        self.store_buffer_in_checkpoints = False
        self.lr_schedule = None
        self.adam_epsilon = 1e-8
        self.grad_clip = 40
        self.tau = 1.0
        # __sphinx_doc_end__
        # fmt: on

        # Overrides of AlgorithmConfig defaults
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
        self.min_time_s_per_iteration = None
        self.min_sample_timesteps_per_iteration = 1000

        # Deprecated.
        self.buffer_size = DEPRECATED_VALUE
        self.prioritized_replay = DEPRECATED_VALUE
        self.learning_starts = DEPRECATED_VALUE
        self.replay_batch_size = DEPRECATED_VALUE
        # Can not use DEPRECATED_VALUE here because -1 is a common config value
        self.replay_sequence_length = None
        self.prioritized_replay_alpha = DEPRECATED_VALUE
        self.prioritized_replay_beta = DEPRECATED_VALUE
        self.prioritized_replay_eps = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        target_network_update_freq: Optional[int] = None,
        replay_buffer_config: Optional[dict] = None,
        store_buffer_in_checkpoints: Optional[bool] = None,
        lr_schedule: Optional[List[List[Union[int, float]]]] = None,
        adam_epsilon: Optional[float] = None,
        grad_clip: Optional[int] = None,
        num_steps_sampled_before_learning_starts: Optional[int] = None,
        tau: Optional[float] = None,
        **kwargs,
    ) -> "SimpleQConfig":
        """Sets the training related configuration.

        Args:
            timesteps_per_iteration: Minimum env steps to optimize for per train call.
                This value does not affect learning, only the length of iterations.
            target_network_update_freq: Update the target network every
                `target_network_update_freq` sample steps.
            replay_buffer_config: Replay buffer config.
                Examples:
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentReplayBuffer",
                "capacity": 50000,
                "replay_sequence_length": 1,
                }
                - OR -
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentPrioritizedReplayBuffer",
                "capacity": 50000,
                "prioritized_replay_alpha": 0.6,
                "prioritized_replay_beta": 0.4,
                "prioritized_replay_eps": 1e-6,
                "replay_sequence_length": 1,
                }
                - Where -
                prioritized_replay_alpha: Alpha parameter controls the degree of
                prioritization in the buffer. In other words, when a buffer sample has
                a higher temporal-difference error, with how much more probability
                should it drawn to use to update the parametrized Q-network. 0.0
                corresponds to uniform probability. Setting much above 1.0 may quickly
                result as the sampling distribution could become heavily “pointy” with
                low entropy.
                prioritized_replay_beta: Beta parameter controls the degree of
                importance sampling which suppresses the influence of gradient updates
                from samples that have higher probability of being sampled via alpha
                parameter and the temporal-difference error.
                prioritized_replay_eps: Epsilon parameter sets the baseline probability
                for sampling so that when the temporal-difference error of a sample is
                zero, there is still a chance of drawing the sample.
            store_buffer_in_checkpoints: Set this to True, if you want the contents of
                your buffer(s) to be stored in any saved checkpoints as well.
                Warnings will be created if:
                - This is True AND restoring from a checkpoint that contains no buffer
                data.
                - This is False AND restoring from a checkpoint that does contain
                buffer data.
            lr_schedule: Learning rate schedule. In the format of [[timestep, value],
                [timestep, value], ...]. A schedule should normally start from
                timestep 0.
            adam_epsilon: Adam optimizer's epsilon hyper parameter.
            grad_clip: If not None, clip gradients during optimization at this value.
            num_steps_sampled_before_learning_starts: Number of timesteps to collect
                from rollout workers before we start sampling from replay buffers for
                learning. Whether we count this in agent steps  or environment steps
                depends on config["multiagent"]["count_steps_by"].
            tau: Update the target by \tau * policy + (1-\tau) * target_policy.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if target_network_update_freq is not None:
            self.target_network_update_freq = target_network_update_freq
        if replay_buffer_config is not None:
            # Override entire `replay_buffer_config` if `type` key changes.
            # Update, if `type` key remains the same or is not specified.
            new_replay_buffer_config = deep_update(
                {"replay_buffer_config": self.replay_buffer_config},
                {"replay_buffer_config": replay_buffer_config},
                False,
                ["replay_buffer_config"],
                ["replay_buffer_config"],
            )
            self.replay_buffer_config = new_replay_buffer_config["replay_buffer_config"]
        if store_buffer_in_checkpoints is not None:
            self.store_buffer_in_checkpoints = store_buffer_in_checkpoints
        if lr_schedule is not None:
            self.lr_schedule = lr_schedule
        if adam_epsilon is not None:
            self.adam_epsilon = adam_epsilon
        if grad_clip is not None:
            self.grad_clip = grad_clip
        if num_steps_sampled_before_learning_starts is not None:
            self.num_steps_sampled_before_learning_starts = (
                num_steps_sampled_before_learning_starts
            )
        if tau is not None:
            self.tau = tau
        return self


class SimpleQ(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return SimpleQConfig().to_dict()

    @override(Algorithm)
    def validate_config(self, config: AlgorithmConfigDict) -> None:
        """Validates the Trainer's config dict.

        Args:
            config: The Trainer's config to check.

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

    @override(Algorithm)
    def get_default_policy_class(
        self, config: AlgorithmConfigDict
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return SimpleQTorchPolicy
        elif config["framework"] == "tf":
            return SimpleQTF1Policy
        else:
            return SimpleQTF2Policy

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        """Simple Q training iteration function.

        Simple Q consists of the following steps:
        - Sample n MultiAgentBatches from n workers synchronously.
        - Store new samples in the replay buffer.
        - Sample one training MultiAgentBatch from the replay buffer.
        - Learn on the training batch.
        - Update the target network every `target_network_update_freq` sample steps.
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
            self.local_replay_buffer.add(batch)

        global_vars = {
            "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
        }
        # Update target network every `target_network_update_freq` sample steps.
        cur_ts = self._counters[
            NUM_AGENT_STEPS_SAMPLED if self._by_agent_steps else NUM_ENV_STEPS_SAMPLED
        ]

        if cur_ts > self.config["num_steps_sampled_before_learning_starts"]:
            # Use deprecated replay() to support old replay buffers for now
            train_batch = self.local_replay_buffer.sample(batch_size)

            # Learn on the training batch.
            # Use simple optimizer (only for multi-agent or tf-eager; all other
            # cases should use the multi-GPU optimizer, even if only using 1 GPU)
            if self.config.get("simple_optimizer") is True:
                train_results = train_one_step(self, train_batch)
            else:
                train_results = multi_gpu_train_one_step(self, train_batch)

            # Update replay buffer priorities.
            update_priorities_in_replay_buffer(
                self.local_replay_buffer,
                self.config,
                train_batch,
                train_results,
            )

            last_update = self._counters[LAST_TARGET_UPDATE_TS]
            if cur_ts - last_update >= self.config["target_network_update_freq"]:
                with self._timers[TARGET_NET_UPDATE_TIMER]:
                    to_update = local_worker.get_policies_to_train()
                    local_worker.foreach_policy_to_train(
                        lambda p, pid: pid in to_update and p.update_target()
                    )
                self._counters[NUM_TARGET_UPDATES] += 1
                self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

            # Update weights and global_vars - after learning on the local worker -
            # on all remote workers.
            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                self.workers.sync_weights(global_vars=global_vars)
        else:
            train_results = {}

        # Return all collected metrics for the iteration.
        return train_results


# Deprecated: Use ray.rllib.algorithms.simple_q.simple_q.SimpleQConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(SimpleQConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.dqn.simple_q::DEFAULT_CONFIG",
        new="ray.rllib.algorithms.simple_q.simple_q::SimpleQConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
