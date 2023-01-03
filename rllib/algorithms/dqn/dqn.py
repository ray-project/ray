"""
Deep Q-Networks (DQN, Rainbow, Parametric DQN)
==============================================

This file defines the distributed Algorithm class for the Deep Q-Networks
algorithm. See `dqn_[tf|torch]_policy.py` for the definition of the policies.

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#deep-q-networks-dqn-rainbow-parametric-dqn
"""  # noqa: E501

import logging
from typing import List, Optional, Type, Callable
import numpy as np

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.algorithms.dqn.dqn_torch_policy import DQNTorchPolicy
from ray.rllib.algorithms.simple_q.simple_q import (
    SimpleQ,
    SimpleQConfig,
)
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.execution.train_ops import (
    train_one_step,
    multi_gpu_train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.utils import update_priorities_in_replay_buffer
from ray.rllib.utils.typing import ResultDict
from ray.rllib.utils.metrics import (
    NUM_ENV_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED,
)
from ray.rllib.utils.deprecation import (
    Deprecated,
)
from ray.rllib.utils.metrics import SYNCH_WORKER_WEIGHTS_TIMER
from ray.rllib.execution.common import (
    LAST_TARGET_UPDATE_TS,
    NUM_TARGET_UPDATES,
)
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.replay_buffers.utils import sample_min_n_steps_from_buffer

logger = logging.getLogger(__name__)


class DQNConfig(SimpleQConfig):
    """Defines a configuration class from which a DQN Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.dqn.dqn import DQNConfig
        >>> config = DQNConfig()
        >>> print(config.replay_buffer_config)  # doctest: +SKIP
        >>> replay_config = config.replay_buffer_config.update( # doctest: +SKIP
        ...     {
        ...         "capacity": 60000,
        ...         "prioritized_replay_alpha": 0.5,
        ...         "prioritized_replay_beta": 0.5,
        ...         "prioritized_replay_eps": 3e-6,
        ...     }
        ... )
        >>> config = config.training(replay_buffer_config=replay_config)# doctest: +SKIP
        >>> config = config.resources(num_gpus=1)  # doctest: +SKIP
        >>> config = config.rollouts(num_rollout_workers=3)  # doctest: +SKIP
        >>> config = config.environment("CartPole-v1")  # doctest: +SKIP
        >>> trainer = DQN(config=config)  # doctest: +SKIP
        >>> trainer.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.dqn.dqn import DQNConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = DQNConfig()
        >>> config = config.training( # doctest: +SKIP
        ...     num_atoms=tune.grid_search(list(range(1,11))))
        >>> config = config.environment(env="CartPole-v1") # doctest: +SKIP
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "DQN",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean":200}),
        ...     param_space=config.to_dict()
        ... ).fit()

    Example:
        >>> from ray.rllib.algorithms.dqn.dqn import DQNConfig
        >>> config = DQNConfig()
        >>> print(config.exploration_config)  # doctest: +SKIP
        >>> explore_config = config.exploration_config.update( # doctest: +SKIP
        ...     {
        ...         "initial_epsilon": 1.5,
        ...         "final_epsilon": 0.01,
        ...         "epsilone_timesteps": 5000,
        ...     }
        ... )
        >>> config.training(lr_schedule=[[1, 1e-3, [500, 5e-3]])\ # doctest: +SKIP
        ...       .exploration(exploration_config=explore_config)

    Example:
        >>> from ray.rllib.algorithms.dqn.dqn import DQNConfig
        >>> config = DQNConfig()
        >>> print(config.exploration_config)  # doctest: +SKIP
        >>> explore_config = config.exploration_config.update( # doctest: +SKIP
        ...     {
        ...         "type": "softq",
        ...         "temperature": [1.0],
        ...     }
        ... )
        >>> config.training(lr_schedule=[[1, 1e-3, [500, 5e-3]])\ # doctest: +SKIP
        ...       .exploration(exploration_config=explore_config)
    """

    def __init__(self, algo_class=None):
        """Initializes a DQNConfig instance."""
        super().__init__(algo_class=algo_class or DQN)

        # DQN specific config settings.
        # fmt: off
        # __sphinx_doc_begin__
        self.num_atoms = 1
        self.v_min = -10.0
        self.v_max = 10.0
        self.noisy = False
        self.sigma0 = 0.5
        self.dueling = True
        self.hiddens = [256]
        self.double_q = True
        self.n_step = 1
        self.before_learn_on_batch = None
        self.training_intensity = None
        self.td_error_loss_fn = "huber"
        self.categorical_distribution_temperature = 1.0

        # Changes to SimpleQConfig's default:
        self.replay_buffer_config = {
            "type": "MultiAgentPrioritizedReplayBuffer",
            # Specify prioritized replay by supplying a buffer type that supports
            # prioritization, for example: MultiAgentPrioritizedReplayBuffer.
            "prioritized_replay": DEPRECATED_VALUE,
            # Size of the replay buffer. Note that if async_updates is set,
            # then each worker will have a replay buffer of this size.
            "capacity": 50000,
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
        # Set to `self.n_step`, if 'auto'.
        self.rollout_fragment_length = "auto"
        # fmt: on
        # __sphinx_doc_end__

    @override(SimpleQConfig)
    def training(
        self,
        *,
        num_atoms: Optional[int] = NotProvided,
        v_min: Optional[float] = NotProvided,
        v_max: Optional[float] = NotProvided,
        noisy: Optional[bool] = NotProvided,
        sigma0: Optional[float] = NotProvided,
        dueling: Optional[bool] = NotProvided,
        hiddens: Optional[int] = NotProvided,
        double_q: Optional[bool] = NotProvided,
        n_step: Optional[int] = NotProvided,
        before_learn_on_batch: Callable[
            [Type[MultiAgentBatch], List[Type[Policy]], Type[int]],
            Type[MultiAgentBatch],
        ] = NotProvided,
        training_intensity: Optional[float] = NotProvided,
        td_error_loss_fn: Optional[str] = NotProvided,
        categorical_distribution_temperature: Optional[float] = NotProvided,
        **kwargs,
    ) -> "DQNConfig":
        """Sets the training related configuration.

        Args:
            num_atoms: Number of atoms for representing the distribution of return.
                When this is greater than 1, distributional Q-learning is used.
            v_min: Minimum value estimation
            v_max: Maximum value estimation
            noisy: Whether to use noisy network to aid exploration. This adds parametric
                noise to the model weights.
            sigma0: Control the initial parameter noise for noisy nets.
            dueling: Whether to use dueling DQN.
            hiddens: Dense-layer setup for each the advantage branch and the value
                branch
            double_q: Whether to use double DQN.
            n_step: N-step for Q-learning.
            before_learn_on_batch: Callback to run before learning on a multi-agent
                batch of experiences.
            training_intensity: The intensity with which to update the model (vs
                collecting samples from the env).
                If None, uses "natural" values of:
                `train_batch_size` / (`rollout_fragment_length` x `num_workers` x
                `num_envs_per_worker`).
                If not None, will make sure that the ratio between timesteps inserted
                into and sampled from the buffer matches the given values.
                Example:
                training_intensity=1000.0
                train_batch_size=250
                rollout_fragment_length=1
                num_workers=1 (or 0)
                num_envs_per_worker=1
                -> natural value = 250 / 1 = 250.0
                -> will make sure that replay+train op will be executed 4x asoften as
                rollout+insert op (4 * 250 = 1000).
                See: rllib/algorithms/dqn/dqn.py::calculate_rr_weights for further
                details.
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
            td_error_loss_fn: "huber" or "mse". loss function for calculating TD error
                when num_atoms is 1. Note that if num_atoms is > 1, this parameter
                is simply ignored, and softmax cross entropy loss will be used.
            categorical_distribution_temperature: Set the temperature parameter used
                by Categorical action distribution. A valid temperature is in the range
                of [0, 1]. Note that this mostly affects evaluation since TD error uses
                argmax for return calculation.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if num_atoms is not NotProvided:
            self.num_atoms = num_atoms
        if v_min is not NotProvided:
            self.v_min = v_min
        if v_max is not NotProvided:
            self.v_max = v_max
        if noisy is not NotProvided:
            self.noisy = noisy
        if sigma0 is not NotProvided:
            self.sigma0 = sigma0
        if dueling is not NotProvided:
            self.dueling = dueling
        if hiddens is not NotProvided:
            self.hiddens = hiddens
        if double_q is not NotProvided:
            self.double_q = double_q
        if n_step is not NotProvided:
            self.n_step = n_step
        if before_learn_on_batch is not NotProvided:
            self.before_learn_on_batch = before_learn_on_batch
        if training_intensity is not NotProvided:
            self.training_intensity = training_intensity
        if td_error_loss_fn is not NotProvided:
            self.td_error_loss_fn = td_error_loss_fn
        if categorical_distribution_temperature is not NotProvided:
            self.categorical_distribution_temperature = (
                categorical_distribution_temperature
            )

        return self

    @override(SimpleQConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if self.td_error_loss_fn not in ["huber", "mse"]:
            raise ValueError("`td_error_loss_fn` must be 'huber' or 'mse'!")

        # Check rollout_fragment_length to be compatible with n_step.
        if (
            not self.in_evaluation
            and self.rollout_fragment_length != "auto"
            and self.rollout_fragment_length < self.n_step
        ):
            raise ValueError(
                f"Your `rollout_fragment_length` ({self.rollout_fragment_length}) is "
                f"smaller than `n_step` ({self.n_step})! "
                f"Try setting config.rollouts(rollout_fragment_length={self.n_step})."
            )

        if self.exploration_config["type"] == "ParameterNoise":
            if self.batch_mode != "complete_episodes":
                raise ValueError(
                    "ParameterNoise Exploration requires `batch_mode` to be "
                    "'complete_episodes'. Try setting `config.rollouts("
                    "batch_mode='complete_episodes')`."
                )
            if self.noisy:
                raise ValueError(
                    "ParameterNoise Exploration and `noisy` network cannot be"
                    " used at the same time!"
                )

    @override(AlgorithmConfig)
    def get_rollout_fragment_length(self, worker_index: int = 0) -> int:
        if self.rollout_fragment_length == "auto":
            return self.n_step
        else:
            return self.rollout_fragment_length


def calculate_rr_weights(config: AlgorithmConfig) -> List[float]:
    """Calculate the round robin weights for the rollout and train steps"""
    if not config["training_intensity"]:
        return [1, 1]

    # Calculate the "native ratio" as:
    # [train-batch-size] / [size of env-rolled-out sampled data]
    # This is to set freshly rollout-collected data in relation to
    # the data we pull from the replay buffer (which also contains old
    # samples).
    native_ratio = config["train_batch_size"] / (
        config.get_rollout_fragment_length()
        * config["num_envs_per_worker"]
        # Add one to workers because the local
        # worker usually collects experiences as well, and we avoid division by zero.
        * max(config["num_workers"] + 1, 1)
    )

    # Training intensity is specified in terms of
    # (steps_replayed / steps_sampled), so adjust for the native ratio.
    sample_and_train_weight = config["training_intensity"] / native_ratio
    if sample_and_train_weight < 1:
        return [int(np.round(1 / sample_and_train_weight)), 1]
    else:
        return [1, int(np.round(sample_and_train_weight))]


class DQN(SimpleQ):
    @classmethod
    @override(SimpleQ)
    def get_default_config(cls) -> AlgorithmConfig:
        return DQNConfig()

    @classmethod
    @override(SimpleQ)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return DQNTorchPolicy
        else:
            return DQNTFPolicy

    @override(SimpleQ)
    def training_step(self) -> ResultDict:
        """DQN training iteration function.

        Each training iteration, we:
        - Sample (MultiAgentBatch) from workers.
        - Store new samples in replay buffer.
        - Sample training batch (MultiAgentBatch) from replay buffer.
        - Learn on training batch.
        - Update remote workers' new policy weights.
        - Update target network every `target_network_update_freq` sample steps.
        - Return all collected metrics for the iteration.

        Returns:
            The results dict from executing the training iteration.
        """
        train_results = {}

        # We alternate between storing new samples and sampling and training
        store_weight, sample_and_train_weight = calculate_rr_weights(self.config)

        for _ in range(store_weight):
            # Sample (MultiAgentBatch) from workers.
            new_sample_batch = synchronous_parallel_sample(
                worker_set=self.workers, concat=True
            )

            # Update counters
            self._counters[NUM_AGENT_STEPS_SAMPLED] += new_sample_batch.agent_steps()
            self._counters[NUM_ENV_STEPS_SAMPLED] += new_sample_batch.env_steps()

            # Store new samples in replay buffer.
            self.local_replay_buffer.add(new_sample_batch)

        global_vars = {
            "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
        }

        # Update target network every `target_network_update_freq` sample steps.
        cur_ts = self._counters[
            NUM_AGENT_STEPS_SAMPLED
            if self.config.count_steps_by == "agent_steps"
            else NUM_ENV_STEPS_SAMPLED
        ]

        if cur_ts > self.config.num_steps_sampled_before_learning_starts:
            for _ in range(sample_and_train_weight):
                # Sample training batch (MultiAgentBatch) from replay buffer.
                train_batch = sample_min_n_steps_from_buffer(
                    self.local_replay_buffer,
                    self.config.train_batch_size,
                    count_by_agent_steps=self.config.count_steps_by == "agent_steps",
                )

                # Postprocess batch before we learn on it
                post_fn = self.config.get("before_learn_on_batch") or (lambda b, *a: b)
                train_batch = post_fn(train_batch, self.workers, self.config)

                # for policy_id, sample_batch in train_batch.policy_batches.items():
                #     print(len(sample_batch["obs"]))
                #     print(sample_batch.count)

                # Learn on training batch.
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
                if cur_ts - last_update >= self.config.target_network_update_freq:
                    to_update = self.workers.local_worker().get_policies_to_train()
                    self.workers.local_worker().foreach_policy_to_train(
                        lambda p, pid: pid in to_update and p.update_target()
                    )
                    self._counters[NUM_TARGET_UPDATES] += 1
                    self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

                # Update weights and global_vars - after learning on the local worker -
                # on all remote workers.
                with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                    self.workers.sync_weights(global_vars=global_vars)

        # Return all collected metrics for the iteration.
        return train_results


# Deprecated: Use ray.rllib.algorithms.dqn.DQNConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(DQNConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.dqn.dqn.DEFAULT_CONFIG",
        new="ray.rllib.algorithms.dqn.dqn.DQNConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()


@Deprecated(new="Sub-class directly from `DQN` and override its methods", error=True)
class GenericOffPolicyTrainer(SimpleQ):
    pass
