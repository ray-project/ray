"""
Deep Q-Networks (DQN, Rainbow, Parametric DQN)
==============================================

This file defines the distributed Trainer class for the Deep Q-Networks
algorithm. See `dqn_[tf|torch]_policy.py` for the definition of the policies.

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#deep-q-networks-dqn-rainbow-parametric-dqn
"""  # noqa: E501

import logging
from typing import List, Optional, Type, Callable
import numpy as np

from ray.rllib.algorithms.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.algorithms.dqn.dqn_torch_policy import DQNTorchPolicy
from ray.rllib.algorithms.dqn.simple_q import (
    SimpleQConfig,
    SimpleQTrainer,
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
from ray.rllib.utils.typing import (
    ResultDict,
    TrainerConfigDict,
)
from ray.rllib.utils.metrics import (
    NUM_ENV_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED,
)
from ray.rllib.utils.deprecation import (
    Deprecated,
)
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.metrics import SYNCH_WORKER_WEIGHTS_TIMER
from ray.rllib.execution.common import (
    LAST_TARGET_UPDATE_TS,
    NUM_TARGET_UPDATES,
)
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.replay_buffers.utils import sample_min_n_steps_from_buffer

logger = logging.getLogger(__name__)


class DQNConfig(SimpleQConfig):
    """Defines a DQNTrainer configuration class from which a DQNTrainer can be built.

    Example:
        >>> from ray.rllib.algorithms.dqn.dqn import DQNConfig
        >>> config = DQNConfig()
        >>> print(config.replay_buffer_config)
        >>> replay_config = config.replay_buffer_config.update(
        >>>     {
        >>>         "capacity": 60000,
        >>>         "prioritized_replay_alpha": 0.5,
        >>>         "prioritized_replay_beta": 0.5,
        >>>         "prioritized_replay_eps": 3e-6,
        >>>     }
        >>> )
        >>> config.training(replay_buffer_config=replay_config)\
        >>>       .resources(num_gpus=1)\
        >>>       .rollouts(num_rollout_workers=3)\
        >>>       .environment("CartPole-v1")
        >>> trainer = DQNTrainer(config=config)
        >>> while True:
        >>>     trainer.train()

    Example:
        >>> from ray.rllib.algorithms.dqn.dqn import DQNConfig
        >>> from ray import tune
        >>> config = DQNConfig()
        >>> config.training(num_atoms=tune.grid_search(list(range(1,11)))
        >>> config.environment(env="CartPole-v1")
        >>> tune.run(
        >>>     "DQN",
        >>>     stop={"episode_reward_mean":200},
        >>>     config=config.to_dict()
        >>> )

    Example:
        >>> from ray.rllib.algorithms.dqn.dqn import DQNConfig
        >>> config = DQNConfig()
        >>> print(config.exploration_config)
        >>> explore_config = config.exploration_config.update(
        >>>     {
        >>>         "initial_epsilon": 1.5,
        >>>         "final_epsilon": 0.01,
        >>>         "epsilone_timesteps": 5000,
        >>>     }
        >>> )
        >>> config.training(lr_schedule=[[1, 1e-3, [500, 5e-3]])\
        >>>       .exploration(exploration_config=explore_config)

    Example:
        >>> from ray.rllib.algorithms.dqn.dqn import DQNConfig
        >>> config = DQNConfig()
        >>> print(config.exploration_config)
        >>> explore_config = config.exploration_config.update(
        >>>     {
        >>>         "type": "softq",
        >>>         "temperature": [1.0],
        >>>     }
        >>> )
        >>> config.training(lr_schedule=[[1, 1e-3, [500, 5e-3]])\
        >>>       .exploration(exploration_config=explore_config)
    """

    def __init__(self, trainer_class=None):
        """Initializes a DQNConfig instance."""
        super().__init__(trainer_class=trainer_class or DQNTrainer)

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
        # fmt: on
        # __sphinx_doc_end__

    @override(SimpleQConfig)
    def training(
        self,
        *,
        num_atoms: Optional[int] = None,
        v_min: Optional[float] = None,
        v_max: Optional[float] = None,
        noisy: Optional[bool] = None,
        sigma0: Optional[float] = None,
        dueling: Optional[bool] = None,
        hiddens: Optional[int] = None,
        double_q: Optional[bool] = None,
        n_step: Optional[int] = None,
        before_learn_on_batch: Callable[
            [Type[MultiAgentBatch], List[Type[Policy]], Type[int]],
            Type[MultiAgentBatch],
        ] = None,
        training_intensity: Optional[float] = None,
        replay_buffer_config: Optional[dict] = None,
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
                See: rllib/agents/dqn/dqn.py::calculate_rr_weights for further details.
            replay_buffer_config: Replay buffer config.
                Examples:
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentReplayBuffer",
                "learning_starts": 1000,
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

        Returns:
            This updated TrainerConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if num_atoms is not None:
            self.num_atoms = num_atoms
        if v_min is not None:
            self.v_min = v_min
        if v_max is not None:
            self.v_max = v_max
        if noisy is not None:
            self.noisy = noisy
        if sigma0 is not None:
            self.sigma0 = sigma0
        if dueling is not None:
            self.dueling = dueling
        if hiddens is not None:
            self.hiddens = hiddens
        if double_q is not None:
            self.double_q = double_q
        if n_step is not None:
            self.n_step = n_step
        if before_learn_on_batch is not None:
            self.before_learn_on_batch = before_learn_on_batch
        if training_intensity is not None:
            self.training_intensity = training_intensity
        if replay_buffer_config is not None:
            self.replay_buffer_config = replay_buffer_config

        return self


def calculate_rr_weights(config: TrainerConfigDict) -> List[float]:
    """Calculate the round robin weights for the rollout and train steps"""
    if not config["training_intensity"]:
        return [1, 1]

    # Calculate the "native ratio" as:
    # [train-batch-size] / [size of env-rolled-out sampled data]
    # This is to set freshly rollout-collected data in relation to
    # the data we pull from the replay buffer (which also contains old
    # samples).
    native_ratio = config["train_batch_size"] / (
        config["rollout_fragment_length"]
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


class DQNTrainer(SimpleQTrainer):
    @classmethod
    @override(SimpleQTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(SimpleQTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        # Update effective batch size to include n-step
        adjusted_rollout_len = max(config["rollout_fragment_length"], config["n_step"])
        config["rollout_fragment_length"] = adjusted_rollout_len

    @override(SimpleQTrainer)
    def get_default_policy_class(
        self, config: TrainerConfigDict
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return DQNTorchPolicy
        else:
            return DQNTFPolicy

    @ExperimentalAPI
    def training_iteration(self) -> ResultDict:
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
            self.local_replay_buffer.add_batch(new_sample_batch)

        global_vars = {
            "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
        }

        for _ in range(sample_and_train_weight):
            # Sample training batch (MultiAgentBatch) from replay buffer.
            train_batch = sample_min_n_steps_from_buffer(
                self.local_replay_buffer,
                self.config["train_batch_size"],
                count_by_agent_steps=self._by_agent_steps,
            )

            # Old-style replay buffers return None if learning has not started
            if train_batch is None or len(train_batch) == 0:
                self.workers.local_worker().set_global_vars(global_vars)
                break

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

            # Update target network every `target_network_update_freq` sample steps.
            cur_ts = self._counters[
                NUM_AGENT_STEPS_SAMPLED
                if self._by_agent_steps
                else NUM_ENV_STEPS_SAMPLED
            ]
            last_update = self._counters[LAST_TARGET_UPDATE_TS]
            if cur_ts - last_update >= self.config["target_network_update_freq"]:
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
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()


@Deprecated(
    new="Sub-class directly from `DQNTrainer` and override its methods", error=False
)
class GenericOffPolicyTrainer(DQNTrainer):
    pass
