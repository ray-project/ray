"""
Deep Q-Networks (DQN, Rainbow, Parametric DQN)
==============================================

This file defines the distributed Trainer class for the Deep Q-Networks
algorithm. See `dqn_[tf|torch]_policy.py` for the definition of the policies.

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#deep-q-networks-dqn-rainbow-parametric-dqn
"""  # noqa: E501

import logging
from typing import List, Optional, Callable, Type, Union

from ray.rllib.agents.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.agents.dqn.dqn_torch_policy import DQNTorchPolicy
from ray.rllib.agents.dqn.simple_q import (
    SimpleQTrainer,
    SimpleQConfig,
    DEFAULT_CONFIG as SIMPLEQ_DEFAULT_CONFIG,
)
from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_ops import Replay, StoreToReplayBuffer
from ray.rllib.execution.rollout_ops import (
    ParallelRollouts,
    synchronous_parallel_sample,
)
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.policy.sample_batch import MultiAgentBatch 
from ray.rllib.policy.policy import Policy
from ray.rllib.execution.train_ops import (
    TrainOneStep,
    UpdateTargetNetwork,
    MultiGPUTrainOneStep,
    train_one_step,
    multi_gpu_train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.utils import update_priorities_in_replay_buffer
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.typing import (
    ResultDict,
    TrainerConfigDict,
)
from ray.rllib.utils.metrics import (
    NUM_ENV_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED,
)
from ray.util.iter import LocalIterator
from ray.rllib.utils.replay_buffers import MultiAgentPrioritizedReplayBuffer
from ray.rllib.execution.buffers.multi_agent_replay_buffer import (
    MultiAgentReplayBuffer as LegacyMultiAgentReplayBuffer,
)
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.metrics import SYNCH_WORKER_WEIGHTS_TIMER
from ray.rllib.execution.common import (
    LAST_TARGET_UPDATE_TS,
    NUM_TARGET_UPDATES,
)

logger = logging.getLogger(__name__)


class DQNConfig(SimpleQConfig):
    """Defines a DQNTrainer configuration class from which a DQNTrainer can be built.
    
    Example:
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
        >>> config = DQNConfig()
        >>> config.training(num_atoms=tune.grid_search(list(range(1,11)))
        >>> config.environment(env="CartPole-v1")
        >>> tune.run(
        >>>     "DQN",
        >>>     stop={"episode_reward_mean":200},
        >>>     config=config.to_dict()
        >>> )

    Example:
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

    def __init__(self):
        """Initializes a DQNConfig instance."""
        SimpleQConfig.__init__(self)

        # DQN specific
        # fmt: off
        # __sphinx_doc_begin__
        #
        self.trainer_class = DQNTrainer
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
        self.worker_side_prioritization = False

        # Changes to SimpleQConfig default 
        self.replay_buffer_config = {
            "_enable_replay_buffer_api": True,
            "type": "MultiAgentPrioritizedReplayBuffer",
            "capacity": 50000,
            "prioritized_replay_alpha": 0.6,
            "prioritized_replay_beta": 0.4,
            "prioritized_replay_eps": 1e-6,
            "replay_sequence_length": 1,
        },
        self._disable_execution_plan_api = True

    @override(SimpleQConfig)
    def training(
        num_atoms: Optional[int] = None,
        v_min: Optional[float] = None,
        v_max: Optional[float] = None,
        noisy: Optional[bool] = None,
        sigma0: Optional[float] = None,
        dueling: Optional[bool] = None,
        hiddens: Optional[int] = None,
        double_q: Optional[bool] = None,
        n_step: Optional[int] = None,
        before_learn_on_batch: Callable[[Type[MultiAgentBatch],List[Type[Policy]],Type[int]], Type[MultiAgentBatch]] = None,
        training_intensity: Optional[float] = None,
        worker_side_prioritization: Optional[bool] = None,
        replay_buffer_config: Optional[dict] = None,
    ) -> "DQNConfig":
        """Sets the training related configuration.

        Args:
            num_atoms: Number of atoms for representing the distribution of return.
                When this is greater than 1, distributional Q-learning is used.
            v_min: Minimum value estimation
            v_max: Maximum value estimation
            noisy: Whether to use noisy network to aid exploration. This adds parametric noise to the model weights.
            sigma0: Control the initial parameter noise for noisy nets 
            dueling: Whether to use dueling dqn
            hiddens: Dense-layer setup for each the advantage branch and the value branch
            double_q: Whether to use double dqn
            n_step: N-step Q-learning
            before_learn_on_batch: Callback to run before learning on a multi-agent batch of experiences
            training_intensity: The intensity with which to update the model (vs collecting samples from the env).
                If None, uses "natural" values of:
                    `train_batch_size` / (`rollout_fragment_length` x `num_workers` x `num_envs_per_worker`).
                If provided, will make sure that the ratio between ts inserted into and sampled from th buffer matches the given values.
                    Example:
                        training_intensity=1000.0
                        train_batch_size=250
                        rollout_fragment_length=1
                        num_workers=1 (or 0)
                        num_envs_per_worker=1
                        -> natural value = 250 / 1 = 250.0
                        -> will make sure that replay+train op will be executed 4x asoften as rollout+insert op (4 * 250 = 1000).
                        See: rllib/agents/dqn/dqn.py::calculate_rr_weights for further details.
            worker_side_prioritization: Whether to compute priorities on workers.
            replay_buffer_config: Replay buffer config.
                Examples:
                    {
                        "_enable_replay_buffer_api": True,
                        "learning_starts": 1000,
                        "type": "MultiAgentReplayBuffer",
                        "capacity": 50000,
                        "replay_batch_size": 32,
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
                    prioritized_replay_alpha: Alpha parameter controls the degree of prioritization in the buffer. In other words, when a buffer sample has a higher temporal-difference error, with how much more probability should it drawn to use to update the parametrized Q-network. 0.0 corresponds to uniform probability. Setting much above 1.0 may quickly result as the sampling distribution could become heavily “pointy” with low entropy.
                    prioritized_replay_beta: Beta parameter controls the degree of importance sampling which suppresses the influence of gradient updates from samples that have higher probability of being sampled via alpha parameter and the temporal-difference error.
                    prioritized_replay_eps: Epsilon parameter sets the baseline probability for sampling so that when the temporal-difference error of a sample is zero, there is still a chance of drawing the sample.
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
        if worker_side_prioritization is not None:
            self.worker_side_priorizatiion = worker_side_prioritization
        if replay_buffer_config is not None:
            self.replay_buffer_config = replay_buffer_config

#Deprecated: Use ray.rllib.agents.dqn.DQNConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(
            Trainer.merge_trainer_configs(
                SIMPLEQ_DEFAULT_CONFIG,
                {
                    # DQN specific keys:
                    "num_atoms": 1,
                    "v_min": -10.0,
                    "v_max": 10.0,
                    "noisy": False,
                    "sigma0": 0.5,
                    "dueling": True,
                    "hiddens": [256],
                    "double_q": True,
                    "n_step": 1,
                    "before_learn_on_batch": None,
                    "training_intensity": None,
                    "worker_side_prioritization": False,
                    "buffer_size": DEPRECATED_VALUE,
                    # Changes to SimpleQConfig default: 
                    "replay_buffer_config": {
                        "_enable_replay_buffer_api": True,
                        "type": "MultiAgentPrioritizedReplayBuffer",
                        "capacity": 50000,
                        "prioritized_replay_alpha": 0.6,
                        "prioritized_replay_beta": 0.4,
                        "prioritized_replay_eps": 1e-6,
                        "replay_sequence_length": 1,
                    },
                    "_disable_execution_plan_api": True,
                },
                _allow_unknown_configs=True,
            )
        )
>>>>>>> simple q and dqn

    @Deprecated(
        old="ray.rllib.agents.dqn.dqn.DEFAULT_CONFIG",
        new="ray.rllib.agents.dqn.dqn.DQNConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)

DEFAULT_CONFIG = _deprecated_default_config()

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
        * config["num_workers"]
    )

    # Training intensity is specified in terms of
    # (steps_replayed / steps_sampled), so adjust for the native ratio.
    weights = [1, config["training_intensity"] / native_ratio]
    return weights


class DQNTrainer(SimpleQTrainer):
    @classmethod
    @override(SimpleQTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DQNConfig().to_dict()

    @override(SimpleQTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        """Validates the Trainer's config dict.

        Args:
            config (TrainerConfigDict): The Trainer's config to check.

        Raises:
            ValueError: In case something is wrong with the config.
        """
        # Call super's validation method.
        super().validate_config(config)

        # Update effective batch size to include n-step
        adjusted_rollout_len = max(config["rollout_fragment_length"], config["n_step"])
        config["rollout_fragment_length"] = adjusted_rollout_len

    @override(SimpleQTrainer)
    def get_default_policy_class(
        self, config: TrainerConfigDict
    ) -> Optional[Type[Policy]]:
        if  config["framework"] == "torch":
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
        - Update target network every target_network_update_freq steps.
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

        for _ in range(sample_and_train_weight):
            # Sample training batch (MultiAgentBatch) from replay buffer.
            train_batch = self.local_replay_buffer.replay()

            # Old-style replay buffers return None if learning has not started
            if not train_batch:
                continue

            # Postprocess batch before we learn on it
            post_fn = self.config.get("before_learn_on_batch") or (lambda b, *a: b)
            train_batch = post_fn(train_batch, self.workers, self.config)

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

            # Update target network every `target_network_update_freq` steps.
            cur_ts = self._counters[NUM_ENV_STEPS_SAMPLED]
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
            global_vars = {
                "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
            }
            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                self.workers.sync_weights(global_vars=global_vars)

        # Return all collected metrics for the iteration.
        return train_results

    @staticmethod
    @override(SimpleQTrainer)
    def execution_plan(
        workers: WorkerSet, config: TrainerConfigDict, **kwargs
    ) -> LocalIterator[dict]:
        assert (
            "local_replay_buffer" in kwargs
        ), "DQN's execution plan requires a local replay buffer."

        # Assign to Trainer, so we can store the MultiAgentReplayBuffer's
        # data when we save checkpoints.
        local_replay_buffer = kwargs["local_replay_buffer"]

        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        # We execute the following steps concurrently:
        # (1) Generate rollouts and store them in our local replay buffer.
        # Calling next() on store_op drives this.
        store_op = rollouts.for_each(
            StoreToReplayBuffer(local_buffer=local_replay_buffer)
        )

        def update_prio(item):
            samples, info_dict = item
            prio_dict = {}
            for policy_id, info in info_dict.items():
                # TODO(sven): This is currently structured differently for
                #  torch/tf. Clean up these results/info dicts across
                #  policies (note: fixing this in torch_policy.py will
                #  break e.g. DDPPO!).
                td_error = info.get("td_error", info[LEARNER_STATS_KEY].get("td_error"))
                samples.policy_batches[policy_id].set_get_interceptor(None)
                batch_indices = samples.policy_batches[policy_id].get("batch_indexes")
                # In case the buffer stores sequences, TD-error could
                # already be calculated per sequence chunk.
                if len(batch_indices) != len(td_error):
                    T = local_replay_buffer.replay_sequence_length
                    assert (
                        len(batch_indices) > len(td_error)
                        and len(batch_indices) % T == 0
                    )
                    batch_indices = batch_indices.reshape([-1, T])[:, 0]
                    assert len(batch_indices) == len(td_error)
                prio_dict[policy_id] = (batch_indices, td_error)
            local_replay_buffer.update_priorities(prio_dict)
            return info_dict

        # (2) Read and train on experiences from the replay buffer. Every batch
        # returned from the LocalReplay() iterator is passed to TrainOneStep to
        # take a SGD step, and then we decide whether to update the target
        # network.
        post_fn = config.get("before_learn_on_batch") or (lambda b, *a: b)

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

        if (
            type(local_replay_buffer) is LegacyMultiAgentReplayBuffer
            and config["replay_buffer_config"].get("prioritized_replay_alpha", 0.0)
            > 0.0
        ) or isinstance(local_replay_buffer, MultiAgentPrioritizedReplayBuffer):
            update_prio_fn = update_prio
        else:

            def update_prio_fn(x):
                return x

        replay_op = (
            Replay(local_buffer=local_replay_buffer)
            .for_each(lambda x: post_fn(x, workers, config))
            .for_each(train_step_op)
            .for_each(update_prio_fn)
            .for_each(
                UpdateTargetNetwork(workers, config["target_network_update_freq"])
            )
        )

        # Alternate deterministically between (1) and (2).
        # Only return the output of (2) since training metrics are not
        # available until (2) runs.
        train_op = Concurrently(
            [store_op, replay_op],
            mode="round_robin",
            output_indexes=[1],
            round_robin_weights=calculate_rr_weights(config),
        )

        return StandardMetricsReporting(train_op, workers, config)


@Deprecated(
    new="Sub-class directly from `DQNTrainer` and override its methods", error=False
)
class GenericOffPolicyTrainer(DQNTrainer):
    pass
