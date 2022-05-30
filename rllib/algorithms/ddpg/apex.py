from typing import List, Optional

from ray.actor import ActorHandle
from ray.rllib.agents import Trainer
from ray.rllib.agents.dqn.apex import ApexTrainer
from ray.rllib.algorithms.ddpg.ddpg import DDPGConfig, DDPGTrainer
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.utils.typing import PartialTrainerConfigDict
from ray.rllib.utils.typing import ResultDict
from ray.rllib.utils.deprecation import Deprecated, DEPRECATED_VALUE
from ray.util.iter import LocalIterator


class ApexDDPGConfig(DDPGConfig):
    """Defines a configuration class from which an ApexDDPGTrainer can be built.

    Example:
        >>> from ray.rllib.algorithms.ddpg.apex import ApexDDPGConfig
        >>> config = ApexDDPGConfig().training(lr=0.01).resources(num_gpus=1)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run one training iteration.
        >>> trainer = config.build(env="Pendulum-v1")
        >>> trainer.train()

    Example:
        >>> from ray.rllib.algorithms.ddpg.apex import ApexDDPGConfig
        >>> from ray import tune
        >>> config = ApexDDPGConfig()
        >>> # Print out some default values.
        >>> print(config.lr) # doctest: +SKIP
        0.0004
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]))
        >>> # Set the config object's env.
        >>> config.environment(env="Pendulum-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "APEX_DDPG",
        ...     stop={"episode_reward_mean": 200},
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self, trainer_class=None):
        """Initializes a DDPGConfig instance."""
        super().__init__(trainer_class=trainer_class or ApexDDPGTrainer)

        # fmt: off
        # __sphinx_doc_begin__
        # ApexDDPG-specific settings.
        self.optimizer = {
            "max_weight_sync_delay": 400,
            "num_replay_buffer_shards": 4,
            "debug": False,
        }
        self.max_requests_in_flight_per_sampler_worker = 2
        self.max_requests_in_flight_per_replay_worker = float("inf")
        self.timeout_s_sampler_manager = 0.0
        self.timeout_s_replay_manager = 0.0

        # Override some of Trainer/DDPG's default values with ApexDDPG-specific values.
        self.n_step = 3
        self.exploration_config = {"type": "PerWorkerOrnsteinUhlenbeckNoise"}
        self.num_gpus = 0
        self.num_workers = 32
        self.min_sample_timesteps_per_reporting = 25000
        self.min_time_s_per_reporting = 30
        self.train_batch_size = 512
        self.rollout_fragment_length = 50
        self.replay_buffer_config = {
            "type": "MultiAgentPrioritizedReplayBuffer",
            "capacity": 2000000,
            "no_local_replay_buffer": True,
            # Alpha parameter for prioritized replay buffer.
            "prioritized_replay_alpha": 0.6,
            # Beta parameter for sampling from prioritized replay buffer.
            "prioritized_replay_beta": 0.4,
            # Epsilon to add to the TD errors when updating priorities.
            "prioritized_replay_eps": 1e-6,
            # How many steps of the model to sample before learning starts.
            "learning_starts": 50000,
            # Whether all shards of the replay buffer must be co-located
            # with the learner process (running the execution plan).
            # This is preferred b/c the learner process should have quick
            # access to the data from the buffer shards, avoiding network
            # traffic each time samples from the buffer(s) are drawn.
            # Set this to False for relaxing this constraint and allowing
            # replay shards to be created on node(s) other than the one
            # on which the learner is located.
            "replay_buffer_shards_colocated_with_driver": True,
            # Whether to compute priorities on workers.
            "worker_side_prioritization": True,
            # Specify prioritized replay by supplying a buffer type that supports
            # prioritization, for example: MultiAgentPrioritizedReplayBuffer.
            "prioritized_replay": DEPRECATED_VALUE,
        }
        self.target_network_update_freq = 500000
        self.training_intensity = 1
        # __sphinx_doc_end__
        # fmt: on

    @override(DDPGConfig)
    def training(
        self,
        *,
        optimizer: Optional[dict] = None,
        max_requests_in_flight_per_sampler_worker: Optional[int] = None,
        max_requests_in_flight_per_replay_worker: Optional[int] = None,
        timeout_s_sampler_manager: Optional[float] = None,
        timeout_s_replay_manager: Optional[float] = None,
        **kwargs,
    ) -> "DDPGConfig":
        """Sets the training related configuration.

        Args:
            optimizer: Apex-DDPG optimizer settings (dict). Set the number of reply
                buffer shards in here via the `num_replay_buffer_shards` key
                (default=4).
            max_requests_in_flight_per_sampler_worker: Max number of inflight requests
                to each sampling worker. See the AsyncRequestsManager class for more
                details. Tuning these values is important when running experimens with
                large sample batches, where there is the risk that the object store may
                fill up, causing spilling of objects to disk. This can cause any
                asynchronous requests to become very slow, making your experiment run
                slow as well. You can inspect the object store during your experiment
                via a call to ray memory on your headnode, and by using the ray
                dashboard. If you're seeing that the object store is filling up,
                turn down the number of remote requests in flight, or enable compression
                in your experiment of timesteps.
            max_requests_in_flight_per_replay_worker: Max number of inflight requests
                to each replay (shard) worker. See the AsyncRequestsManager class for
                more details. Tuning these values is important when running experimens
                with large sample batches, where there is the risk that the object store
                may fill up, causing spilling of objects to disk. This can cause any
                asynchronous requests to become very slow, making your experiment run
                slow as well. You can inspect the object store during your experiment
                via a call to ray memory on your headnode, and by using the ray
                dashboard. If you're seeing that the object store is filling up,
                turn down the number of remote requests in flight, or enable compression
                in your experiment of timesteps.
            timeout_s_sampler_manager: The timeout for waiting for sampling results
                for workers -- typically if this is too low, the manager won't be able
                to retrieve ready sampling results.
            timeout_s_replay_manager: The timeout for waiting for replay worker
                results -- typically if this is too low, the manager won't be able to
                retrieve ready replay requests.

        Returns:
            This updated ApexDDPGConfig object.
        """
        super().training(**kwargs)

        if optimizer is not None:
            self.optimizer = optimizer
        if max_requests_in_flight_per_sampler_worker is not None:
            self.max_requests_in_flight_per_sampler_worker = (
                max_requests_in_flight_per_sampler_worker
            )
        if max_requests_in_flight_per_replay_worker is not None:
            self.max_requests_in_flight_per_replay_worker = (
                max_requests_in_flight_per_replay_worker
            )
        if timeout_s_sampler_manager is not None:
            self.timeout_s_sampler_manager = timeout_s_sampler_manager
        if timeout_s_replay_manager is not None:
            self.timeout_s_replay_manager = timeout_s_replay_manager

        return self


class ApexDDPGTrainer(DDPGTrainer, ApexTrainer):
    @classmethod
    @override(DDPGTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return ApexDDPGConfig().to_dict()

    @override(DDPGTrainer)
    def setup(self, config: PartialTrainerConfigDict):
        return ApexTrainer.setup(self, config)

    @override(DDPGTrainer)
    def training_iteration(self) -> ResultDict:
        """Use APEX-DQN's training iteration function."""
        return ApexTrainer.training_iteration(self)

    @override(Trainer)
    def on_worker_failures(
        self, removed_workers: List[ActorHandle], new_workers: List[ActorHandle]
    ):
        """Handle the failures of remote sampling workers

        Args:
            removed_workers: removed worker ids.
            new_workers: ids of newly created workers.
        """
        self._sampling_actor_manager.remove_workers(removed_workers)
        self._sampling_actor_manager.add_workers(new_workers)

    @staticmethod
    @override(DDPGTrainer)
    def execution_plan(
        workers: WorkerSet, config: dict, **kwargs
    ) -> LocalIterator[dict]:
        """Use APEX-DQN's execution plan."""
        return ApexTrainer.execution_plan(workers, config, **kwargs)


# Deprecated: Use ray.rllib.algorithms.ddpg.ApexDDPGConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(ApexDDPGConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.ddpg.apex.APEX_DDPG_DEFAULT_CONFIG",
        new="ray.rllib.algorithms.ddpg.apex.ApexDDPGConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


APEX_DDPG_DEFAULT_CONFIG = _deprecated_default_config()
