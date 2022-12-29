from typing import Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQN
from ray.rllib.algorithms.ddpg.ddpg import DDPG, DDPGConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE, Deprecated
from ray.rllib.utils.typing import (
    ResultDict,
)


class ApexDDPGConfig(DDPGConfig):
    """Defines a configuration class from which an ApexDDPG Trainer can be built.

    Example:
        >>> from ray.rllib.algorithms.apex_ddpg.apex_ddpg import ApexDDPGConfig
        >>> config = ApexDDPGConfig().training(lr=0.01).resources(num_gpus=1)
        >>> print(config.to_dict()) # doctest: +SKIP
        >>> # Build a Trainer object from the config and run one training iteration.
        >>> algo = config.build(env="Pendulum-v1")
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.apex_ddpg.apex_ddpg import ApexDDPGConfig
        >>> from ray import tune
        >>> import ray.air as air
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
        >>> tune.Tuner( # doctest: +SKIP
        ...     "APEX_DDPG",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes an ApexDDPGConfig instance."""
        super().__init__(algo_class=algo_class or ApexDDPG)

        # fmt: off
        # __sphinx_doc_begin__
        # ApexDDPG-specific settings.
        self.optimizer = {
            "max_weight_sync_delay": 400,
            "num_replay_buffer_shards": 4,
            "debug": False,
        }
        # Overwrite the default max_requests_in_flight_per_replay_worker.
        self.max_requests_in_flight_per_replay_worker = float("inf")
        self.timeout_s_sampler_manager = 0.0
        self.timeout_s_replay_manager = 0.0

        # Override some of Trainer/DDPG's default values with ApexDDPG-specific values.
        self.n_step = 3
        self.exploration_config = {"type": "PerWorkerOrnsteinUhlenbeckNoise"}
        self.num_gpus = 0
        self.num_rollout_workers = 32
        self.min_sample_timesteps_per_iteration = 25000
        self.min_time_s_per_iteration = 30
        self.train_batch_size = 512
        self.rollout_fragment_length = 50
        self.replay_buffer_config = {
            "type": "MultiAgentPrioritizedReplayBuffer",
            "capacity_ts": 2000000,
            "no_local_replay_buffer": True,
            # Alpha parameter for prioritized replay buffer.
            "prioritized_replay_alpha": 0.6,
            # Beta parameter for sampling from prioritized replay buffer.
            "prioritized_replay_beta": 0.4,
            # Epsilon to add to the TD errors when updating priorities.
            "prioritized_replay_eps": 1e-6,
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
        # Number of timesteps to collect from rollout workers before we start
        # sampling from replay buffers for learning. Whether we count this in agent
        # steps  or environment steps depends on config["multiagent"]["count_steps_by"].
        self.num_steps_sampled_before_learning_starts = 50000
        self.target_network_update_freq = 500000
        self.training_intensity = 1
        # __sphinx_doc_end__
        # fmt: on

    @override(DDPGConfig)
    def training(
        self,
        *,
        timeout_s_sampler_manager: Optional[float] = NotProvided,
        timeout_s_replay_manager: Optional[float] = NotProvided,
        **kwargs,
    ) -> "ApexDDPGConfig":
        """Sets the training related configuration.

        Args:
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

        if timeout_s_sampler_manager is not NotProvided:
            self.timeout_s_sampler_manager = timeout_s_sampler_manager
        if timeout_s_replay_manager is not NotProvided:
            self.timeout_s_replay_manager = timeout_s_replay_manager

        return self


class ApexDDPG(DDPG, ApexDQN):
    @classmethod
    @override(DDPG)
    def get_default_config(cls) -> AlgorithmConfig:
        return ApexDDPGConfig()

    @override(DDPG)
    def setup(self, config: AlgorithmConfig):
        return ApexDQN.setup(self, config)

    @override(DDPG)
    def training_step(self) -> ResultDict:
        """Use APEX-DQN's training iteration function."""
        return ApexDQN.training_step(self)


# Deprecated: Use ray.rllib.algorithms.apex_ddpg.ApexDDPGConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(ApexDDPGConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.ddpg.apex.APEX_DDPG_DEFAULT_CONFIG",
        new="ray.rllib.algorithms.apex_ddpg.apex_ddpg::ApexDDPGConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


APEX_DDPG_DEFAULT_CONFIG = _deprecated_default_config()
