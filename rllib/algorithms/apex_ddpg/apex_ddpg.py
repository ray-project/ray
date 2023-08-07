from typing import Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQN
from ray.rllib.algorithms.ddpg.ddpg import DDPG, DDPGConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)
from ray.rllib.utils.typing import (
    ResultDict,
)


class ApexDDPGConfig(DDPGConfig):
    """Defines a configuration class from which an ApexDDPG can be built."""

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

        # Override some of Algorithm/DDPG's default values with ApexDDPG-specific
        # values.
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
            "capacity": 2000000,
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
        # steps  or environment steps depends on config.multi_agent(count_steps_by=..).
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


@Deprecated(
    old="rllib/algorithms/apex_ddpg/",
    new="rllib_contrib/apex_ddpg/",
    help=ALGO_DEPRECATION_WARNING,
    error=False,
)
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
