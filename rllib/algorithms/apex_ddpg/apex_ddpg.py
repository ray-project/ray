from typing import Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.ddpg.ddpg import DDPG, DDPGConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)


class ApexDDPGConfig(DDPGConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or ApexDDPG)

        # fmt: off
        # __sphinx_doc_begin__
        # ApexDDPG-specific settings.
        self.optimizer = {
            "max_weight_sync_delay": 400,
            "num_replay_buffer_shards": 4,
            "debug": False,
        }
        self.max_requests_in_flight_per_replay_worker = float("inf")
        self.timeout_s_sampler_manager = 0.0
        self.timeout_s_replay_manager = 0.0
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
            "prioritized_replay_alpha": 0.6,
            "prioritized_replay_beta": 0.4,
            "prioritized_replay_eps": 1e-6,
            "replay_buffer_shards_colocated_with_driver": True,
            "worker_side_prioritization": True,
            "prioritized_replay": DEPRECATED_VALUE,
        }
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
    error=True,
)
class ApexDDPG(DDPG):
    @classmethod
    @override(DDPG)
    def get_default_config(cls) -> AlgorithmConfig:
        return ApexDDPGConfig()
