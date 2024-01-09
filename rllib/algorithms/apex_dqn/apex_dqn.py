from typing import Optional

from ray._private.dict import merge_dicts
from ray.rllib.algorithms.algorithm_config import NotProvided
from ray.rllib.algorithms.dqn.dqn import DQN, DQNConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)


class ApexDQNConfig(DQNConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or ApexDQN)

        # fmt: off
        # __sphinx_doc_begin__
        # APEX-DQN settings overriding DQN ones:
        self.optimizer = merge_dicts(
            DQNConfig().optimizer, {
                "max_weight_sync_delay": 400,
                "num_replay_buffer_shards": 4,
                "debug": False
            })
        self.n_step = 3
        self.train_batch_size = 512
        self.target_network_update_freq = 500000
        self.training_intensity = 1
        self.num_steps_sampled_before_learning_starts = 50000

        self.max_requests_in_flight_per_replay_worker = float("inf")
        self.timeout_s_sampler_manager = 0.0
        self.timeout_s_replay_manager = 0.0
        self.replay_buffer_config = {
            "no_local_replay_buffer": True,
            "type": "MultiAgentPrioritizedReplayBuffer",
            "capacity": 2000000,
            "prioritized_replay_alpha": 0.6,
            "prioritized_replay_beta": 0.4,
            "prioritized_replay_eps": 1e-6,
            "replay_buffer_shards_colocated_with_driver": True,
            "worker_side_prioritization": True,
            "prioritized_replay": DEPRECATED_VALUE,
        }
        self.num_rollout_workers = 32
        self.rollout_fragment_length = 50
        self.exploration_config = {
            "type": "PerWorkerEpsilonGreedy",
        }
        self.num_gpus = 1
        self.min_time_s_per_iteration = 30
        self.min_sample_timesteps_per_iteration = 25000

        # fmt: on
        # __sphinx_doc_end__

    def training(
        self,
        *,
        max_requests_in_flight_per_replay_worker: Optional[int] = NotProvided,
        timeout_s_sampler_manager: Optional[float] = NotProvided,
        timeout_s_replay_manager: Optional[float] = NotProvided,
        **kwargs,
    ) -> "ApexDQNConfig":
        super().training(**kwargs)

        if max_requests_in_flight_per_replay_worker is not NotProvided:
            self.max_requests_in_flight_per_replay_worker = (
                max_requests_in_flight_per_replay_worker
            )
        if timeout_s_sampler_manager is not NotProvided:
            self.timeout_s_sampler_manager = timeout_s_sampler_manager
        if timeout_s_replay_manager is not NotProvided:
            self.timeout_s_replay_manager = timeout_s_replay_manager

        return self

    @override(DQNConfig)
    def validate(self) -> None:
        if self.num_gpus > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for APEX-DQN!")
        # Call DQN's validation method.
        super().validate()


@Deprecated(
    old="rllib/algorithms/apex_dqn/",
    new="rllib_contrib/apex_dqn/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class ApexDQN(DQN):
    @classmethod
    @override(DQN)
    def get_default_config(cls) -> ApexDQNConfig:
        return ApexDQNConfig()
