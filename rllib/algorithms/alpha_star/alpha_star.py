from typing import Any, Dict, Optional

import ray.rllib.algorithms.appo.appo as appo
from ray.rllib.algorithms.algorithm_config import NotProvided
from ray.rllib.utils import deep_update
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


class AlphaStarConfig(appo.APPOConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or AlphaStar)

        # fmt: off
        # __sphinx_doc_begin__
        self.replay_buffer_capacity = 20
        self.replay_buffer_replay_ratio = 0.5
        self.max_requests_in_flight_per_sampler_worker = 2
        self.max_requests_in_flight_per_learner_worker = 2
        self.timeout_s_sampler_manager = 0.0
        self.timeout_s_learner_manager = 0.0
        self.league_builder_config = {
            "type": None,
            "num_random_policies": 2,
            "num_learning_league_exploiters": 4,
            "num_learning_main_exploiters": 4,
            "win_rate_threshold_for_new_snapshot": 0.9,
            "keep_new_snapshot_training_prob": 0.0,
            "prob_league_exploiter_match": 0.33,
            "prob_main_exploiter_match": 0.33,
            "prob_main_exploiter_playing_against_learning_main": 0.5,
        }
        self.max_num_policies_to_train = None
        self.min_time_s_per_iteration = 2
        self.policies = None
        self.simple_optimizer = True
        # __sphinx_doc_end__
        # fmt: on

    @override(appo.APPOConfig)
    def training(
        self,
        *,
        replay_buffer_capacity: Optional[int] = NotProvided,
        replay_buffer_replay_ratio: Optional[float] = NotProvided,
        max_requests_in_flight_per_sampler_worker: Optional[int] = NotProvided,
        max_requests_in_flight_per_learner_worker: Optional[int] = NotProvided,
        timeout_s_sampler_manager: Optional[float] = NotProvided,
        timeout_s_learner_manager: Optional[float] = NotProvided,
        league_builder_config: Optional[Dict[str, Any]] = NotProvided,
        max_num_policies_to_train: Optional[int] = NotProvided,
        **kwargs,
    ) -> "AlphaStarConfig":
        super().training(**kwargs)

        # TODO: Unify the buffer API, then clean up our existing
        #  implementations of different buffers.
        if replay_buffer_capacity is not NotProvided:
            self.replay_buffer_capacity = replay_buffer_capacity
        if replay_buffer_replay_ratio is not NotProvided:
            self.replay_buffer_replay_ratio = replay_buffer_replay_ratio
        if timeout_s_sampler_manager is not NotProvided:
            self.timeout_s_sampler_manager = timeout_s_sampler_manager
        if timeout_s_learner_manager is not NotProvided:
            self.timeout_s_learner_manager = timeout_s_learner_manager
        if league_builder_config is not NotProvided:
            # Override entire `league_builder_config` if `type` key changes.
            # Update, if `type` key remains the same or is not specified.
            new_league_builder_config = deep_update(
                {"league_builder_config": self.league_builder_config},
                {"league_builder_config": league_builder_config},
                False,
                ["league_builder_config"],
                ["league_builder_config"],
            )
            self.league_builder_config = new_league_builder_config[
                "league_builder_config"
            ]
        if max_num_policies_to_train is not NotProvided:
            self.max_num_policies_to_train = max_num_policies_to_train
        if max_requests_in_flight_per_sampler_worker is not NotProvided:
            self.max_requests_in_flight_per_sampler_worker = (
                max_requests_in_flight_per_sampler_worker
            )
        if max_requests_in_flight_per_learner_worker is not NotProvided:
            self.max_requests_in_flight_per_learner_worker = (
                max_requests_in_flight_per_learner_worker
            )

        return self


@Deprecated(
    old="rllib/algorithms/alpha_star/",
    new="rllib_contrib/alpha_star/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class AlphaStar(appo.APPO):
    @classmethod
    @override(appo.APPO)
    def get_default_config(cls) -> AlphaStarConfig:
        return AlphaStarConfig()
