from typing import Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dqn import DQN, DQNConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)


class R2D2Config(DQNConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or R2D2)

        # fmt: off
        # __sphinx_doc_begin__
        self.zero_init_states = True
        self.use_h_function = True
        self.h_function_epsilon = 1e-3
        self.adam_epsilon = 1e-3
        self.lr = 1e-4
        self.gamma = 0.997
        self.train_batch_size = 1000
        self.target_network_update_freq = 1000
        self.training_intensity = 150
        self.replay_buffer_config = {
            "type": "MultiAgentReplayBuffer",
            "prioritized_replay": DEPRECATED_VALUE,
            "capacity": 100000,
            "storage_unit": "sequences",
            "replay_sequence_length": -1,
            "replay_burn_in": 0,
        }
        self.num_rollout_workers = 2
        self.batch_mode = "complete_episodes"
        # fmt: on
        # __sphinx_doc_end__
        self.burn_in = DEPRECATED_VALUE

    def training(
        self,
        *,
        zero_init_states: Optional[bool] = NotProvided,
        use_h_function: Optional[bool] = NotProvided,
        h_function_epsilon: Optional[float] = NotProvided,
        **kwargs,
    ) -> "R2D2Config":
        super().training(**kwargs)

        if zero_init_states is not NotProvided:
            self.zero_init_states = zero_init_states
        if use_h_function is not NotProvided:
            self.use_h_function = use_h_function
        if h_function_epsilon is not NotProvided:
            self.h_function_epsilon = h_function_epsilon

        return self


@Deprecated(
    old="rllib/algorithms/r2d2/",
    new="rllib_contrib/r2d2/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class R2D2(DQN):
    @classmethod
    @override(DQN)
    def get_default_config(cls) -> AlgorithmConfig:
        return R2D2Config()
