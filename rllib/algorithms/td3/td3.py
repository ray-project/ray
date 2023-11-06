from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ddpg.ddpg import DDPG, DDPGConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)


class TD3Config(DDPGConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or TD3)

        # fmt: off
        # __sphinx_doc_begin__
        self.twin_q = True
        self.policy_delay = 2
        self.smooth_target_policy = True,
        self.l2_reg = 0.0
        self.tau = 5e-3
        self.train_batch_size = 100
        self.replay_buffer_config = {
            "type": "MultiAgentReplayBuffer",
            "prioritized_replay": DEPRECATED_VALUE,
            "capacity": 1000000,
            "worker_side_prioritization": False,
        }
        self.num_steps_sampled_before_learning_starts = 10000
        self.exploration_config = {
            "type": "GaussianNoise",
            "random_timesteps": 10000,
            "stddev": 0.1,
            "initial_scale": 1.0,
            "final_scale": 1.0,
            "scale_timesteps": 1,
        }
        # __sphinx_doc_end__
        # fmt: on


@Deprecated(
    old="rllib/algorithms/td3/",
    new="rllib_contrib/td3/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class TD3(DDPG):
    @classmethod
    @override(DDPG)
    def get_default_config(cls) -> AlgorithmConfig:
        return TD3Config()
