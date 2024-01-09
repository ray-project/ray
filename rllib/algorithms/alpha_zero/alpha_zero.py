from typing import List, Optional, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)


class AlphaZeroConfig(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or AlphaZero)

        # fmt: off
        # __sphinx_doc_begin__
        # AlphaZero specific config settings:
        self.sgd_minibatch_size = 128
        self.shuffle_sequences = True
        self.num_sgd_iter = 30
        self.replay_buffer_config = {
            "type": "ReplayBuffer",
            "capacity": 1000,
            "storage_unit": "fragments",
        }
        self.num_steps_sampled_before_learning_starts = 1000
        self.lr_schedule = None
        self.vf_share_layers = False
        self.mcts_config = {
            "puct_coefficient": 1.0,
            "num_simulations": 30,
            "temperature": 1.5,
            "dirichlet_epsilon": 0.25,
            "dirichlet_noise": 0.03,
            "argmax_tree_policy": False,
            "add_dirichlet_noise": True,
        }
        self.ranked_rewards = {
            "enable": True,
            "percentile": 75,
            "buffer_max_length": 1000,
            "initialize_buffer": True,
            "num_init_rewards": 100,
        }

        self.framework_str = "torch"
        self.lr = 5e-5
        self.num_rollout_workers = 2
        self.rollout_fragment_length = 200
        self.train_batch_size = 4000
        self.batch_mode = "complete_episodes"
        self.evaluation(evaluation_config={
            "mcts_config": {
                "argmax_tree_policy": True,
                "add_dirichlet_noise": False,
            },
        })
        self.exploration_config = {
            "type": "StochasticSampling",
        }
        # __sphinx_doc_end__
        # fmt: on

        self.buffer_size = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        sgd_minibatch_size: Optional[int] = NotProvided,
        shuffle_sequences: Optional[bool] = NotProvided,
        num_sgd_iter: Optional[int] = NotProvided,
        replay_buffer_config: Optional[dict] = NotProvided,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        vf_share_layers: Optional[bool] = NotProvided,
        mcts_config: Optional[dict] = NotProvided,
        ranked_rewards: Optional[dict] = NotProvided,
        num_steps_sampled_before_learning_starts: Optional[int] = NotProvided,
        **kwargs,
    ) -> "AlphaZeroConfig":
        super().training(**kwargs)

        if sgd_minibatch_size is not NotProvided:
            self.sgd_minibatch_size = sgd_minibatch_size
        if shuffle_sequences is not NotProvided:
            self.shuffle_sequences = shuffle_sequences
        if num_sgd_iter is not NotProvided:
            self.num_sgd_iter = num_sgd_iter
        if replay_buffer_config is not NotProvided:
            self.replay_buffer_config = replay_buffer_config
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if vf_share_layers is not NotProvided:
            self.vf_share_layers = vf_share_layers
        if mcts_config is not NotProvided:
            self.mcts_config = mcts_config
        if ranked_rewards is not NotProvided:
            self.ranked_rewards.update(ranked_rewards)
        if num_steps_sampled_before_learning_starts is not NotProvided:
            self.num_steps_sampled_before_learning_starts = (
                num_steps_sampled_before_learning_starts
            )

        return self


@Deprecated(
    old="rllib/algorithms/alpha_zero/",
    new="rllib_contrib/alpha_zero/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class AlphaZero(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return AlphaZeroConfig()
