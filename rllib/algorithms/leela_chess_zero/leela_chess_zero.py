from typing import List, Optional, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers import PrioritizedReplayBuffer
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)


class LeelaChessZeroConfig(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or LeelaChessZero)

        # fmt: off
        # __sphinx_doc_begin__
        self.sgd_minibatch_size = 256
        self.shuffle_sequences = True
        self.num_sgd_iter = 30
        self.replay_buffer_config = {
            "_enable_replay_buffer_api": True,
            "type": "MultiAgentReplayBuffer",
            "underlying_replay_buffer_config": {
                "type": PrioritizedReplayBuffer,
                "capacity": 10000, "storage_unit": "episodes",
                "prioritized_replay_alpha": 0.6, "prioritized_replay_beta": 0.4,
                "prioritized_replay_eps": 1e-6,
            },
        }
        self.num_steps_sampled_before_learning_starts = 1000
        self.lr_schedule = None
        self.vf_share_layers = False
        self.mcts_config = {
            "puct_coefficient": 2**0.5,
            "num_simulations": 25,
            "temperature": 1.5,
            "dirichlet_epsilon": 0.25,
            "dirichlet_noise": 0.03,
            "argmax_tree_policy": True,
            "add_dirichlet_noise": True,
            "epsilon": 0.05,
            "turn_based_flip": True,
            "argmax_child_value": True,
        }
        self.framework_str = "torch"
        self.lr = 1e-3
        self.num_rollout_workers = 8
        self.rollout_fragment_length = 200
        self.train_batch_size = 2048
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
        lr: Optional[float] = NotProvided,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        vf_share_layers: Optional[bool] = NotProvided,
        mcts_config: Optional[dict] = NotProvided,
        num_steps_sampled_before_learning_starts: Optional[int] = NotProvided,
        model: Optional[dict] = NotProvided,
        **kwargs,
    ) -> "LeelaChessZeroConfig":
        super().training(**kwargs)

        if sgd_minibatch_size is not NotProvided:
            self.sgd_minibatch_size = sgd_minibatch_size
        if shuffle_sequences is not NotProvided:
            self.shuffle_sequences = shuffle_sequences
        if num_sgd_iter is not NotProvided:
            self.num_sgd_iter = num_sgd_iter
        if replay_buffer_config is not NotProvided:
            self.replay_buffer_config = replay_buffer_config
        if lr is not NotProvided:
            self.lr = lr
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if vf_share_layers is not NotProvided:
            self.vf_share_layers = vf_share_layers
        if mcts_config is not NotProvided:
            # only assign provided keys
            for k, v in mcts_config.items():
                self.mcts_config[k] = v
        if num_steps_sampled_before_learning_starts is not NotProvided:
            self.num_steps_sampled_before_learning_starts = (
                num_steps_sampled_before_learning_starts
            )
        if model is not NotProvided:
            self.model = model

        return self


@Deprecated(
    old="rllib/algorithms/leela_chess_zero/",
    new="rllib_contrib/leela_chess_zero/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class LeelaChessZero(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return LeelaChessZeroConfig()
