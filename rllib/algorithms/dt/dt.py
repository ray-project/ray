from typing import List, Optional, Dict, Any, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.utils import deep_update
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


class DTConfig(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or DT)
        # fmt: off
        # __sphinx_doc_begin__
        self.target_return = None
        self.horizon = None
        self.model = {
            "max_seq_len": 5,
        }
        self.embed_dim = 128
        self.num_layers = 2
        self.num_heads = 1
        self.embed_pdrop = 0.1
        self.resid_pdrop = 0.1
        self.attn_pdrop = 0.1
        self.lr = 1e-4
        self.lr_schedule = None
        self.optimizer = {
            "weight_decay": 1e-4,
            "betas": (0.9, 0.95),
        }
        self.grad_clip = None
        self.loss_coef_actions = 1
        self.loss_coef_obs = 0
        self.loss_coef_returns_to_go = 0
        # __sphinx_doc_end__
        # fmt: on

        self.min_train_timesteps_per_iteration = 5000
        self.offline_sampling = True
        self.postprocess_inputs = True
        self.discount = None

    def training(
        self,
        *,
        replay_buffer_config: Optional[Dict[str, Any]] = NotProvided,
        embed_dim: Optional[int] = NotProvided,
        num_layers: Optional[int] = NotProvided,
        num_heads: Optional[int] = NotProvided,
        embed_pdrop: Optional[float] = NotProvided,
        resid_pdrop: Optional[float] = NotProvided,
        attn_pdrop: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        loss_coef_actions: Optional[float] = NotProvided,
        loss_coef_obs: Optional[float] = NotProvided,
        loss_coef_returns_to_go: Optional[float] = NotProvided,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        horizon: Optional[int] = NotProvided,
        **kwargs,
    ) -> "DTConfig":
        super().training(**kwargs)
        if replay_buffer_config is not NotProvided:
            new_replay_buffer_config = deep_update(
                {"replay_buffer_config": self.replay_buffer_config},
                {"replay_buffer_config": replay_buffer_config},
                False,
                ["replay_buffer_config"],
                ["replay_buffer_config"],
            )
            self.replay_buffer_config = new_replay_buffer_config["replay_buffer_config"]
        if embed_dim is not NotProvided:
            self.embed_dim = embed_dim
        if num_layers is not NotProvided:
            self.num_layers = num_layers
        if num_heads is not NotProvided:
            self.num_heads = num_heads
        if embed_pdrop is not NotProvided:
            self.embed_pdrop = embed_pdrop
        if resid_pdrop is not NotProvided:
            self.resid_pdrop = resid_pdrop
        if attn_pdrop is not NotProvided:
            self.attn_pdrop = attn_pdrop
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if loss_coef_actions is not NotProvided:
            self.loss_coef_actions = loss_coef_actions
        if loss_coef_obs is not NotProvided:
            self.loss_coef_obs = loss_coef_obs
        if loss_coef_returns_to_go is not NotProvided:
            self.loss_coef_returns_to_go = loss_coef_returns_to_go
        if horizon is not NotProvided:
            self.horizon = horizon

        return self

    def evaluation(
        self,
        *,
        target_return: Optional[float] = NotProvided,
        **kwargs,
    ) -> "DTConfig":
        super().evaluation(**kwargs)
        if target_return is not NotProvided:
            self.target_return = target_return

        return self


@Deprecated(
    old="rllib/algorithms/dt/",
    new="rllib_contrib/dt/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class DT(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return DTConfig()
