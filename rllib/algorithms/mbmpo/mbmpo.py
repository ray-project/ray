from typing import Optional

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.env.wrappers.model_vector_env import model_vector_env
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)


class MBMPOConfig(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or MBMPO)

        # fmt: off
        # __sphinx_doc_begin__

        self.use_gae = True
        self.lambda_ = 1.0
        self.kl_coeff = 0.0005
        self.vf_loss_coeff = 0.5
        self.entropy_coeff = 0.0
        self.clip_param = 0.5
        self.vf_clip_param = 10.0
        self.grad_clip = None
        self.kl_target = 0.01
        self.inner_adaptation_steps = 1
        self.maml_optimizer_steps = 8
        self.inner_lr = 1e-3
        self.dynamics_model = {
            "custom_model": None,
            "ensemble_size": 5,
            "fcnet_hiddens": [512, 512, 512],
            "lr": 1e-3,
            "train_epochs": 500,
            "batch_size": 500,
            "valid_split_ratio": 0.2,
            "normalize_data": True,
        }
        self.custom_vector_env = model_vector_env
        self.num_maml_steps = 10
        self.batch_mode = "complete_episodes"
        self.num_rollout_workers = 2
        self.rollout_fragment_length = 200
        self.create_env_on_local_worker = True
        self.lr = 1e-3
        # __sphinx_doc_end__
        # fmt: on

        self.vf_share_layers = DEPRECATED_VALUE
        self._disable_execution_plan_api = False

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        use_gae: Optional[float] = NotProvided,
        lambda_: Optional[float] = NotProvided,
        kl_coeff: Optional[float] = NotProvided,
        vf_loss_coeff: Optional[float] = NotProvided,
        entropy_coeff: Optional[float] = NotProvided,
        clip_param: Optional[float] = NotProvided,
        vf_clip_param: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        kl_target: Optional[float] = NotProvided,
        inner_adaptation_steps: Optional[int] = NotProvided,
        maml_optimizer_steps: Optional[int] = NotProvided,
        inner_lr: Optional[float] = NotProvided,
        dynamics_model: Optional[dict] = NotProvided,
        custom_vector_env: Optional[type] = NotProvided,
        num_maml_steps: Optional[int] = NotProvided,
        **kwargs,
    ) -> "MBMPOConfig":
        super().training(**kwargs)

        if use_gae is not NotProvided:
            self.use_gae = use_gae
        if lambda_ is not NotProvided:
            self.lambda_ = lambda_
        if kl_coeff is not NotProvided:
            self.kl_coeff = kl_coeff
        if vf_loss_coeff is not NotProvided:
            self.vf_loss_coeff = vf_loss_coeff
        if entropy_coeff is not NotProvided:
            self.entropy_coeff = entropy_coeff
        if clip_param is not NotProvided:
            self.clip_param = clip_param
        if vf_clip_param is not NotProvided:
            self.vf_clip_param = vf_clip_param
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        if kl_target is not NotProvided:
            self.kl_target = kl_target
        if inner_adaptation_steps is not NotProvided:
            self.inner_adaptation_steps = inner_adaptation_steps
        if maml_optimizer_steps is not NotProvided:
            self.maml_optimizer_steps = maml_optimizer_steps
        if inner_lr is not NotProvided:
            self.inner_lr = inner_lr
        if dynamics_model is not NotProvided:
            self.dynamics_model.update(dynamics_model)
        if custom_vector_env is not NotProvided:
            self.custom_vector_env = custom_vector_env
        if num_maml_steps is not NotProvided:
            self.num_maml_steps = num_maml_steps

        return self


@Deprecated(
    old="rllib/algorithms/mbmpo/",
    new="rllib_contrib/mbmpo/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class MBMPO(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return MBMPOConfig()
