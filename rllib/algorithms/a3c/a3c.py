from typing import List, Optional, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    ALGO_DEPRECATION_WARNING,
    Deprecated,
    deprecation_warning,
)


class A3CConfig(AlgorithmConfig):
    def __init__(self, algo_class=None):
        """Initializes a A3CConfig instance."""
        super().__init__(algo_class=algo_class or A3C)

        # fmt: off
        # __sphinx_doc_begin__
        #
        # A3C specific settings.
        self.use_critic = True
        self.use_gae = True
        self.lambda_ = 1.0

        self.grad_clip = 40.0
        # Note: Only when using _enable_new_api_stack=True can the clipping mode be
        # configured by the user. On the old API stack, RLlib will always clip by
        # global_norm, no matter the value of `grad_clip_by`.
        self.grad_clip_by = "global_norm"

        self.lr_schedule = None
        self.vf_loss_coeff = 0.5
        self.entropy_coeff = 0.01
        self.entropy_coeff_schedule = None
        self.sample_async = False

        # Override some of AlgorithmConfig's default values with PPO-specific values.
        self.num_rollout_workers = 2
        self.rollout_fragment_length = 10
        self.lr = 0.0001
        # Min time (in seconds) per reporting.
        # This causes not every call to `training_iteration` to be reported,
        # but to wait until n seconds have passed and then to summarize the
        # thus far collected results.
        self.min_time_s_per_iteration = 5
        self.exploration_config = {
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }
        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        use_critic: Optional[bool] = NotProvided,
        use_gae: Optional[bool] = NotProvided,
        lambda_: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        vf_loss_coeff: Optional[float] = NotProvided,
        entropy_coeff: Optional[float] = NotProvided,
        entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        sample_async: Optional[bool] = NotProvided,
        **kwargs,
    ) -> "A3CConfig":
        super().training(**kwargs)

        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if use_critic is not NotProvided:
            self.lr_schedule = use_critic
        if use_gae is not NotProvided:
            self.use_gae = use_gae
        if lambda_ is not NotProvided:
            self.lambda_ = lambda_
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        if vf_loss_coeff is not NotProvided:
            self.vf_loss_coeff = vf_loss_coeff
        if entropy_coeff is not NotProvided:
            self.entropy_coeff = entropy_coeff
        if entropy_coeff_schedule is not NotProvided:
            self.entropy_coeff_schedule = entropy_coeff_schedule

        # Deprecated settings.
        if sample_async is not False:
            deprecation_warning(
                old="A3CConfig.training(sample_async=True)",
                help="AsyncSampler is not supported anymore.",
                error=True,
            )

        return self


@Deprecated(
    old="rllib/algorithms/a3c/",
    new="rllib_contrib/a3c/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class A3C(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return A3CConfig()
