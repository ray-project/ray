"""
TQC (Truncated Quantile Critics) Algorithm.

Paper: https://arxiv.org/abs/2005.04269
"Controlling Overestimation Bias with Truncated Mixture of Continuous
Distributional Quantile Critics"

TQC extends SAC by using distributional RL with quantile regression to
control overestimation bias in the Q-function.
"""

import logging
from typing import Optional, Type, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.sac.sac import SAC, SACConfig
from ray.rllib.core.learner import Learner
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import RLModuleSpecType

logger = logging.getLogger(__name__)


class TQCConfig(SACConfig):
    """Configuration for the TQC algorithm.

    TQC extends SAC with distributional critics using quantile regression.

    Example:
        >>> from ray.rllib.algorithms.tqc import TQCConfig
        >>> config = (
        ...     TQCConfig()
        ...     .environment("Pendulum-v1")
        ...     .training(
        ...         n_quantiles=25,
        ...         n_critics=2,
        ...         top_quantiles_to_drop_per_net=2,
        ...     )
        ... )
        >>> algo = config.build()
    """

    def __init__(self, algo_class=None):
        """Initializes a TQCConfig instance."""
        super().__init__(algo_class=algo_class or TQC)

        # TQC-specific parameters
        self.n_quantiles = 25
        self.n_critics = 2
        self.top_quantiles_to_drop_per_net = 2

    @override(SACConfig)
    def training(
        self,
        *,
        n_quantiles: Optional[int] = NotProvided,
        n_critics: Optional[int] = NotProvided,
        top_quantiles_to_drop_per_net: Optional[int] = NotProvided,
        **kwargs,
    ):
        """Sets the training-related configuration.

        Args:
            n_quantiles: Number of quantiles for each critic network.
                Default is 25.
            n_critics: Number of critic networks. Default is 2.
            top_quantiles_to_drop_per_net: Number of quantiles to drop per
                network when computing the target Q-value. This controls
                the overestimation bias. Default is 2.
            **kwargs: Additional arguments passed to SACConfig.training().

        Returns:
            This updated TQCConfig object.
        """
        super().training(**kwargs)

        if n_quantiles is not NotProvided:
            self.n_quantiles = n_quantiles
        if n_critics is not NotProvided:
            self.n_critics = n_critics
        if top_quantiles_to_drop_per_net is not NotProvided:
            self.top_quantiles_to_drop_per_net = top_quantiles_to_drop_per_net

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        """Validates the TQC configuration."""
        super().validate()

        # Validate TQC-specific parameters
        if self.n_quantiles < 1:
            raise ValueError(f"`n_quantiles` must be >= 1, got {self.n_quantiles}")
        if self.n_critics < 1:
            raise ValueError(f"`n_critics` must be >= 1, got {self.n_critics}")

        # Ensure top_quantiles_to_drop_per_net is non-negative
        if self.top_quantiles_to_drop_per_net < 0:
            raise ValueError(
                f"`top_quantiles_to_drop_per_net` must be >= 0, got "
                f"{self.top_quantiles_to_drop_per_net}"
            )

        # Ensure we don't drop more quantiles than we have
        total_quantiles = self.n_quantiles * self.n_critics
        quantiles_to_drop = self.top_quantiles_to_drop_per_net * self.n_critics
        if quantiles_to_drop >= total_quantiles:
            raise ValueError(
                f"Cannot drop {quantiles_to_drop} quantiles when only "
                f"{total_quantiles} total quantiles are available. "
                f"Reduce `top_quantiles_to_drop_per_net` or increase "
                f"`n_quantiles` or `n_critics`."
            )

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpecType:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.tqc.torch.default_tqc_torch_rl_module import (
                DefaultTQCTorchRLModule,
            )

            return RLModuleSpec(module_class=DefaultTQCTorchRLModule)
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. Use `torch`."
            )

    @override(AlgorithmConfig)
    def get_default_learner_class(self) -> Union[Type["Learner"], str]:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.tqc.torch.tqc_torch_learner import (
                TQCTorchLearner,
            )

            return TQCTorchLearner
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. Use `torch`."
            )

    @property
    @override(AlgorithmConfig)
    def _model_config_auto_includes(self):
        return super()._model_config_auto_includes | {
            "n_quantiles": self.n_quantiles,
            "n_critics": self.n_critics,
            "top_quantiles_to_drop_per_net": self.top_quantiles_to_drop_per_net,
        }


class TQC(SAC):
    """TQC (Truncated Quantile Critics) Algorithm.

    TQC extends SAC by using distributional critics with quantile regression
    and truncating the top quantiles to control overestimation bias.

    Key differences from SAC:
    - Uses multiple critic networks, each outputting multiple quantiles
    - Computes target Q-values by sorting and truncating top quantiles
    - Uses quantile Huber loss for critic training

    See the paper for more details:
    https://arxiv.org/abs/2005.04269
    """

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> TQCConfig:
        return TQCConfig()
