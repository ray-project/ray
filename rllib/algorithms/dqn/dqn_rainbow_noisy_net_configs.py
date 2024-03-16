from dataclasses import dataclass

from ray.rllib.core.models.base import Encoder
from ray.rllib.core.models.configs import _framework_implemented, _MLPConfig
from ray.rllib.utils.annotations import ExperimentalAPI, override


@ExperimentalAPI
@dataclass
class NoisyMLPConfig(_MLPConfig):
    std_init: float = 0.1

    @override(_MLPConfig)
    def _validate(self, framework: str = "torch"):
        """Makes sure that standard deviation is positive."""
        super()._validate(framework=framework)

        if self.std_init < 0.0:
            raise ValueError(
                f"`std_init` ({self.std_init}) of `NoisyMLPConfig must be "
                "non-negative."
            )


@ExperimentalAPI
@dataclass
class NoisyMLPEncoderConfig(NoisyMLPConfig):
    @_framework_implemented()
    def build(self, framework: str = "torch") -> "Encoder":
        self._validate(framework)

        if framework == "torch":
            from ray.rllib.algorithms.dqn.torch.dqn_rainbow_torch_noisy_net import (
                TorchNoisyMLPEncoder,
            )

            return TorchNoisyMLPEncoder(self)
        else:
            raise ValueError(
                "`NoisyMLPEncoder` is not implemented for framework " f"{framework}. "
            )


@ExperimentalAPI
@dataclass
class NoisyMLPHeadConfig(NoisyMLPConfig):
    @_framework_implemented()
    def build(self, framework: str = "torch") -> "Encoder":
        self._validate(framework)

        if framework == "torch":
            from ray.rllib.algorithms.dqn.torch.dqn_rainbow_torch_noisy_net import (
                TorchNoisyMLPHead,
            )

            return TorchNoisyMLPHead(self)
        else:
            raise ValueError(
                "`NoisyMLPHead` is not implemented for framework " f"{framework}. "
            )
