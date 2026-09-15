"""TQC PyTorch implementations."""

from ray.rllib.algorithms.tqc.torch.default_tqc_torch_rl_module import (
    DefaultTQCTorchRLModule,
)
from ray.rllib.algorithms.tqc.torch.tqc_torch_learner import TQCTorchLearner

__all__ = [
    "DefaultTQCTorchRLModule",
    "TQCTorchLearner",
]
