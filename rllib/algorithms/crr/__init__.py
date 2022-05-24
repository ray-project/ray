from .algorithm_crr import CRR
from .config_crr import CRRConfig
from rllib.algorithms.crr.torch.policy_torch_crr import CRRTorchPolicy

__all__ = [
    "CRR",
    "CRRConfig",
    "CRRTorchPolicy",
]