from ray.rllib.algorithms.crr.crr import CRR, CRRConfig
from ray.rllib.algorithms import rllib_contrib_warning


rllib_contrib_warning("CRR")

__all__ = [
    "CRR",
    "CRRConfig",
]
