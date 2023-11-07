from ray.rllib.algorithms.apex_ddpg.apex_ddpg import (
    ApexDDPG,
    ApexDDPGConfig,
)
from ray.rllib.algorithms import rllib_contrib_warning


rllib_contrib_warning("ApexDDPG")

__all__ = [
    "ApexDDPG",
    "ApexDDPGConfig",
]
