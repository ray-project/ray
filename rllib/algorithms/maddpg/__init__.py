from ray.rllib.algorithms.maddpg.maddpg import (
    MADDPG,
    MADDPGConfig,
)
from ray.rllib.algorithms import rllib_contrib_warning


rllib_contrib_warning("MADDDPG")

__all__ = ["MADDPGConfig", "MADDPG"]
