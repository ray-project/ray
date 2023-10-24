from ray.rllib.algorithms.ddpg.ddpg import DDPG, DDPGConfig
from ray.rllib.algorithms import rllib_contrib_warning


rllib_contrib_warning("DDPG")


__all__ = [
    "DDPG",
    "DDPGConfig",
]
