from ray.rllib.algorithms.ddpg.apex import ApexDDPGTrainer
from ray.rllib.algorithms.ddpg.ddpg import DDPGConfig, DDPGTrainer, DEFAULT_CONFIG
from ray.rllib.algorithms.ddpg.td3 import TD3Trainer


__all__ = [
    "ApexDDPGTrainer",
    "DDPGConfig",
    "DDPGTrainer",
    "DEFAULT_CONFIG",
    "TD3Trainer",
]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("ray.rllib.agents.ddpg", "ray.rllib.algorithms.ddpg", error=False)
