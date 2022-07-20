import ray.rllib.agents.ddpg.apex as apex  # noqa
import ray.rllib.agents.ddpg.td3 as td3  # noqa
from ray.rllib.algorithms.apex_ddpg.apex_ddpg import ApexDDPG as ApexDDPGTrainer
from ray.rllib.algorithms.ddpg.ddpg import (
    DDPGConfig,
    DDPG as DDPGTrainer,
    DEFAULT_CONFIG,
)
from ray.rllib.algorithms.td3.td3 import TD3 as TD3Trainer


__all__ = [
    "ApexDDPGTrainer",
    "DDPGConfig",
    "DDPGTrainer",
    "DEFAULT_CONFIG",
    "TD3Trainer",
]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("ray.rllib.agents.ddpg", "ray.rllib.algorithms.ddpg", error=False)
