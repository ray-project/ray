import ray.rllib.agents.ddpg.apex as apex  # noqa
import ray.rllib.agents.ddpg.td3 as td3  # noqa
from ray.rllib.algorithms.apex_ddpg.apex_ddpg import ApexDDPG as ApexDDPGTrainer
from ray.rllib.algorithms.ddpg.ddpg import DDPG as DDPGTrainer
from ray.rllib.algorithms.ddpg.ddpg import DEFAULT_CONFIG, DDPGConfig
from ray.rllib.algorithms.td3.td3 import TD3 as TD3Trainer
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "ApexDDPGTrainer",
    "DDPGConfig",
    "DDPGTrainer",
    "DEFAULT_CONFIG",
    "TD3Trainer",
]


deprecation_warning("ray.rllib.agents.ddpg", "ray.rllib.algorithms.ddpg", error=False)
