from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.ddpg.apex import ApexDDPGTrainer
from ray.rllib.agents.ddpg.ddpg import DDPGTrainer, DEFAULT_CONFIG
from ray.rllib.utils import renamed_class

ApexDDPGAgent = renamed_class(ApexDDPGTrainer)
DDPGAgent = renamed_class(DDPGTrainer)

__all__ = [
    "DDPGAgent", "ApexDDPGAgent", "DDPGTrainer", "ApexDDPGTrainer",
    "DEFAULT_CONFIG"
]
