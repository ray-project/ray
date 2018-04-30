from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.ddpg.apex import ApexDDPGAgent
from ray.rllib.ddpg.ddpg import DDPGAgent, DEFAULT_CONFIG

__all__ = ["DDPGAgent", "ApexDDPGAgent", "DEFAULT_CONFIG"]
