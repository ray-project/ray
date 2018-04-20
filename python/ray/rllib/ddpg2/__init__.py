from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.ddpg2.apex import ApexDDPG2Agent
from ray.rllib.ddpg2.ddpg import DDPG2Agent, DEFAULT_CONFIG

__all__ = ["DDPG2Agent", "ApexDDPG2Agent", "DEFAULT_CONFIG"]
