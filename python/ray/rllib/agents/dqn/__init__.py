from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.dqn.apex import ApexAgent
from ray.rllib.agents.dqn.dqn import DQNAgent, DEFAULT_CONFIG
from ray.rllib.agents.dqn.r2d2 import R2D2Agent

__all__ = ["ApexAgent", "DQNAgent", "R2D2Agent", "DEFAULT_CONFIG"]
