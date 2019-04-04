from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.dqn.apex import ApexTrainer
from ray.rllib.agents.dqn.dqn import DQNTrainer, DEFAULT_CONFIG

# TODO: remove the legacy agent names
DQNAgent = DQNTrainer
ApexAgent = ApexTrainer

__all__ = ["DQNAgent", "ApexAgent", "ApexTrainer", "DQNTrainer", "DEFAULT_CONFIG"]
