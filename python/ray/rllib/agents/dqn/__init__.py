from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.dqn.apex import ApexTrainer
from ray.rllib.agents.dqn.dqn import DQNTrainer, DEFAULT_CONFIG
from ray.rllib.utils import renamed_class

DQNAgent = renamed_class(DQNTrainer)
ApexAgent = renamed_class(ApexTrainer)

__all__ = [
    "DQNAgent", "ApexAgent", "ApexTrainer", "DQNTrainer", "DEFAULT_CONFIG"
]
