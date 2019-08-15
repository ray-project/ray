from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.dqn.apex import ApexTrainer
from ray.rllib.agents.dqn.dqn import DQNTrainer, SimpleQTrainer, DEFAULT_CONFIG
from ray.rllib.utils import renamed_agent

DQNAgent = renamed_agent(DQNTrainer)
ApexAgent = renamed_agent(ApexTrainer)

__all__ = [
    "DQNAgent", "ApexAgent", "ApexTrainer", "DQNTrainer", "DEFAULT_CONFIG",
    "SimpleQTrainer"
]
