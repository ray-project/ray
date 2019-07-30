from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.sac.apex import ApexSACTrainer
from ray.rllib.agents.sac.sac import SACTrainer, DEFAULT_CONFIG
from ray.rllib.agents.sac.td3 import TD3Trainer
from ray.rllib.utils import renamed_agent

ApexSACAgent = renamed_agent(ApexSACTrainer)
SACAgent = renamed_agent(SACTrainer)

__all__ = [
    "SACTrainer", "DEFAULT_CONFIG",
]
