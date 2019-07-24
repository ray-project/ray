from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.maddpg.maddpg import MADDPGTrainer, DEFAULT_CONFIG
from ray.rllib.utils import renamed_agent

MADDPGAgent = renamed_agent(MADDPGTrainer)

__all__ = [
    "MADDPGAgent", "MADDPGTrainer",
    "DEFAULT_CONFIG"
]
