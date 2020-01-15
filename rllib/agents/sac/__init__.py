from ray.rllib.agents.sac.sac import SACTrainer, DEFAULT_CONFIG
from ray.rllib.utils import renamed_agent

SACAgent = renamed_agent(SACTrainer)

__all__ = [
    "SACTrainer",
    "DEFAULT_CONFIG",
]
