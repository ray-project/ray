from ray.rllib.agents.ars.ars import (ARSTrainer, DEFAULT_CONFIG)
from ray.rllib.utils import renamed_agent

ARSAgent = renamed_agent(ARSTrainer)

__all__ = ["ARSAgent", "ARSTrainer", "DEFAULT_CONFIG"]
