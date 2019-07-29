from ...agents.es.es import (ESTrainer, DEFAULT_CONFIG)
from ...utils import renamed_agent

ESAgent = renamed_agent(ESTrainer)

__all__ = ["ESAgent", "ESTrainer", "DEFAULT_CONFIG"]
