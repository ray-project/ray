from ray.rllib.agents.es.es import (ESTrainer, DEFAULT_CONFIG)
from ray.rllib.utils import renamed_class

ESAgent = renamed_class(ESTrainer)

__all__ = ["ESAgent", "ESTrainer", "DEFAULT_CONFIG"]
