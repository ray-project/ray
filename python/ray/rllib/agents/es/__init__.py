from ray.rllib.agents.es.es import (ESTrainer, DEFAULT_CONFIG)

# TODO: remove the legacy agent names
ESAgent = ESTrainer

__all__ = ["ESAgent", "ESTrainer", "DEFAULT_CONFIG"]
