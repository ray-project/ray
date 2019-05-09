from ray.rllib.agents.ars.ars import (ARSTrainer, DEFAULT_CONFIG)
from ray.rllib.utils import renamed_class

ARSAgent = renamed_class(ARSTrainer)

__all__ = ["ARSAgent", "ARSTrainer", "DEFAULT_CONFIG"]
