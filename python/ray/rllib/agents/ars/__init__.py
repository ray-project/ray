from ray.rllib.agents.ars.ars import (ARSTrainer, DEFAULT_CONFIG)

# TODO: remove the legacy agent names
ARSAgent = ARSTrainer

__all__ = ["ARSAgent", "ARSTrainer", "DEFAULT_CONFIG"]
