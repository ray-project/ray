from ray.rllib.agents.a3c.a3c import A3CTrainer, DEFAULT_CONFIG
from ray.rllib.agents.a3c.a2c import A2CTrainer

# TODO: remove the legacy agent names
A2CAgent = A2CTrainer
A3CAgent = A3CTrainer

__all__ = [
    "A2CAgent", "A3CAgent", "A2CTrainer", "A3CTrainer", "DEFAULT_CONFIG"
]
