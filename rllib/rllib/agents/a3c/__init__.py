from ...agents.a3c.a3c import A3CTrainer, DEFAULT_CONFIG
from ...agents.a3c.a2c import A2CTrainer
from ...utils import renamed_agent

A2CAgent = renamed_agent(A2CTrainer)
A3CAgent = renamed_agent(A3CTrainer)

__all__ = [
    "A2CAgent", "A3CAgent", "A2CTrainer", "A3CTrainer", "DEFAULT_CONFIG"
]
