from ...agents.ars.ars import (ARSTrainer, DEFAULT_CONFIG)
from ...utils import renamed_agent

ARSAgent = renamed_agent(ARSTrainer)

__all__ = ["ARSAgent", "ARSTrainer", "DEFAULT_CONFIG"]
