from ...agents.pg.pg import PGTrainer, DEFAULT_CONFIG
from ...utils import renamed_agent

PGAgent = renamed_agent(PGTrainer)

__all__ = ["PGAgent", "PGTrainer", "DEFAULT_CONFIG"]
