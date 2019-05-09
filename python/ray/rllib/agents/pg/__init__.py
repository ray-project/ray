from ray.rllib.agents.pg.pg import PGTrainer, DEFAULT_CONFIG
from ray.rllib.utils import renamed_class

PGAgent = renamed_class(PGTrainer)

__all__ = ["PGAgent", "PGTrainer", "DEFAULT_CONFIG"]
