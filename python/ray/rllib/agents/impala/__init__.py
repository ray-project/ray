from ray.rllib.agents.impala.impala import ImpalaTrainer, DEFAULT_CONFIG
from ray.rllib.utils import renamed_class

ImpalaAgent = renamed_class(ImpalaTrainer)

__all__ = ["ImpalaAgent", "ImpalaTrainer", "DEFAULT_CONFIG"]
