from ...agents.impala.impala import ImpalaTrainer, DEFAULT_CONFIG
from ...utils import renamed_agent

ImpalaAgent = renamed_agent(ImpalaTrainer)

__all__ = ["ImpalaAgent", "ImpalaTrainer", "DEFAULT_CONFIG"]
