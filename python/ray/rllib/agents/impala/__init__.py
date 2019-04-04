from ray.rllib.agents.impala.impala import ImpalaTrainer, DEFAULT_CONFIG

# TODO: remove the legacy agent names
ImpalaAgent = ImpalaTrainer

__all__ = ["ImpalaAgent", "ImpalaTrainer", "DEFAULT_CONFIG"]
