from ray.rllib.algorithms.impala.impala import (
    DEFAULT_CONFIG,
    ImpalaConfig,
    Impala as ImpalaTrainer,
)
from ray.rllib.utils.deprecation import deprecation_warning


__all__ = [
    "ImpalaConfig",
    "ImpalaTrainer",
    "DEFAULT_CONFIG",
]

deprecation_warning(
    "ray.rllib.agents.impala", "ray.rllib.algorithms.impala", error=True
)
