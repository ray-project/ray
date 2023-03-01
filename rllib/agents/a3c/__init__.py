import ray.rllib.agents.a3c.a2c as a2c  # noqa
from ray.rllib.algorithms.a2c.a2c import (
    A2CConfig,
    A2C as A2CTrainer,
    A2C_DEFAULT_CONFIG,
)
from ray.rllib.algorithms.a3c.a3c import A3CConfig, A3C as A3CTrainer, DEFAULT_CONFIG
from ray.rllib.utils.deprecation import deprecation_warning


__all__ = [
    "A2CConfig",
    "A2C_DEFAULT_CONFIG",  # deprecated
    "A2CTrainer",
    "A3CConfig",
    "A3CTrainer",
    "DEFAULT_CONFIG",  # A3C default config (deprecated)
]

deprecation_warning(
    "ray.rllib.agents.a3c", "ray.rllib.algorithms.[a3c|a2c]", error=True
)
