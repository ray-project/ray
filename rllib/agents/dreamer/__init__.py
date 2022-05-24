from ray.rllib.algorithms.dreamer.dreamer import (
    DREAMERConfig,
    DREAMERTrainer,
    DEFAULT_CONFIG,
)

__all__ = [
    "DREAMERConfig",
    "DREAMERTrainer",
    "DEFAULT_CONFIG",
]


from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    "ray.rllib.agents.dreamer", "ray.rllib.algorithms.dreamer", error=False
)
