from ray.rllib.algorithms.dreamer.dreamer import (
    DreamerConfig,
    Dreamer as DREAMERTrainer,
    DEFAULT_CONFIG,
)

__all__ = [
    "DreamerConfig",
    "DREAMERTrainer",
    "DEFAULT_CONFIG",
]


from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    "ray.rllib.agents.dreamer", "ray.rllib.algorithms.dreamer", error=True
)
