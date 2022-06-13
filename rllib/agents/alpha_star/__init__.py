from ray.rllib.algorithms.alpha_star.alpha_star import (
    DEFAULT_CONFIG,
    AlphaStarConfig,
    AlphaStarTrainer,
)
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "AlphaStarConfig",
    "AlphaStarTrainer",
    "DEFAULT_CONFIG",
]


deprecation_warning(
    "ray.rllib.agents.alpha_star", "ray.rllib.algorithms.alpha_star", error=False
)
