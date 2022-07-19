from ray.rllib.algorithms.alpha_star.alpha_star import (
    AlphaStarConfig,
    AlphaStarTrainer,
    DEFAULT_CONFIG,
)

__all__ = [
    "AlphaStarConfig",
    "AlphaStarTrainer",
    "DEFAULT_CONFIG",
]


from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    "ray.rllib.agents.alpha_star", "ray.rllib.algorithms.alpha_star", error=False
)
