from ray.rllib.algorithms.bandit.bandit import (
    BanditLinTSTrainer,
    BanditLinUCBTrainer,
    BanditLinTSConfig,
    BanditLinUCBConfig,
)

__all__ = [
    "BanditLinTSTrainer",
    "BanditLinUCBTrainer",
    "BanditLinTSConfig",
    "BanditLinUCBConfig",
]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    "ray.rllib.agents.bandits", "ray.rllib.algorithms.bandits", error=False
)
