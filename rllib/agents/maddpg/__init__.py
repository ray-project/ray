from ray.rllib.algorithms.maddpg.maddpg import (
    MADDPGTrainer,
    MADDPGTFPolicy,
    DEFAULT_CONFIG,
)

__all__ = [
    "MADDPGTrainer",
    "MADDPGTFPolicy",
    "DEFAULT_CONFIG",
]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    "ray.rllib.agents.maddpg",
    "ray.rllib.algorithms.maddpg",
    error=False,
)
