from ray.rllib.algorithms.dreamer.dreamer import DEFAULT_CONFIG
from ray.rllib.algorithms.dreamer.dreamer import Dreamer as DREAMERTrainer
from ray.rllib.algorithms.dreamer.dreamer import DreamerConfig
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "DreamerConfig",
    "DREAMERTrainer",
    "DEFAULT_CONFIG",
]


deprecation_warning(
    "ray.rllib.agents.dreamer", "ray.rllib.algorithms.dreamer", error=False
)
