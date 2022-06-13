from ray.rllib.algorithms.mbmpo.mbmpo import DEFAULT_CONFIG
from ray.rllib.algorithms.mbmpo.mbmpo import MBMPO as MBMPOTrainer
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "MBMPOTrainer",
    "DEFAULT_CONFIG",
]


deprecation_warning("ray.rllib.agents.mbmpo", "ray.rllib.algorithms.mbmpo", error=False)
