from ray.rllib.algorithms.mbmpo.mbmpo import MBMPOTrainer, DEFAULT_CONFIG

__all__ = [
    "MBMPOTrainer",
    "DEFAULT_CONFIG",
]


from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("ray.rllib.agents.mbmpo", "ray.rllib.algorithms.mbmpo", error=False)
