from ray.rllib.algorithms.qmix.qmix import QMixConfig, QMixTrainer, DEFAULT_CONFIG

__all__ = ["QMixConfig", "QMixTrainer", "DEFAULT_CONFIG"]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("ray.rllib.agents.qmix", "ray.rllib.algorithms.qmix", error=False)
