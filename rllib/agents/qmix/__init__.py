from ray.rllib.algorithms.qmix.qmix import DEFAULT_CONFIG
from ray.rllib.algorithms.qmix.qmix import QMix as QMixTrainer
from ray.rllib.algorithms.qmix.qmix import QMixConfig
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = ["QMixConfig", "QMixTrainer", "DEFAULT_CONFIG"]


deprecation_warning("ray.rllib.agents.qmix", "ray.rllib.algorithms.qmix", error=False)
