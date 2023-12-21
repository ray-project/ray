from rllib_qmix.qmix.qmix import QMix, QMixConfig
from rllib_qmix.qmix.qmix_policy import QMixTorchPolicy

from ray.tune.registry import register_trainable

__all__ = ["QMixConfig", "QMix", "QMixTorchPolicy"]

register_trainable("rllib-contrib-qmix", QMix)
