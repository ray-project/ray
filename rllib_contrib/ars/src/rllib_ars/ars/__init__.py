from rllib_ars.ars.ars import ARS, ARSConfig
from rllib_ars.ars.ars_tf_policy import ARSTFPolicy
from rllib_ars.ars.ars_torch_policy import ARSTorchPolicy

from ray.tune.registry import register_trainable

__all__ = ["ARSConfig", "ARS", "ARSTorchPolicy", "ARSTFPolicy"]

register_trainable("rllib-contrib-ars", ARS)
