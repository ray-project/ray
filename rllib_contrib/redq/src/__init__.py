from redq.redq import REDQ, REDQConfig

from ray.tune.registry import register_trainable

__all__ = ["REDQConfig", "REDQ"]

register_trainable("rllib-contrib-redq", REDQ)
