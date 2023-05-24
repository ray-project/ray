from rllib_a3c.a3c.a3c import A3C, A3CConfig

from ray.tune.registry import register_trainable

__all__ = ["A3CConfig", "A3C"]

register_trainable("rllib-contrib-a3c", A3C)
