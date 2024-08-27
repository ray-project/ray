from rllib_a2c.a2c.a2c import A2C, A2CConfig

from ray.tune.registry import register_trainable

__all__ = ["A2CConfig", "A2C"]

register_trainable("rllib-contrib-a2c", A2C)
