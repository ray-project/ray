from rllib_td3.td3.td3 import TD3, TD3Config

from ray.tune.registry import register_trainable

__all__ = ["TD3Config", "TD3"]

register_trainable("rllib-contrib-td3", TD3)
