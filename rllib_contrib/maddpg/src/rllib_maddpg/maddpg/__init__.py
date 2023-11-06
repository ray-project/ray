from rllib_maddpg.maddpg.maddpg import MADDPG, MADDPGConfig

from ray.tune.registry import register_trainable

__all__ = ["MADDPGConfig", "MADDPG"]

register_trainable("rllib-contrib-maddpg", MADDPG)
