from rllib_ddpg.ddpg.ddpg import DDPG, DDPGConfig

from ray.tune.registry import register_trainable

__all__ = ["DDPGConfig", "DDPG"]

register_trainable("rllib-contrib-ddpg", DDPG)
