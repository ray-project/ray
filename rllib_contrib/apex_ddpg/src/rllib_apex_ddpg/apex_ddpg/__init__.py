from rllib_apex_ddpg.apex_ddpg.apex_ddpg import ApexDDPG, ApexDDPGConfig

from ray.tune.registry import register_trainable

__all__ = ["ApexDDPGConfig", "ApexDDPG"]

register_trainable("rllib-contrib-apex-ddpg", ApexDDPG)
