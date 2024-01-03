from rllib_ddppo.ddppo.ddppo import DDPPO, DDPPOConfig

from ray.tune.registry import register_trainable

__all__ = ["DDPPOConfig", "DDPPO"]

register_trainable("rllib-contrib-ddppo", DDPPO)
