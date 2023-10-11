from rllib_mbmpo.mbmpo.mbmpo import MBMPO, MBMPOConfig
from rllib_mbmpo.mbmpo.mbmpo_torch_policy import MBMPOTorchPolicy

from ray.tune.registry import register_trainable

__all__ = ["MBMPOConfig", "MBMPO", "MBMPOTorchPolicy"]

register_trainable("rllib-contrib-mbmpo", MBMPO)
