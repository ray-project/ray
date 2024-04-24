from rllib_crr.crr.crr import CRR, CRRConfig
from rllib_crr.crr.crr_torch_model import CRRModel
from rllib_crr.crr.crr_torch_policy import CRRTorchPolicy

from ray.tune.registry import register_trainable

__all__ = ["CRR", "CRRConfig", "CRRModel", "CRRTorchPolicy"]

register_trainable("rllib-contrib-crr", CRR)
