from rllib_dt.dt.dt import DT, DTConfig
from rllib_dt.dt.dt_torch_model import DTTorchModel
from rllib_dt.dt.dt_torch_policy import DTTorchPolicy

from ray.tune.registry import register_trainable

__all__ = ["DT", "DTConfig", "DTTorchModel", "DTTorchPolicy"]

register_trainable("rllib-contrib-dt", DT)
