from rllib_simple_q.simple_q.simple_q import SimpleQ, SimpleQConfig
from rllib_simple_q.simple_q.simple_q_tf_policy import (
    SimpleQTF1Policy,
    SimpleQTF2Policy,
)
from rllib_simple_q.simple_q.simple_q_torch_policy import SimpleQTorchPolicy

from ray.tune.registry import register_trainable

__all__ = [
    "SimpleQ",
    "SimpleQConfig",
    "SimpleQTF1Policy",
    "SimpleQTF2Policy",
    "SimpleQTorchPolicy",
]

register_trainable("rllib-contrib-simple-q", SimpleQ)
