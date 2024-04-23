from rllib_slate_q.slate_q.slateq import SlateQ, SlateQConfig
from rllib_slate_q.slate_q.slateq_tf_policy import SlateQTFPolicy
from rllib_slate_q.slate_q.slateq_torch_policy import SlateQTorchPolicy

from ray.tune.registry import register_trainable

__all__ = ["SlateQConfig", "SlateQ", "SlateQTFPolicy", "SlateQTorchPolicy"]

register_trainable("rllib-contrib-slate-q", SlateQ)
