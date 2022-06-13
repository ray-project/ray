from ray.rllib.algorithms.slateq.slateq import DEFAULT_CONFIG
from ray.rllib.algorithms.slateq.slateq import SlateQ as SlateQTrainer
from ray.rllib.algorithms.slateq.slateq import SlateQConfig
from ray.rllib.algorithms.slateq.slateq_tf_policy import SlateQTFPolicy
from ray.rllib.algorithms.slateq.slateq_torch_policy import SlateQTorchPolicy
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "DEFAULT_CONFIG",
    "SlateQConfig",
    "SlateQTFPolicy",
    "SlateQTorchPolicy",
    "SlateQTrainer",
]


deprecation_warning(
    "ray.rllib.agents.slateq", "ray.rllib.algorithms.slateq", error=False
)
