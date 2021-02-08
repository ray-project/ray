from ray.rllib.agents.misc.slateq.slateq import SlateQTrainer, \
	DEFAULT_CONFIG
from ray.rllib.agents.misc.slateq.slateq_torch_policy import \
	SlateQTorchPolicy

__all__ = [
    "SlateQTrainer",
    "SlateQTorchPolicy",
    "DEFAULT_CONFIG",
]
