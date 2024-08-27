from rllib_es.es.es import ES, ESConfig
from rllib_es.es.es_tf_policy import ESTFPolicy
from rllib_es.es.es_torch_policy import ESTorchPolicy

from ray.tune.registry import register_trainable

__all__ = ["ES", "ESConfig", "ESTFPolicy", "ESTorchPolicy"]

register_trainable("rllib-contrib-es", ES)
