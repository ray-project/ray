from ray.rllib.algorithms.es.es import ESTrainer, DEFAULT_CONFIG
from ray.rllib.algorithms.es.es_tf_policy import ESTFPolicy
from ray.rllib.algorithms.es.es_torch_policy import ESTorchPolicy

__all__ = ["ESTFPolicy", "ESTorchPolicy", "ESTrainer", "DEFAULT_CONFIG"]


from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("ray.rllib.agents.es", "ray.rllib.algorithms.es", error=False)
