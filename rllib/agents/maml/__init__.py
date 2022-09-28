from ray.rllib.algorithms.maml.maml import MAML as MAMLTrainer, DEFAULT_CONFIG

__all__ = [
    "MAMLTrainer",
    "DEFAULT_CONFIG",
]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("ray.rllib.agents.maml", "ray.rllib.algorithms.maml", error=True)
