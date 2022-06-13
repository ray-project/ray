from ray.rllib.algorithms.maml.maml import DEFAULT_CONFIG
from ray.rllib.algorithms.maml.maml import MAML as MAMLTrainer
from ray.rllib.utils.deprecation import deprecation_warning

__all__ = [
    "MAMLTrainer",
    "DEFAULT_CONFIG",
]


deprecation_warning("ray.rllib.agents.maml", "ray.rllib.algorithms.maml", error=False)
