from ray.rllib.algorithms.pg.pg import DEFAULT_CONFIG, PGConfig, PG as PGTrainer
from ray.rllib.algorithms.pg.pg_torch_policy import PGTorchPolicy
from ray.rllib.algorithms.pg.utils import post_process_advantages

__all__ = [
    "DEFAULT_CONFIG",
    "post_process_advantages",
    "PGConfig",
    "PGTorchPolicy",
    "PGTrainer",
]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("ray.rllib.agents.pg", "ray.rllib.algorithms.pg", error=True)
