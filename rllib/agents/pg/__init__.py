from ray.rllib.algorithms.pg.pg import DEFAULT_CONFIG, PGConfig, PGTrainer
from ray.rllib.algorithms.pg.pg_tf_policy import pg_tf_loss, PGTFPolicy
from ray.rllib.algorithms.pg.pg_torch_policy import pg_torch_loss, PGTorchPolicy
from ray.rllib.algorithms.pg.utils import post_process_advantages

__all__ = [
    "DEFAULT_CONFIG",
    "pg_tf_loss",
    "pg_torch_loss",
    "post_process_advantages",
    "PGConfig",
    "PGTFPolicy",
    "PGTorchPolicy",
    "PGTrainer",
]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("ray.rllib.agents.pg", "ray.rllib.algorithms.pg", error=False)
