from ray.rllib.trainer.pg.pg_trainer import PGTrainer, DEFAULT_CONFIG
from ray.rllib.trainer.pg.pg_tf_policy import pg_tf_loss, \
    post_process_advantages, PGTFPolicy
from ray.rllib.trainer.pg.pg_torch_policy import pg_torch_loss, PGTorchPolicy
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.agents.pg", new="ray.rllib.trainer.pg", error=False)

__all__ = [
    "pg_tf_loss",
    "pg_torch_loss",
    "post_process_advantages",
    "DEFAULT_CONFIG",
    "PGTFPolicy",
    "PGTorchPolicy",
    "PGTrainer",
]
