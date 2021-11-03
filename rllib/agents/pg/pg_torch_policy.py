from ray.rllib.trainer.pg.pg_torch_policy import pg_torch_loss, \
    pg_loss_stats, PGTorchPolicy  # noqa
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.agents.pg.pg_torch_policy",
    new="ray.rllib.trainer.pg.pg_torch_policy",
    error=False,
)
