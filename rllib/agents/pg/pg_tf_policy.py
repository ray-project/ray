from ray.rllib.trainer.pg.pg_tf_policy import pg_tf_loss, PGTFPolicy  # noqa
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.agents.pg.pg_tf_policy",
    new="ray.rllib.trainer.pg.pg_tf_policy",
    error=False,
)
