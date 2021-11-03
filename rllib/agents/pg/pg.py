from ray.rllib.trainer.pg.pg_trainer import DEFAULT_CONFIG, PGTrainer  # noqa
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.agents.pg.pg",
    new="ray.rllib.trainer.pg.pg_trainer",
    error=False,
)
