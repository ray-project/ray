from ray.rllib.algorithms.pg import DEFAULT_CONFIG  # noqa
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.algorithms.pg.default_config::DEFAULT_CONFIG (python dict)",
    new="ray.rllib.algorithms.pg.pg::PGConfig() (RLlib TrainerConfig class)",
    error=True,
)
