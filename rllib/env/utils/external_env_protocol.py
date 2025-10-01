from ray.rllib.env.external.rllink import RLlink  # noqa
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.env.utils.external_env_protocol",
    new="ray.rllib.env.external.rllink",
    error=False,
)
