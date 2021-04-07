from ray.rllib.env.wrappers.dm_env_wrapper import DMEnv as DE
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.env.dm_env_wrapper.DMEnv",
    new="ray.rllib.env.wrappers.dm_env_wrapper.DMEnv",
    error=False,
)

DMEnv = DE
