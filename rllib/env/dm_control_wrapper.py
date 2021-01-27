from ray.rllib.env.wrappers.dm_control_wrapper import DMCEnv as DCE
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.env.dm_control_wrapper.DMCEnv",
    new="ray.rllib.env.wrappers.dm_control_wrapper.DMCEnv",
    error=False,
)

DMCEnv = DCE
