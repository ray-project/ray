from ray.rllib.env.remote_base_env import RemoteBaseEnv
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="rllib.env.remote_vector_env.RemoteVectorEnv",
    new="ray.rllib.env.remote_base_env.RemoteBaseEnv",
    error=False,
)

RemoteVectorEnv = RemoteBaseEnv
