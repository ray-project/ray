from ray.rllib.env.remote_base_env import RemoteBaseEnv
from ray.rllib.utils.annotations import Deprecated


@Deprecated(new="ray.rllib.env.remote_base_env.RemoteBaseEnv", error=False)
class RemoteVectorEnv(RemoteBaseEnv):
    pass
