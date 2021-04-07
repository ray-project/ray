from ray.rllib.env.wrappers.unity3d_env import Unity3DEnv as UE
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.env.unity3d_env.Unity3DEnv",
    new="ray.rllib.env.wrappers.unity3d_env.Unity3DEnv",
    error=False,
)

Unity3DEnv = UE
