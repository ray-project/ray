from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.contrib.maddpg",
    new="ray.rllib.agents.maddpg",
    error=True,
)
