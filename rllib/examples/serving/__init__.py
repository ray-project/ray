from ray.rllib.utils.deprecation import deprecation_warning


deprecation_warning(
    old="ray.rllib.examples.serving...",
    new="ray.rllib.examples.envs.external_envs...",
    error=True,
)
