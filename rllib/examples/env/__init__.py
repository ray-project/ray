from ray.rllib.utils.deprecation import deprecation_warning


deprecation_warning(
    old="ray.rllib.examples.envs...",
    new="ray.rllib.examples.envs.classes...",
    error=True,
)
