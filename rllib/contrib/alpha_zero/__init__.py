from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.contrib.alpha_zero",
    new="ray.rllib.agents.alpha_zero",
    error=True,
)
