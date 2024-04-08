from ray.rllib.utils.deprecation import deprecation_warning


deprecation_warning(
    old="ray.rllib.examples.policy...",
    new="ray.rllib.examples._old_api_stack.policy...",
    error=True,
)
