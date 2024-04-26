from ray.rllib.utils.deprecation import deprecation_warning


deprecation_warning(
    old="ray.rllib.examples.models...",
    new="ray.rllib.examples._old_api_stack.models...",
    error=True,
)
