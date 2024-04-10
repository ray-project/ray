from ray.rllib.utils.deprecation import deprecation_warning


deprecation_warning(
    old="ray.rllib.examples.inference_and_serving...",
    new="ray.rllib.examples.inference... AND ray.rllib.examples.ray_serve...",
    error=True,
)
