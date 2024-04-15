from ray.rllib.utils.deprecation import deprecation_warning


deprecation_warning(
    old="ray.rllib.examples.catalog...",
    new="ray.rllib.examples.catalogs...",
    error=True,
)
