from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.core.distribution.distribution import Distribution  # noqa

deprecation_warning(
    old="ray.rllib.models.distributions.Distribution",
    new="ray.rllib.core.distribution.distribution.Distribution",
    error=False,
)
