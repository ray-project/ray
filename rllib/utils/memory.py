from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.numpy import aligned_array, concat_aligned  # noqa

deprecation_warning(
    old="ray.rllib.utils.memory.[...]",
    new="ray.rllib.utils.numpy.[...]",
    error=True,
)
