from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.tf_utils import *  # noqa

deprecation_warning(
    old="ray.rllib.utils.tf_ops.[...]",
    new="ray.rllib.utils.tf_utils.[...]",
    error=True,
)
