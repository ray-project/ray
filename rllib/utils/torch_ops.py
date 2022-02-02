from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.torch_utils import *  # noqa

deprecation_warning(
    old="ray.rllib.utils.torch_ops.[...]",
    new="ray.rllib.utils.torch_utils.[...]",
    error=True,
)
