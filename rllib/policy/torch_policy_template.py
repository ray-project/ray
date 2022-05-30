from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


@Deprecated(
    new="sub-class directly from `ray.rllib.policy.torch_policy_v2::TorchPolicyV2` "
        "and override needed methods",
    error=True,
)
def build_torch_policy(*args, **kwargs):
    pass
