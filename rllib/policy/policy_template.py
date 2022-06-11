from ray.rllib.utils.annotations import Deprecated


@Deprecated(
    new="sub-class directly from `ray.rllib.policy.torch_policy_v2::TorchPolicyV2` "
        "and override needed methods",
    error=True,
)
def build_policy_class(*args, **kwargs):
    pass
