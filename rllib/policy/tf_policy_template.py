from ray.rllib.utils.deprecation import Deprecated


@Deprecated(
    new="sub-class directly from `ray.rllib.policy.[eager|"
        "dynamic]_tf_policy_v2::[Eager|Dynamic]TFPolicyV2` "
        "and override needed methods",
    error=True,
)
def build_tf_policy(*args, **kwargs):
    pass
