from ray.rllib.utils.deprecation import Deprecated


@Deprecated(new="build_policy_class(framework='torch')", error=True)
def build_torch_policy(*args, **kwargs):
    pass
