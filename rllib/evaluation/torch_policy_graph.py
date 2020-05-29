from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="rllib.evaluation.torch_policy_graph",
    new="rllib.policy.torch_policy",
    error=True)
