from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="rllib.evaluation.tf_policy_graph",
    new="rllib.policy.tf_policy",
    error=True)
