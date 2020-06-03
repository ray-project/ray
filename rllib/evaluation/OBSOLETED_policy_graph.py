from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="rllib.evaluation.policy_graph",
    new="rllib.policy.policy",
    error=True)
