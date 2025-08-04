from ray.rllib.offline.estimators.off_policy_estimator import (  # noqa: F401
    OffPolicyEstimator,
)
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.offline.off_policy_estimator",
    new="ray.rllib.offline.estimators.off_policy_estimator",
    error=True,
)
