from ray._common.deprecation import deprecation_warning
from ray.rllib.offline.estimators.off_policy_estimator import (  # noqa: F401
    OffPolicyEstimator,
)

deprecation_warning(
    old="ray.rllib.offline.off_policy_estimator",
    new="ray.rllib.offline.estimators.off_policy_estimator",
    error=True,
)
