from ray.rllib.offline.estimators.off_policy_estimator import (  # noqa: F401
    OffPolicyEstimate,
)
from ray.rllib.utils.deprecation import Deprecated

# TODO (rohan): Fix deprecation warnings


@Deprecated(
    new="ray.rllib.offline.estimators.off_policy_estimator::" "OffPolicyEstimator",
    error=True,
)
class OffPolicyEstimator:
    pass
