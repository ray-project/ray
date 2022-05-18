from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimate
from ray.rllib.utils.deprecation import Deprecated

# TODO (rohan): Fix deprecation warnings
@Deprecated(
    new="ray.rllib.offline.estimators.off_policy_estimator::"
    "OffPolicyEstimator",
    error=False,
)
class OffPolicyEstimator:
    pass