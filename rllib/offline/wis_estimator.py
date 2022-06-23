from ray.rllib.offline.estimators.weighted_importance_sampling import (
    WeightedImportanceSampling,
)
from ray.rllib.utils.deprecation import Deprecated


@Deprecated(
    new="ray.rllib.offline.estimators.weighted_importance_sampling::"
    "WeightedImportanceSampling",
    error=False,
)
class WeightedImportanceSamplingEstimator(WeightedImportanceSampling):
    pass
