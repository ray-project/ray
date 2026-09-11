from ray._common.deprecation import Deprecated
from ray.rllib.offline.estimators.weighted_importance_sampling import (
    WeightedImportanceSampling,
)


@Deprecated(
    new="ray.rllib.offline.estimators.weighted_importance_sampling::"
    "WeightedImportanceSampling",
    error=True,
)
class WeightedImportanceSamplingEstimator(WeightedImportanceSampling):
    pass
