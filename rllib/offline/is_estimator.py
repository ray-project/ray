from ray.rllib.offline.estimators.importance_sampling import ImportanceSampling
from ray._common.deprecation import Deprecated


@Deprecated(
    new="ray.rllib.offline.estimators.importance_sampling::ImportanceSampling",
    error=True,
)
class ImportanceSamplingEstimator(ImportanceSampling):
    pass
