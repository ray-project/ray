from ray.rllib.offline.estimators.importance_sampling import ImportanceSampling
from ray.rllib.utils.deprecation import Deprecated


@Deprecated(
    new="ray.rllib.offline.estimators.importance_sampling::ImportanceSampling",
    error=False,
)
class ImportanceSamplingEstimator(ImportanceSampling):
    pass
