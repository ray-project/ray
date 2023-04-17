from ray.rllib.offline.feature_importance import FeatureImportance

__all__ = ["FeatureImportance"]

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    "ray.rllib.offline.estimators.feature_importance.FeatureImportance",
    "ray.rllib.offline.feature_importance.FeatureImportance",
    error=True,
)
