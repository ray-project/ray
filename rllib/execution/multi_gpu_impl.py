from ray.rllib.policy.dynamic_tf_policy import TFMultiGPUTowerStack
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("LocalSyncParallelOptimizer", "TFMultiGPUTowerStack")
LocalSyncParallelOptimizer = TFMultiGPUTowerStack
