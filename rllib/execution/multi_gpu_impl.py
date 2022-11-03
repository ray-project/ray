from ray.rllib.utils.deprecation import deprecation_warning

# Backward compatibility.
deprecation_warning(
    old="ray.rllib.execution.multi_gpu_impl.LocalSyncParallelOptimizer",
    new="ray.rllib.policy.dynamic_tf_policy.TFMultiGPUTowerStack",
    error=True,
)
