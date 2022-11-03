from ray.rllib.utils.deprecation import deprecation_warning

# Backward compatibility.
deprecation_warning(
    old="ray.rllib.execution.multi_gpu_learner.py",
    new="ray.rllib.execution.multi_gpu_learner_thread.py",
    error=True,
)
