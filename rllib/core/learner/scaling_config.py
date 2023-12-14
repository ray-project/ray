from ray.rllib.utils.deprecation import deprecation_warning


deprecation_warning(
    old="LearnerGroupScalingConfig",
    help="`LearnerGroupScalingConfig` has been replaced by the `AlgorithmConfig` "
    "object of your experiment. All information that used to be inside "
    "`LearnerGroupScalingConfig` is already available inside an `AlgorithmConfig` "
    "object (e.g. `num_learner_workers` or `num_gpus_per_learner_worker`). "
    "You can build a LearnerGroup directly using an `AlgorithmConfig` via: "
    "`config.build_learner_group(env=..., spaces=..., rl_module_spec=...)`.",
    error=True,
)
