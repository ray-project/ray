from dataclasses import dataclass


@dataclass
class LearnerGroupConfig:
    def __init_subclass__(cls, **kwargs):
        raise ValueError(
            "`LearnerGroupConfig` has been replaced by the `AlgorithmConfig` object of "
            "your experiment. All information that used to be inside "
            "`LearnerGroupConfig` is already available inside an `AlgorithmConfig` "
            "object (e.g. `num_learners` or `num_gpus_per_learner`). "
            "You can build a LearnerGroup directly using an `AlgorithmConfig` via: "
            "`config.build_learner_group(env=..., spaces=..., rl_module_spec=...)`."
        )
