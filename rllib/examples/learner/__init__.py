from ray.rllib.utils.deprecation import deprecation_warning


deprecation_warning(
    old="ray.rllib.examples.learner...",
    new="ray.rllib.examples.learners...",
    error=True,
)
