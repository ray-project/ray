from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray/rllib/examples/recsim_with_slateq.py",
    new="ray/rllib/examples/recommender_system_with_recsim_and_slateq.py",
    error=True,
)
