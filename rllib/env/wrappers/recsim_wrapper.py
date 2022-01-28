# Deprecated module: Use ray.rllib.env.wrappers.recsim instead!
from ray.rllib.env.wrappers.recsim import make_recsim_env, \
    MultiDiscreteToDiscreteActionWrapper, RecSimObservationSpaceWrapper, \
    RecSimResetWrapper  # noqa
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.env.wrappers.recsim_wrapper",
    new="ray.rllib.env.wrappers.recsim",
    error=False)
