# Backward compat import.
from ray.rllib.algorithms.appo.default_appo_rl_module import (  # noqa
    DefaultAPPORLModule as APPORLModule,
)
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.algorithms.appo.appo_rl_module.APPORLModule",
    new="ray.rllib.algorithms.appo.default_appo_rl_module.DefaultAPPORLModule",
    error=False,
)
