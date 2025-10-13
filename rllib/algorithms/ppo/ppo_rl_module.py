# Backward compat import.
from ray.rllib.algorithms.ppo.default_ppo_rl_module import (  # noqa
    DefaultPPORLModule as PPORLModule,
)
from ray._common.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.algorithms.ppo.ppo_rl_module.PPORLModule",
    new="ray.rllib.algorithms.ppo.default_ppo_rl_module.DefaultPPORLModule",
    error=False,
)
