from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv as PE
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.env.pettingzoo_env.PettingZooEnv",
    new="ray.rllib.env.wrappers.pettingzoo_env.PettingZooEnv",
    error=False,
)

PettingZooEnv = PE
