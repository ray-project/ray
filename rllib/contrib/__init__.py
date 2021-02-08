import sys
from ray.rllib.contrib.agents import alpha_zero as AlphaZero
from ray.rllib.contrib.agents import bandits as Bandits
from ray.rllib.contrib.agents import maddpg as Maddpg
from ray.rllib.contrib.agents import random_agent as RandomAgent
from ray.rllib.contrib.env import sumo as Sumo
from ray.rllib.utils.deprecation import deprecation_warning

# Will be deprecated soon, old path names will still give the right class
sys.modules["ray.rllib.contrib.alpha_zero"] = AlphaZero
sys.modules["ray.rllib.contrib.bandits"] = Bandits
sys.modules["ray.rllib.contrib.maddpg"] = Maddpg
sys.modules["ray.rllib.contrib.random_agent"] = RandomAgent
sys.modules["ray.rllib.contrib.sumo"] = Sumo

deprecation_warning(
    old="ray.rllib.contrib.alpha_zero",
    new="ray.rllib.contrib.agents.alpha_zero",
    error=False,
)

deprecation_warning(
    old="ray.rllib.contrib.bandits",
    new="ray.rllib.contrib.agents.bandits",
    error=False,
)

deprecation_warning(
    old="ray.rllib.contrib.maddpg",
    new="ray.rllib.contrib.agents.maddpg",
    error=False,
)

deprecation_warning(
    old="ray.rllib.contrib.sumo",
    new="ray.rllib.contrib.env.sumo",
    error=False,
)
