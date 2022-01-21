from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.contrib.bandits.agents.[...]",
    new="ray.rllib.agents.bandits.[...]",
    error=True)
