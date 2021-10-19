from ray.rllib.env.wrappers.group_agents_wrapper import GroupAgentsWrapper as \
    GAW
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.env.group_agents_wrapper._GroupAgentsWrapper",
    new="ray.rllib.env.wrappers.group_agents_wrapper.GroupAgentsWrapper",
    error=False,
)

_GroupAgentsWrapper = GAW
