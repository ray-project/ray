from ray.rllib.utils.deprecation import deprecation_warning

from ray.rllib.core.rl_module.multi_rl_module import (
    MultiRLModule,
    MultiRLModuleSpec,
    MultiRLModuleConfig,
)


MultiAgentRLModule = MultiRLModule
MultiAgentRLModuleConfig = MultiRLModuleConfig
MultiAgentRLModuleSpec = MultiRLModuleSpec

deprecation_warning(
    old="ray.rllib.core.rl_module.marl_module",
    new="ray.rllib.core.rl_module.multi_rl_module",
    error=False,
)
