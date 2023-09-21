from typing import Any, Callable, List, Optional
from ray.rllib.core.rl_module.marl_module import ModuleID, MultiAgentRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.typing import T


def foreach_module(
    marl_module: MultiAgentRLModule,
    func: Callable[[RLModule, ModuleID, Optional[Any]], T],
    **kwargs
) -> List[T]:
    """Calls the given function with each (module, module_id).

    Args:
        func: The function to call with each (module, module_id) tuple.

    Returns:
        The lsit of return values of all calls to
        `func([module, module_id, **kwargs])`.
    """
    return [
        func(module, module_id, **kwargs)
        for module_id, module in marl_module._rl_modules.items()
    ]
