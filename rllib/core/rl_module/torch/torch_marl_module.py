from typing import Any, Mapping
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.core.rl_module import MultiAgentRLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule


torch, nn = try_import_torch()


class TorchMultiAgentRLModule(MultiAgentRLModule, nn.Module):
    def __init__(self, config: Mapping[str, Any]) -> None:
        MultiAgentRLModule.__init__(self, config)
        nn.Module.__init__(self)

        # after initialization, all submodules need to be TorchRLModules
        for module_id, module in self._rl_modules.items():
            assert isinstance(
                module, TorchRLModule
            ), f"Module {module_id} is not a TorchRLModule!"
