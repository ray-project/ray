from typing import Any, Mapping, Type
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.core.rl_module import RLModule, MultiAgentRLModule
from ray.rllib.core.rl_module.rl_module import ModuleID


torch, nn = try_import_torch()


class TorchRLModule(RLModule, nn.Module):
    def __init__(self, rl_modules: Mapping[ModuleID, RLModule] = None) -> None:
        RLModule.__init__(self, rl_modules)
        nn.Module.__init__(self)

    @override(nn.Module)
    def forward(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """forward pass of the module.

        This is aliased to forward_train because Torch DDP requires a forward method to
        be implemented for backpropagation to work.
        """
        return self.forward_train(batch, **kwargs)

    @override(RLModule)
    def get_state(self) -> Mapping[str, Any]:
        return self.state_dict()

    @override(RLModule)
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        self.load_state_dict(state_dict)

    @override(RLModule)
    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        """Makes the module distributed."""
        # TODO (Avnish): Implement this.
        pass

    @override(RLModule)
    def is_distributed(self) -> bool:
        """Returns True if the module is distributed."""
        # TODO (Avnish): Implement this.
        return False

    @classmethod
    @override(RLModule)
    def get_multi_agent_class(cls) -> Type["MultiAgentRLModule"]:
        """Returns the multi-agent wrapper class for this module."""
        from ray.rllib.core.rl_module.torch.torch_marl_module import (
            TorchMultiAgentRLModule,
        )

        return TorchMultiAgentRLModule
