from typing import Any, Mapping, Type
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.core.rl_module import RLModule, MultiAgentRLModule

torch, nn = try_import_torch()


class TorchRLModule(RLModule, nn.Module):
    def __init__(self, config) -> None:
        RLModule.__init__(self, config)
        nn.Module.__init__(self)

    def forward(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """This is only done because Torch DDP requires a forward method to be
        implemented for backpropagation to work."""
        return self.forward_train(batch, **kwargs)

    def get_state(self) -> Mapping[str, Any]:
        return self.state_dict()

    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        self.load_state_dict(state_dict)

    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        """Makes the module distributed."""
        # TODO (Avnish): Implement this.
        raise NotImplementedError

    def is_distributed(self) -> bool:
        """Returns True if the module is distributed."""
        # TODO (Avnish): Implement this.
        return False

    @classmethod
    def get_multi_agent_class(cls) -> Type["MultiAgentRLModule"]:
        """Returns the multi-agent wrapper class for this module."""
        return TorchMARLModule
class TorchMARLModule(MultiAgentRLModule, nn.Module):

    def __init__(self, config: Mapping[str, Any]) -> None:
        MultiAgentRLModule.__init__(self, config)
        nn.Module.__init__(self)

        # after initialization, all submodules need to be TorchRLModules
        for module_id, module in self._rl_modules.items():
            assert isinstance(module, TorchRLModule), \
                f"Module {module_id} is not a TorchRLModule!"


        


    
