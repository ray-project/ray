from typing import Any, Mapping
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.core.rl_module import RLModule

torch, nn = try_import_torch()


class TorchRLModule(RLModule, nn.Module):
    def __init__(self, config) -> None:
        RLModule.__init__(self, config)
        nn.Module.__init__(self)

    def forward(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """forward pass of the module.

        This is aliased to forward_train because Torch DDP requires a forward method to
        be implemented for backpropagation to work.
        """
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
