import pathlib
from typing import Any, List, Mapping, Tuple, Union, Type

from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.core.rl_module import RLModule
from ray.rllib.models.distributions import Distribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import NetworkType

torch, nn = try_import_torch()


class TorchRLModule(nn.Module, RLModule):
    framwork: str = "torch"

    def __init__(self, *args, **kwargs) -> None:
        nn.Module.__init__(self)
        RLModule.__init__(self, *args, **kwargs)

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

    def _module_state_file_name(self) -> pathlib.Path:
        return pathlib.Path("module_state.pt")

    @override(RLModule)
    def save_state(self, path: Union[str, pathlib.Path]) -> None:
        torch.save(self.state_dict(), str(path))

    @override(RLModule)
    def load_state(self, path: Union[str, pathlib.Path]) -> None:
        self.set_state(torch.load(str(path)))


class TorchDDPRLModule(RLModule, nn.parallel.DistributedDataParallel):
    def __init__(self, *args, **kwargs) -> None:
        nn.parallel.DistributedDataParallel.__init__(self, *args, **kwargs)
        # we do not want to call RLModule.__init__ here because all we need is
        # the interface of that base-class not the actual implementation.
        self.config = self.unwrapped().config

    def get_train_action_dist_cls(self, *args, **kwargs) -> Type[Distribution]:
        return self.unwrapped().get_train_action_dist_cls(*args, **kwargs)

    def get_exploration_action_dist_cls(self, *args, **kwargs) -> Type[Distribution]:
        return self.unwrapped().get_exploration_action_dist_cls(*args, **kwargs)

    def get_inference_action_dist_cls(self, *args, **kwargs) -> Type[Distribution]:
        return self.unwrapped().get_inference_action_dist_cls(*args, **kwargs)

    @override(RLModule)
    def _forward_train(self, *args, **kwargs):
        return self(*args, **kwargs)

    @override(RLModule)
    def _forward_inference(self, *args, **kwargs) -> Mapping[str, Any]:
        return self.unwrapped()._forward_inference(*args, **kwargs)

    @override(RLModule)
    def _forward_exploration(self, *args, **kwargs) -> Mapping[str, Any]:
        return self.unwrapped()._forward_exploration(*args, **kwargs)

    @override(RLModule)
    def get_state(self, *args, **kwargs):
        return self.unwrapped().get_state(*args, **kwargs)

    @override(RLModule)
    def set_state(self, *args, **kwargs):
        self.unwrapped().set_state(*args, **kwargs)

    @override(RLModule)
    def save_state(self, *args, **kwargs):
        self.unwrapped().save_state(*args, **kwargs)

    @override(RLModule)
    def load_state(self, *args, **kwargs):
        self.unwrapped().load_state(*args, **kwargs)

    @override(RLModule)
    def save_to_checkpoint(self, *args, **kwargs):
        self.unwrapped().save_to_checkpoint(*args, **kwargs)

    @override(RLModule)
    def _save_module_metadata(self, *args, **kwargs):
        self.unwrapped()._save_module_metadata(*args, **kwargs)

    @override(RLModule)
    def _module_metadata(self, *args, **kwargs):
        return self.unwrapped()._module_metadata(*args, **kwargs)

    @override(RLModule)
    def unwrapped(self) -> "RLModule":
        return self.module


class TorchDDPRLModuleWithTargetNetworksInterface(
    TorchDDPRLModule,
    RLModuleWithTargetNetworksInterface,
):
    @override(RLModuleWithTargetNetworksInterface)
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        return self.module.get_target_network_pairs()
