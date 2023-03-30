import pathlib
from typing import Any, Mapping, Union

from ray.rllib.core.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchRLModule(nn.Module, RLModule):
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
    def save_state_to_file(self, path: Union[str, pathlib.Path]):
        torch.save(self.state_dict(), str(path))

    @override(RLModule)
    def load_state_from_file(self, path: Union[str, pathlib.Path]) -> None:
        self.set_state(torch.load(str(path)))


class TorchDDPRLModule(RLModule, nn.parallel.DistributedDataParallel):
    def __init__(self, *args, **kwargs) -> None:
        nn.parallel.DistributedDataParallel.__init__(self, *args, **kwargs)
        # we do not want to call RLModule.__init__ here because all we need is
        # the interface of that base-class not the actual implementation.

    @override(RLModule)
    def _forward_train(self, *args, **kwargs):
        return self(*args, **kwargs)

    @override(RLModule)
    def _forward_inference(self, *args, **kwargs) -> Mapping[str, Any]:
        return self.module._forward_inference(*args, **kwargs)

    @override(RLModule)
    def _forward_exploration(self, *args, **kwargs) -> Mapping[str, Any]:
        return self.module._forward_exploration(*args, **kwargs)

    @override(RLModule)
    def get_state(self, *args, **kwargs):
        return self.module.get_state(*args, **kwargs)

    @override(RLModule)
    def set_state(self, *args, **kwargs):
        self.module.set_state(*args, **kwargs)

    @override(RLModule)
    def save_state_to_file(self, *args, **kwargs) -> str:
        return self.module.save_state_to_file(*args, **kwargs)

    @override(RLModule)
    def load_state_from_file(self, *args, **kwargs):
        self.module.load_state_from_file(*args, **kwargs)
