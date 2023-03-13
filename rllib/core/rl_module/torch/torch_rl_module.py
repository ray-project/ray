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

    @override(RLModule)
    def save_state_to_file(self, path: Union[str, pathlib.Path]) -> str:
        if isinstance(path, str):
            path = pathlib.Path(path)
        module_state_path = path / "module_state.pt"
        torch.save(self.state_dict(), str(module_state_path))
        return str(module_state_path)

    @override(RLModule)
    def load_state_from_file(self, path: Union[str, pathlib.Path]) -> None:
        if isinstance(path, str):
            path = pathlib.Path(path)
        if not path.exists():
            raise ValueError(
                f"While loading state from path, the path does not exist: {path}"
            )
        self.set_state(torch.load(str(path)))

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
    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        # TODO (Kourosh): Not to sure about this make_distributed api belonging to
        # RLModule or the Learner? For now the logic is kept in Learner.
        # We should see if we can use this api end-point for both tf
        # and torch instead of doing it in the learner.
        pass

    @override(RLModule)
    def is_distributed(self) -> bool:
        return True
