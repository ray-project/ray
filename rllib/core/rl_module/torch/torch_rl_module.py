import abc
import pathlib
from typing import Any, Mapping, Union, Type

from ray.rllib.core.rl_module import RLModule
from ray.rllib.models.torch.torch_distributions import TorchDistribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchRLModule(nn.Module, RLModule):
    """A base class for RLlib torch RLModules.

    Note that the `_forward` methods of this class are meant to be torch.compiled:
        - TorchRLModule._forward_train()
        - TorchRLModule._forward_inference()
        - TorchRLModule._forward_exploration()
    Generally, they should only contain torch-native tensor manipulations, or
    otherwise they may yield wrong outputs. In particular, the creation of RLlib
    distributions inside these methods should be avoided when using torch.compile.
    """

    framwork: str = "torch"

    def __init__(self, *args, **kwargs) -> None:
        nn.Module.__init__(self)
        RLModule.__init__(self, *args, **kwargs)

        if self.config and self.config.model_config_dict.get("torch_compile") is True:
            # Replace original forward methods by compiled versions of themselves
            self._forward_train = torch.compile(
                self._forward_train, backend="aot_eager"
            )
            self._forward_inference = torch.compile(
                self._forward_inference, backend="aot_eager"
            )
            self._forward_exploration = torch.compile(
                self._forward_exploration, backend="aot_eager"
            )

    @abc.abstractmethod
    def get_action_dist_cls(self) -> Type[TorchDistribution]:
        """Returns the action distribution class for this RL Module.

        This class is used to create action distributions from outputs of the forward
        methods. If the rare case that no action distribution class is needed,
        this method can return None.
        """

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
