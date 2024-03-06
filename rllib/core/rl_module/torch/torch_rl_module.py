from typing import Any, Dict, List, Mapping, Tuple, Type

from packaging import version

from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.core.rl_module.torch.torch_compile_config import TorchCompileConfig
from ray.rllib.models.torch.torch_distributions import TorchDistribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import (
    convert_to_torch_tensor,
    TORCH_COMPILE_REQUIRED_VERSION,
)
from ray.rllib.utils.typing import NetworkType

torch, nn = try_import_torch()


def compile_wrapper(rl_module: "TorchRLModule", compile_config: TorchCompileConfig):
    """A wrapper that compiles the forward methods of a TorchRLModule."""

    # TODO(Artur): Remove this once our requirements enforce torch >= 2.0.0
    # Check if torch framework supports torch.compile.
    if (
        torch is not None
        and version.parse(torch.__version__) < TORCH_COMPILE_REQUIRED_VERSION
    ):
        raise ValueError("torch.compile is only supported from torch 2.0.0")

    compiled_forward_train = torch.compile(
        rl_module._forward_train,
        backend=compile_config.torch_dynamo_backend,
        mode=compile_config.torch_dynamo_mode,
        **compile_config.kwargs
    )

    rl_module._forward_train = compiled_forward_train

    compiled_forward_inference = torch.compile(
        rl_module._forward_inference,
        backend=compile_config.torch_dynamo_backend,
        mode=compile_config.torch_dynamo_mode,
        **compile_config.kwargs
    )

    rl_module._forward_inference = compiled_forward_inference

    compiled_forward_exploration = torch.compile(
        rl_module._forward_exploration,
        backend=compile_config.torch_dynamo_backend,
        mode=compile_config.torch_dynamo_mode,
        **compile_config.kwargs
    )

    rl_module._forward_exploration = compiled_forward_exploration

    return rl_module


class TorchRLModule(nn.Module, RLModule):
    """A base class for RLlib PyTorch RLModules.

    Note that the `_forward` methods of this class can be 'torch.compiled' individually:
        - `TorchRLModule._forward_train()`
        - `TorchRLModule._forward_inference()`
        - `TorchRLModule._forward_exploration()`

    As a rule of thumb, they should only contain torch-native tensor manipulations,
    or otherwise they may yield wrong outputs. In particular, the creation of RLlib
    distributions inside these methods should be avoided when using `torch.compile`.
    When in doubt, you can use `torch.dynamo.explain()` to check whether a compiled
    method has broken up into multiple sub-graphs.

    Compiling these methods can bring speedups under certain conditions.
    """

    framework: str = "torch"

    def __init__(self, *args, **kwargs) -> None:
        nn.Module.__init__(self)
        RLModule.__init__(self, *args, **kwargs)

    def forward(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """forward pass of the module.

        This is aliased to forward_train because Torch DDP requires a forward method to
        be implemented for backpropagation to work.
        """
        return self.forward_train(batch, **kwargs)

    def compile(self, compile_config: TorchCompileConfig):
        """Compile the forward methods of this module.

        This is a convenience method that calls `compile_wrapper` with the given
        compile_config.

        Args:
            compile_config: The compile config to use.
        """
        return compile_wrapper(self, compile_config)

    @override(RLModule)
    def get_state(self) -> Dict[str, Any]:
        # Make sure to reliably return numpy arrays from this method.
        torch_state = self.state_dict()
        return convert_to_numpy(torch_state)

    @override(RLModule)
    def set_state(self, state: Dict[str, Any]) -> None:
        # Make sure to convert possible numpy arrays in `state to torch tensors first.
        torch_state = convert_to_torch_tensor(state)
        self.load_state_dict(torch_state)

    @override(RLModule)
    def _save_state(self, path) -> None:
        torch_state = self.state_dict()
        torch.save(torch_state, path)

    @override(RLModule)
    def _load_state(self, path):
        torch_state = torch.load(path)
        self.load_state_dict(torch_state)


class TorchDDPRLModule(RLModule, nn.parallel.DistributedDataParallel):
    def __init__(self, *args, **kwargs) -> None:
        nn.parallel.DistributedDataParallel.__init__(self, *args, **kwargs)
        # we do not want to call RLModule.__init__ here because all we need is
        # the interface of that base-class not the actual implementation.
        self.config = self.unwrapped().config

    def get_train_action_dist_cls(self, *args, **kwargs) -> Type[TorchDistribution]:
        return self.unwrapped().get_train_action_dist_cls(*args, **kwargs)

    def get_exploration_action_dist_cls(
        self, *args, **kwargs
    ) -> Type[TorchDistribution]:
        return self.unwrapped().get_exploration_action_dist_cls(*args, **kwargs)

    def get_inference_action_dist_cls(self, *args, **kwargs) -> Type[TorchDistribution]:
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
    def save(self, *args, **kwargs):
        self.unwrapped().save(*args, **kwargs)

    @override(RLModule)
    def restore(self, *args, **kwargs):
        self.unwrapped().load(*args, **kwargs)

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
