from typing import Any, Collection, Dict, Optional, Union, Type

from packaging import version

from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_compile_config import TorchCompileConfig
from ray.rllib.models.torch.torch_distributions import TorchDistribution
from ray.rllib.utils.annotations import override, OverrideToImplementCustomLogic
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import (
    convert_to_torch_tensor,
    TORCH_COMPILE_REQUIRED_VERSION,
)
from ray.rllib.utils.typing import StateDict

torch, nn = try_import_torch()


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

    # Stick with torch default.
    STATE_FILE_NAME = "module_state.pt"

    def __init__(self, *args, **kwargs) -> None:
        nn.Module.__init__(self)
        RLModule.__init__(self, *args, **kwargs)

    @override(nn.Module)
    def forward(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
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

    @OverrideToImplementCustomLogic
    @override(RLModule)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        inference_only: bool = False,
        **kwargs,
    ) -> StateDict:
        return convert_to_numpy(self.state_dict())

    @OverrideToImplementCustomLogic
    @override(RLModule)
    def set_state(self, state: StateDict) -> None:
        # If state contains more keys than `self.state_dict()`, then we simply ignore
        # these keys (strict=False). This is most likely due to `state` coming from
        # an `inference_only=False` RLModule, while `self` is an `inference_only=True`
        # RLModule.
        self.load_state_dict(convert_to_torch_tensor(state), strict=False)

    def _set_inference_only_state_dict_keys(self) -> None:
        """Sets expected and unexpected keys for the inference-only module.

        This method is called during setup to set the expected and unexpected keys
        for the inference-only module. The expected keys are used to rename the keys
        in the state dict when syncing from the learner to the inference module.
        The unexpected keys are used to remove keys from the state dict when syncing
        from the learner to the inference module.
        """
        pass

    def _inference_only_get_state_hook(self, state_dict: StateDict) -> StateDict:
        """Removes or renames the parameters in the state dict for the inference module.

        This hook is called when the state dict is created on a learner module for an
        inference-only module. The method removes or renames the parameters in the state
        dict that are not used by the inference module.
        The hook uses the expected and unexpected keys set during setup to remove or
        rename the parameters.

        Args:
            state_dict: The state dict to be modified.

        Returns:
            The modified state dict.
        """
        pass


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
    def _forward_inference(self, *args, **kwargs) -> Dict[str, Any]:
        return self.unwrapped()._forward_inference(*args, **kwargs)

    @override(RLModule)
    def _forward_exploration(self, *args, **kwargs) -> Dict[str, Any]:
        return self.unwrapped()._forward_exploration(*args, **kwargs)

    @override(RLModule)
    def get_state(self, *args, **kwargs):
        return self.unwrapped().get_state(*args, **kwargs)

    @override(RLModule)
    def set_state(self, *args, **kwargs):
        self.unwrapped().set_state(*args, **kwargs)

    @override(RLModule)
    def save_to_path(self, *args, **kwargs):
        self.unwrapped().save_to_path(*args, **kwargs)

    @override(RLModule)
    def restore_from_path(self, *args, **kwargs):
        self.unwrapped().restore_from_path(*args, **kwargs)

    @override(RLModule)
    def get_metadata(self, *args, **kwargs):
        self.unwrapped().get_metadata(*args, **kwargs)

    # TODO (sven): Figure out a better way to avoid having to method-spam this wrapper
    #  class, whenever we add a new API to any wrapped RLModule here. We could try
    #  auto generating the wrapper methods, but this will bring its own challenge
    #  (e.g. recursive calls due to __getattr__ checks, etc..).
    def _compute_values(self, *args, **kwargs):
        return self.unwrapped()._compute_values(*args, **kwargs)

    @override(RLModule)
    def unwrapped(self) -> "RLModule":
        return self.module


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
        **compile_config.kwargs,
    )

    rl_module._forward_train = compiled_forward_train

    compiled_forward_inference = torch.compile(
        rl_module._forward_inference,
        backend=compile_config.torch_dynamo_backend,
        mode=compile_config.torch_dynamo_mode,
        **compile_config.kwargs,
    )

    rl_module._forward_inference = compiled_forward_inference

    compiled_forward_exploration = torch.compile(
        rl_module._forward_exploration,
        backend=compile_config.torch_dynamo_backend,
        mode=compile_config.torch_dynamo_mode,
        **compile_config.kwargs,
    )

    rl_module._forward_exploration = compiled_forward_exploration

    return rl_module
