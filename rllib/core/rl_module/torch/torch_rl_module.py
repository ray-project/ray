import abc
import pathlib
import sys
from dataclasses import dataclass
from typing import Any, Mapping, Union, Type

from ray.rllib.core.models.specs.checker import (
    check_input_specs,
    check_output_specs,
)
from ray.rllib.core.rl_module import RLModule
from ray.rllib.models.torch.torch_distributions import TorchDistribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import SampleBatchType

torch, nn = try_import_torch()


@dataclass
class TorchCompileConfig:
    """Configuration options for RLlib's usage of torch.compile in RLModules.

    # On `torch.compile` in Torch RLModules
    `torch.compile` invokes torch's dynamo JIT compiler that can potentially bring
    speedups to RL Module's forward methods.
    This is a performance optimization that should be disabled for debugging.

    General usage:
    - Usually, you only want to `RLModule._forward_train` to be compiled on
      instances of RLModule used for learning. (e.g. the learner)
    - In some cases, it can bring speedups to also compile
      `RLModule._forward_exploration` on instances used for exploration. (e.g.
      RolloutWorker)
    - In some cases, it can bring speedups to also compile
      `RLModule._forward_inference` on instances used for inference. (e.g.
      RolloutWorker)

    Note that different backends are available on different platforms.
    Also note that the default backend for torch dynamo is "aot_eager" on macOS.
    This is a debugging backend that is expected not to improve performance because
    the inductor backend is not supported on OSX so far.

    Args:
        compile_forward_train: Whether to compile the forward_train method.
        compile_forward_inference: Whether to compile the forward_inference method.
        compile_forward_exploration: Whether to compile the forward_exploration method.
        torch_dynamo_backend: The torch.dynamo backend to use. One of

    """

    compile_forward_train = False
    compile_forward_inference = False
    compile_forward_exploration = False
    torch_dynamo_backend = "aot_eager" if sys.platform == "darwin" else "inductor"

    def maybe_compile_forward_methods(
        self, rl_module: "TorchRLModule"
    ) -> "TorchRLModule":
        """Compiles the forward methods of the given RLModule according to this config.

        Args:
            rl_module: The RLModule to compile the forward methods of.
        """
        if self.compile_forward_train:
            rl_module.compile_forward_train(backend=self.torch_dynamo_backend)
        if self.compile_forward_inference:
            rl_module.compile_forward_inference(backend=self.torch_dynamo_backend)
        if self.compile_forward_exploration:
            rl_module.compile_forward_exploration(backend=self.torch_dynamo_backend)

        return rl_module


class TorchRLModule(nn.Module, RLModule):
    """A base class for RLlib torch RLModules.

    Note that the `_forward` methods of this class are meant to be 'torch.compiled'
    individually:
        - TorchRLModule._forward_train()
        - TorchRLModule._forward_inference()
        - TorchRLModule._forward_exploration()
    As a rule of thumb, they should only contain torch-native tensor manipulations, or
    otherwise they may yield wrong outputs. In particular, the creation of RLlib
    distributions inside these methods should be avoided when using torch.compile.
    When in doubt, you can use torch.dynamo.explain() to check whether a compiled
    method has broken up into multiple sub-graphs.
    """

    framwork: str = "torch"

    def __init__(self, *args, **kwargs) -> None:
        nn.Module.__init__(self)
        RLModule.__init__(self, *args, **kwargs)

        # Whether to retrace torch compiled forward methods on set_weights.
        self._retrace_on_set_weights = False

    @check_input_specs("_input_specs_inference")
    @check_output_specs("_output_specs_inference")
    def forward_inference(self, batch: SampleBatchType, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during evaluation, called from the sampler.

        This method should not be overriden to implement a custom forward inference
        method. Instead, override the _forward_inference method.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_inference().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            ouptut_specs_inference().
        """
        # If this forward method was compiled, we call the compiled version.
        if hasattr(self, "__compiled_forward_inference"):
            return self.__compiled_forward_inference(batch, **kwargs)
        return self._forward_inference(batch, **kwargs)

    @check_input_specs("_input_specs_exploration")
    @check_output_specs("_output_specs_exploration")
    def forward_exploration(
        self, batch: SampleBatchType, **kwargs
    ) -> Mapping[str, Any]:
        """Forward-pass during exploration, called from the sampler.

        This method should not be overriden to implement a custom forward exploration
        method. Instead, override the _forward_exploration method.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_exploration().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            ouptut_specs_exploration().
        """
        # If this forward method was compiled, we call the compiled version.
        if hasattr(self, "__compiled_forward_exploration"):
            return self.__compiled_forward_exploration(batch, **kwargs)
        return self._forward_exploration(batch, **kwargs)

    @check_input_specs("_input_specs_train")
    @check_output_specs("_output_specs_train")
    def forward_train(self, batch: SampleBatchType, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during training called from the learner. This method should
        not be overriden. Instead, override the _forward_train method.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_train().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            ouptut_specs_train().
        """
        # If this forward method was compiled, we call the compiled version.
        if hasattr(self, "__compiled_forward_train"):
            return self.__compiled_forward_train(batch, **kwargs)
        return self._forward_train(batch, **kwargs)

    def compile_forward_train(self, backend="inductor", retrace_on_set_weights=True):
        """Compiles the forward_train method."""
        self.__compiled_forward_train = torch.compile(
            self._forward_train, backend=backend
        )
        self._retrace_on_set_weights = retrace_on_set_weights

    def compile_forward_inference(
        self, backend="inductor", retrace_on_set_weights=True
    ):
        """Compiles the forward_inference method."""
        self.__compiled_forward_inference = torch.compile(
            self._forward_inference, backend=backend
        )
        self._retrace_on_set_weights = retrace_on_set_weights

    def compile_forward_exploration(
        self, backend="inductor", retrace_on_set_weights=True
    ):
        """Compiles the forward_exploration method."""
        self.__compiled_forward_exploration = torch.compile(
            self._forward_exploration, backend=backend
        )
        self._retrace_on_set_weights = retrace_on_set_weights

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
        if self._retrace_on_set_weights:
            torch._dynamo.reset()

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
