import logging
import pathlib
from typing import (
    Any,
    Hashable,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from ray.rllib.core.rl_module.rl_module import (
    RLModule,
    ModuleID,
    SingleAgentRLModuleSpec,
)
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.learner.learner import (
    FrameworkHyperparameters,
    Learner,
    ParamOptimizerPair,
    NamedParamOptimizerPairs,
    ParamType,
    ParamDictType,
)
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchDDPRLModule
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.torch_utils import (
    clip_gradients,
    convert_to_torch_tensor,
    copy_torch_tensors,
)
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()

if torch:
    from ray.train.torch.train_loop_utils import get_device


logger = logging.getLogger(__name__)


class TorchLearner(Learner):
    framework: str = "torch"

    def __init__(
        self,
        *,
        framework_hyperparameters: Optional[FrameworkHyperparameters] = None,
        **kwargs,
    ):
        super().__init__(
            framework_hyperparameters=(
                framework_hyperparameters or FrameworkHyperparameters()
            ),
            **kwargs,
        )

        # Will be set during build.
        self._device = None

    @override(Learner)
    def configure_optimizer_per_module(
        self, module_id: ModuleID
    ) -> Union[ParamOptimizerPair, NamedParamOptimizerPairs]:
        module = self._module[module_id]
        lr = self._optimizer_config["lr"]
        pair: ParamOptimizerPair = (
            self.get_parameters(module),
            torch.optim.Adam(self.get_parameters(module), lr=lr),
        )
        return pair

    @override(Learner)
    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]]
    ) -> ParamDictType:
        for optim in self._optimizer_parameters:
            # set_to_none is a faster way to zero out the gradients
            optim.zero_grad(set_to_none=True)
        loss[self.TOTAL_LOSS_KEY].backward()
        grads = {pid: p.grad for pid, p in self._params.items()}

        return grads

    @override(Learner)
    def postprocess_gradients(
        self,
        gradients_dict: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Postprocesses gradients depending on the optimizer config."""

        # Perform gradient clipping, if necessary.
        clip_gradients(
            gradients_dict,
            grad_clip=self._optimizer_config.get("grad_clip"),
            grad_clip_by=self._optimizer_config.get("grad_clip_by"),
        )

        return gradients_dict

    @override(Learner)
    def apply_gradients(self, gradients: ParamDictType) -> None:
        # make sure the parameters do not carry gradients on their own
        for optim in self._optimizer_parameters:
            optim.zero_grad(set_to_none=True)

        # set the gradient of the parameters
        for pid, grad in gradients.items():
            self._params[pid].grad = grad

        # for each optimizer call its step function with the gradients
        for optim in self._optimizer_parameters:
            optim.step()

    @override(Learner)
    def set_weights(self, weights: Mapping[str, Any]) -> None:
        """Sets the weights of the underlying MultiAgentRLModule"""
        weights = convert_to_torch_tensor(weights, device=self._device)
        return self._module.set_state(weights)

    @override(Learner)
    def _save_optimizers(self, path: Union[str, pathlib.Path]) -> None:
        path = pathlib.Path(path)
        path.mkdir(parents=True, exist_ok=True)
        optim_weights = self.get_optimizer_weights()
        for name, weights in optim_weights.items():
            torch.save(weights, path / f"{name}.pt")

    @override(Learner)
    def _load_optimizers(self, path: Union[str, pathlib.Path]) -> None:
        path = pathlib.Path(path)
        if not path.exists():
            raise ValueError(f"Directory {path} does not exist.")
        weights = {}
        for name in self._named_optimizers.keys():
            weights[name] = torch.load(path / f"{name}.pt")
        self.set_optimizer_weights(weights)

    @override(Learner)
    def get_optimizer_weights(self) -> Mapping[str, Any]:
        optimizer_name_weights = {}
        for name, optim in self._named_optimizers.items():
            optim_state_dict = optim.state_dict()
            optim_state_dict_cpu = copy_torch_tensors(optim_state_dict, device="cpu")
            optimizer_name_weights[name] = optim_state_dict_cpu
        return optimizer_name_weights

    @override(Learner)
    def set_optimizer_weights(self, weights: Mapping[str, Any]) -> None:
        for name, weight_dict in weights.items():
            if name not in self._named_optimizers:
                raise ValueError(
                    f"Optimizer {name} in weights is not known."
                    f"Known optimizers are {self._named_optimizers.keys()}"
                )
            optim = self._named_optimizers[name]
            weight_dict_correct_device = copy_torch_tensors(
                weight_dict, device=self._device
            )
            optim.load_state_dict(weight_dict_correct_device)

    @override(Learner)
    def get_param_ref(self, param: ParamType) -> Hashable:
        return param

    @override(Learner)
    def get_parameters(self, module: RLModule) -> Sequence[ParamType]:
        return list(module.parameters())

    @override(Learner)
    def _convert_batch_type(self, batch: MultiAgentBatch):
        batch = convert_to_torch_tensor(batch.policy_batches, device=self._device)
        batch = NestedDict(batch)
        return batch

    @override(Learner)
    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: SingleAgentRLModuleSpec,
    ) -> None:
        super().add_module(
            module_id=module_id,
            module_spec=module_spec,
        )

        # we need to ddpify the module that was just added to the pool
        module = self._module[module_id]
        if isinstance(module, TorchRLModule):
            self._module[module_id].to(self._device)
            if self.distributed:
                self._module.add_module(
                    module_id, TorchDDPRLModule(module), override=True
                )

    @override(Learner)
    def build(self) -> None:
        """Builds the TorchLearner.

        This method is specific to TorchLearner. Before running super() it will
        initialzed the device properly based on use_gpu and distributed flags, so that
        _make_module() can place the created module on the correct device. After
        running super() it will wrap the module in a TorchDDPRLModule if distributed is
        set.
        """
        # TODO (Kourosh): How do we handle model parallism?
        # TODO (Kourosh): Instead of using _TorchAccelerator, we should use the public
        # api in ray.train but allow for session to be None without any errors raised.
        if self._use_gpu:
            # get_device() returns the 0th device if
            # it is called from outside of a Ray Train session. Its necessary to give
            # the user the option to run on the gpu of their choice, so we enable that
            # option here via the local gpu id scaling config parameter.
            if self._distributed:
                self._device = get_device()
            else:
                assert self._local_gpu_idx < torch.cuda.device_count(), (
                    f"local_gpu_idx {self._local_gpu_idx} is not a valid GPU id or is "
                    " not available."
                )
                # this is an index into the available cuda devices. For example if
                # os.environ["CUDA_VISIBLE_DEVICES"] = "1" then
                # torch.cuda.device_count() = 1 and torch.device(0) will actuall map to
                # the gpu with id 1 on the node.
                self._device = torch.device(self._local_gpu_idx)
        else:
            self._device = torch.device("cpu")

        super().build()
        # if the module is a MultiAgentRLModule and nn.Module we can simply assume
        # all the submodules are registered. Otherwise, we need to loop through
        # each submodule and move it to the correct device.
        # TODO (Kourosh): This can result in missing modules if the user does not
        # register them in the MultiAgentRLModule. We should find a better way to
        # handle this.
        if self._distributed:
            if isinstance(self._module, TorchRLModule):
                self._module = TorchDDPRLModule(self._module)
            else:
                for key in self._module.keys():
                    if isinstance(self._module[key], TorchRLModule):
                        self._module.add_module(
                            key, TorchDDPRLModule(self._module[key]), override=True
                        )

    def _is_module_compatible_with_learner(self, module: RLModule) -> bool:
        return isinstance(module, nn.Module)

    @override(Learner)
    def _check_structure_param_optim_pair(self, param_optim_pair: Any) -> None:
        super()._check_structure_param_optim_pair(param_optim_pair)
        params, optim = param_optim_pair
        if not isinstance(optim, torch.optim.Optimizer):
            raise ValueError(
                f"The optimizer in {param_optim_pair} is not a torch.optim.Optimizer. "
                "Please use a torch.optim.Optimizer for TorchLearner."
            )
        for param in params:
            if not isinstance(param, torch.Tensor):
                raise ValueError(
                    f"One of the parameters {param} in this ParamOptimizerPair "
                    f"{param_optim_pair} is not a torch.Tensor. Please use a "
                    "torch.Tensor for TorchLearner."
                )

    @override(Learner)
    def _make_module(self) -> MultiAgentRLModule:
        module = super()._make_module()
        self._map_module_to_device(module)
        return module

    def _map_module_to_device(self, module: MultiAgentRLModule) -> None:
        """Moves the module to the correct device."""
        if isinstance(module, torch.nn.Module):
            module.to(self._device)
        else:
            for key in module.keys():
                if isinstance(module[key], torch.nn.Module):
                    module[key].to(self._device)
