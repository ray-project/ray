import logging
from typing import (
    Any,
    Mapping,
    Union,
    Type,
    Sequence,
    Hashable,
    Optional,
    Callable,
    TYPE_CHECKING,
)

from ray.rllib.core.rl_module.rl_module import (
    RLModule,
    ModuleID,
    SingleAgentRLModuleSpec,
)
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModule,
    MultiAgentRLModuleSpec,
)
from ray.rllib.core.rl_trainer.rl_trainer import (
    RLTrainer,
    ParamOptimizerPairs,
    Optimizer,
    ParamType,
    ParamDictType,
)
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchDDPRLModule
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()

if torch:
    from ray.air.config import ScalingConfig
    from ray.train.torch.train_loop_utils import _TorchAccelerator

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

logger = logging.getLogger(__name__)


class TorchRLTrainer(RLTrainer):

    framework: str = "torch"

    def __init__(
        self,
        *,
        module_spec: Optional[
            Union[SingleAgentRLModuleSpec, MultiAgentRLModuleSpec]
        ] = None,
        module: Optional[RLModule] = None,
        optimizer_config: Mapping[str, Any],
        distributed: bool = False,
        scaling_config: Optional["ScalingConfig"] = None,
        algorithm_config: Optional["AlgorithmConfig"] = None,
    ):
        super().__init__(
            module_spec=module_spec,
            module=module,
            optimizer_config=optimizer_config,
            distributed=distributed,
            scaling_config=scaling_config,
            algorithm_config=algorithm_config,
        )

        # TODO (Kourosh): Scaling config is required for torch trainer to do proper DDP
        # wraping setup but not so much required for tf. we need to
        scaling_config = scaling_config or ScalingConfig()
        self._world_size = scaling_config.num_workers or 1
        self._use_gpu = scaling_config.use_gpu

        # These attributes are set in the `build` method.
        self._device = None

    @property
    @override(RLTrainer)
    def module(self) -> MultiAgentRLModule:
        return self._module

    @override(RLTrainer)
    def configure_optimizers(self) -> ParamOptimizerPairs:
        # TODO (Kourosh): convert optimizer_config to dataclass later.
        lr = self.optimizer_config["lr"]
        return [
            (
                self.get_parameters(self._module[key]),
                torch.optim.Adam(self.get_parameters(self._module[key]), lr=lr),
            )
            for key in self._module.keys()
        ]

    @override(RLTrainer)
    def compute_gradients(
        self, loss: Union[TensorType, Mapping[str, Any]]
    ) -> ParamDictType:
        for optim in self._optim_to_param:
            # set_to_none is a faster way to zero out the gradients
            optim.zero_grad(set_to_none=True)
        loss[self.TOTAL_LOSS_KEY].backward()
        grads = {pid: p.grad for pid, p in self._params.items()}

        return grads

    @override(RLTrainer)
    def apply_gradients(self, gradients: ParamDictType) -> None:

        # make sure the parameters do not carry gradients on their own
        for optim in self._optim_to_param:
            optim.zero_grad(set_to_none=True)

        # set the gradient of the parameters
        for pid, grad in gradients.items():
            self._params[pid].grad = grad

        # for each optimizer call its step function with the gradients
        for optim in self._optim_to_param:
            optim.step()

    @override(RLTrainer)
    def build(self) -> None:
        # TODO (Kourosh): How do we handle model parallism?
        # TODO (Kourosh): Instead of using _TorchAccelerator, we should use the public
        # api in ray.train but allow for session to be None without any errors raised.
        if self._use_gpu:
            self._device = _TorchAccelerator().get_device()
        else:
            self._device = torch.device("cpu")
        super().build()

    @override(RLTrainer)
    def _make_distributed_module(self) -> MultiAgentRLModule:
        module = self._make_module()

        # if the module is a MultiAgentRLModule and nn.Module we can simply assume
        # all the submodules are registered. Otherwise, we need to loop through
        # each submodule and move it to the correct device.
        # TODO (Kourosh): This can result in missing modules if the user does not
        # register them in the MultiAgentRLModule. We should find a better way to
        # handle this.
        if isinstance(module, torch.nn.Module):
            module.to(self._device)
            module = TorchDDPRLModule(module)
        else:
            for key in module.keys():
                module[key].to(self._device)
                module.add_module(key, TorchDDPRLModule(module[key]), override=True)

        return module

    @override(RLTrainer)
    def _convert_batch_type(self, batch: MultiAgentBatch):
        batch = NestedDict(batch.policy_batches)
        batch = NestedDict(
            {
                k: torch.as_tensor(v, dtype=torch.float32, device=self._device)
                for k, v in batch.items()
            }
        )
        return batch

    @override(RLTrainer)
    def do_distributed_update(self, batch: MultiAgentBatch) -> Mapping[str, Any]:
        # in torch the distributed update is no different than the normal update
        return self._update(batch)

    @override(RLTrainer)
    def get_param_ref(self, param: ParamType) -> Hashable:
        return param

    @override(RLTrainer)
    def get_parameters(self, module: RLModule) -> Sequence[ParamType]:
        return list(module.parameters())

    @override(RLTrainer)
    def get_optimizer_obj(
        self, module: RLModule, optimizer_cls: Type[Optimizer]
    ) -> Optimizer:
        # TODO (Kourosh): the abstraction should take in optimizer_config as a
        # parameter as well.
        lr = self.optimizer_config.get("lr", 1e-3)
        return optimizer_cls(module.parameters(), lr=lr)

    @override(RLTrainer)
    def add_module(
        self,
        *,
        module_id: ModuleID,
        module_spec: SingleAgentRLModuleSpec,
        set_optimizer_fn: Optional[Callable[[RLModule], ParamOptimizerPairs]] = None,
        optimizer_cls: Optional[Type[Optimizer]] = None,
    ) -> None:
        super().add_module(
            module_id=module_id,
            module_spec=module_spec,
            set_optimizer_fn=set_optimizer_fn,
            optimizer_cls=optimizer_cls,
        )

        # we need to ddpify the module that was just added to the pool
        self._module[module_id].to(self._device)
        if self.distributed:
            self._module.add_module(
                module_id, TorchDDPRLModule(self._module[module_id]), override=True
            )
