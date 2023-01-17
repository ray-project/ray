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
)
import torch
from torch.nn.parallel import DistributedDataParallel as DDP

from ray.train.torch.train_loop_utils import _TorchAccelerator

from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID
from ray.rllib.core.rl_trainer.rl_trainer import (
    RLTrainer,
    MultiAgentRLModule,
    ParamOptimizerPairs,
    Optimizer,
    ParamType,
    ParamDictType,
)
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType
from ray.rllib.utils.nested_dict import NestedDict

logger = logging.getLogger(__name__)



class DDPRLModuleWrapper(DDP, RLModule):

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
        # RLModule or not? we should see if we use this api end-point for both tf and 
        # torch instead of doing it in the trainer.
        pass

    @override(RLModule)
    def is_distributed(self) -> bool:
        return True


class TorchRLTrainer(RLTrainer):
    def __init__(
        self,
        module_class: Union[Type[RLModule], Type[MultiAgentRLModule]],
        module_kwargs: Mapping[str, Any],
        scaling_config: Mapping[str, Any],
        optimizer_config: Mapping[str, Any],
        distributed: bool = False,
        in_test: bool = False,
    ):
        super().__init__(
            module_class=module_class,
            module_kwargs=module_kwargs,
            scaling_config=scaling_config,
            optimizer_config=optimizer_config,
            distributed=distributed,
            in_test=in_test,
        )

        self._world_size = scaling_config.get("num_workers", 1)
        self._use_gpu = scaling_config.get("use_gpu", False)
        
    @property
    @override(RLTrainer)
    def module(self) -> MultiAgentRLModule:
        return self._module

    @override(RLTrainer)
    def configure_optimizers(self) -> ParamOptimizerPairs:
        lr = self.optimizer_config.get("lr", 1e-3)
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
    def _make_distributed(self) -> MultiAgentRLModule:
        module = self._make_module()

        # TODO (Kourosh): How do we handle model parallism?
        # TODO (Kourosh): Instead of using _TorchAccelerator, we should use the public 
        # api in ray.train but allow for session to be None without any errors raised. 
        self._device = _TorchAccelerator().get_device()

        # if the module is a MultiAgentRLModule and nn.Module we can simply assume
        # all the submodules are registered. Otherwise, we need to loop through
        # each submodule and move it to the correct device.
        # TODO (Kourosh): This can result in missing modules if the user does not
        # register them in the MultiAgentRLModule. We should find a better way to
        # handle this.
        if isinstance(module, torch.nn.Module):
            module.to(self._device)
            module = DDPRLModuleWrapper(module)
        else:
            for key in module.keys():
                module[key].to(self._device)
                module[key] = DDPRLModuleWrapper(module[key])

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
        module_cls: Type[RLModule],
        module_kwargs: Mapping[str, Any],
        set_optimizer_fn: Optional[Callable[[RLModule], ParamOptimizerPairs]] = None,
        optimizer_cls: Optional[Type[Optimizer]] = None,
    ) -> None:
        super().add_module(
            module_id=module_id,
            module_cls=module_cls,
            module_kwargs=module_kwargs,
            set_optimizer_fn=set_optimizer_fn,
            optimizer_cls=optimizer_cls,
        )

        # we need to ddpify the module that was just added to the pool
        self._module[module_id].to(self._device)
        self._module[module_id] = DDPRLModuleWrapper(self._module[module_id])
