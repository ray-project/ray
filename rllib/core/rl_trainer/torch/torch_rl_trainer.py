import logging
from typing import (
    Any,
    Mapping,
    Union,
    Type,
    Sequence,
    Hashable,
)
import torch
from torch.nn.parallel import DistributedDataParallel as DDP

import ray

from ray.rllib.core.rl_module import RLModule
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


logger = logging.getLogger(__name__)


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

        gpu_ids = ray.get_gpu_ids()
        self._world_size = scaling_config.get("num_workers", 1)
        self._gpu_id = gpu_ids[0] if gpu_ids else None

    @property
    @override(RLTrainer)
    def module(self) -> MultiAgentRLModule:
        if self.distributed:
            return self._module.module
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
        pg = torch.distributed.new_group(list(range(self._world_size)))
        if self._gpu_id is not None:
            module.to(self._gpu_id)
            module = DDP(module, device_ids=[self._gpu_id], process_group=pg)
        else:
            module = DDP(module, process_group=pg)
        return module

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
        return optimizer_cls(module.parameters, lr=lr)
