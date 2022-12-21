from typing import Any, Dict, Mapping, Union, Type
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType


class MultiAgentRLOptimizer(RLOptimizer):
    def __init__(self, rl_optimizers: Mapping[ModuleID, RLOptimizer] = None):
        self._optimizers = rl_optimizers

    @classmethod
    def from_config(
        cls,
        module: MultiAgentRLModule,
        rl_optimizer_classes: Union[
            Type[RLOptimizer], Dict[ModuleID, Type[RLOptimizer]]
        ],
        optim_configs: Union[Dict[str, Any], Mapping[ModuleID, Mapping[str, Any]]],
    ):

        if issubclass(rl_optimizer_classes, RLOptimizer):
            rl_optimizer_classes = {DEFAULT_POLICY_ID: rl_optimizer_classes}
            optim_configs = {DEFAULT_POLICY_ID: optim_configs}
        assert len(rl_optimizer_classes) == len(optim_configs)
        assert set(rl_optimizer_classes.keys()) == set(optim_configs.keys())
        assert set(rl_optimizer_classes.keys()) == set(module.keys())
        optimizers = {}
        for module_id in module.keys():
            submodule = module[module_id]
            config = optim_configs[module_id]
            optimizers[module_id] = rl_optimizer_classes[module_id](submodule, config)
        return cls(optimizers)

    @override(RLOptimizer)
    def compute_loss(
        self,
        fwd_out: Mapping[ModuleID, Mapping[str, Any]],
        batch: Mapping[ModuleID, Mapping[str, Any]],
    ) -> Union[TensorType, Mapping[str, Any]]:
        total_loss = None
        ret = {}
        for module_id in batch.keys():
            optimizer = self._optimizers[module_id]
            loss = optimizer.compute_loss(fwd_out[module_id], batch[module_id])
            if not isinstance(loss, dict):
                loss = {"total_loss": loss}
            if total_loss is None:
                total_loss = loss["total_loss"]
            else:
                total_loss += loss["total_loss"]
            ret[module_id] = loss
        ret["total_loss"] = total_loss
        return ret

    @override(RLOptimizer)
    def get_state(self) -> Mapping[ModuleID, Mapping[str, Any]]:
        return {
            module_id: optimizer.get_state()
            for module_id, optimizer in self._optimizers.items()
        }

    @override(RLOptimizer)
    def set_state(self, state: Mapping[ModuleID, Mapping[str, Any]]) -> None:
        for module_id, sub_optimizer_state in state.items():
            optimizer = self._optimizers[module_id]
            optimizer.set_state(sub_optimizer_state)

    @override(RLOptimizer)
    def as_multi_agent(self) -> "MultiAgentRLOptimizer":
        return self

    def _configure_optimizers(self) -> None:
        # Do not implement as this will not be used
        assert False
