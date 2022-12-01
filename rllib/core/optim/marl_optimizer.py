import abc
from typing import Any, List, Mapping, Union

from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.loss_and_optim.rl_optimizer import RLOptimizer
from ray.rllib.utils.nested_dict import NestedDict


class MultiAgentRLOptimizer(RLOptimizer):
    def __init__(
        self,
        module: MultiAgentRLModule,
        rl_optimizer_classes: Union[RLOptimizer, List[RLOptimizer]],
        optim_configs: Union[NestedDict, Mapping[str, Any]],
    ):
        self._module = module
        self._rl_optimizer_classes = rl_optimizer_classes
        self._optim_configs = optim_configs

    @abc.abstractproperty
    def trainable_modules(self) -> Mapping[RLModule, RLOptimizer]:
        """The map of trainable `RLModule`s to `RLOptimizer` instances"""


class DefaultMARLLossAndOptim(MultiAgentRLOptimizer):
    def __init__(
        self,
        module: MultiAgentRLModule,
        rl_optimizer_classes: Union[RLOptimizer, List[RLOptimizer]],
        optim_configs: Union[NestedDict, Mapping[str, Any]],
    ):
        super().__init__(module, rl_optimizer_classes, optim_configs)
        self._trainable_modules = {}
        for submodule_id in module.get_trainable_module_ids():
            submodule = module[submodule_id]
            if isinstance(rl_optimizer_classes, RLOptimizer) and isinstance(
                optim_configs, NestedDict
            ):
                self._trainable_modules[submodule] = rl_optimizer_classes(optim_configs)
            elif isinstance(rl_optimizer_classes, dict) and isinstance(
                optim_configs, dict
            ):
                cls = rl_optimizer_classes[submodule_id]
                cfg = optim_configs[submodule_id]
                self._trainable_modules[submodule] = cls(cfg)
            else:
                # TODO: avnishn fill in the value error.
                raise ValueError

    def compute_optimization_vars(
        self, fwd_out: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        multi_agent_optim_vars = {}
        trainable_module_map = self.trainable_modules
        for module_id, agent_fwd_out in fwd_out.items():
            loss_and_optim = trainable_module_map[module_id]
            loss_dict = loss_and_optim.compute_optimization_vars(agent_fwd_out)
            multi_agent_optim_vars[module_id] = loss_dict
        return multi_agent_optim_vars

    def optimize(
        self, optimization_vars: Mapping[str, Any], **kwargs
    ) -> Mapping[str, Any]:
        stats_and_infos = {}
        trainable_module_map = self.trainable_modules
        for module_id, module_optim_vars in optimization_vars.items():
            rl_optimizer = trainable_module_map[module_id]
            module_stats_and_infos_out = rl_optimizer.optimize(module_optim_vars)
            stats_and_infos[module_id] = module_stats_and_infos_out
        return stats_and_infos
