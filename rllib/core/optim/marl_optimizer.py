from typing import Any, List, Mapping, Union

from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule, ModuleID
from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.utils.nested_dict import NestedDict


class MultiAgentRLOptimizer(RLOptimizer):
    def __init__(
        self,
        module: MultiAgentRLModule,
        rl_optimizer_classes: Union[RLOptimizer, List[RLOptimizer]],
        optim_configs: Union[NestedDict, Mapping[str, Any]],
    ):
        # self._module = module
        self._rl_optimizer_classes = rl_optimizer_classes
        # self._optim_configs = optim_configs
        # self._trainable_modules = {}
        super().__init__(module, optim_configs)


class DefaultMARLOptimizer(MultiAgentRLOptimizer):
    def _configure_optimizers(self) -> Mapping[ModuleID, RLOptimizer]:
        optimizers = {}
        rl_optimizer_classes = self._rl_optimizer_classes
        for submodule_id in self.module.keys():
            submodule = self.module[submodule_id]
            if issubclass(rl_optimizer_classes, RLOptimizer):
                assert len(self.module.keys()) == 1
                optimizers[submodule_id] = rl_optimizer_classes(submodule, self._config)
            elif isinstance(rl_optimizer_classes, dict) and isinstance(
                self._config, dict
            ):
                cls = rl_optimizer_classes[submodule_id]
                cfg = self._config[submodule_id]
                optimizers[submodule_id] = cls(submodule, cfg)
            else:
                # TODO: avnishn fill in the value error.
                raise ValueError
        return optimizers

    def compute_loss(
        self,
        fwd_out: Mapping[ModuleID, Mapping[str, Any]],
        batch: Mapping[ModuleID, Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        loss_dict = {}
        total_loss = None
        for submodule_id in batch.keys():
            assert submodule_id in self._trainable_modules
            assert submodule_id in fwd_out, "fwd_out must contain all keys in batch"
            submodule_loss = self._trainable_modules[submodule_id].compute_loss(
                fwd_out[submodule_id], batch[submodule_id]
            )
            if isinstance(submodule_loss, dict):
                to_add_to_total_loss = submodule_loss["total_loss"]
                loss_dict[submodule_id] = submodule_loss
            else:
                to_add_to_total_loss = submodule_loss
                loss_dict[submodule_id] = {"total_loss": to_add_to_total_loss}
            if total_loss is None:
                total_loss = to_add_to_total_loss
            else:
                total_loss += to_add_to_total_loss
        return loss_dict

    def get_state(self) -> Mapping[ModuleID, Mapping[str, Any]]:
        state = {}
        for submodule_id, rl_optim in self._optimizers.items():
            state[submodule_id] = rl_optim.get_state()
        return state

    def set_state(self, state: Mapping[ModuleID, Mapping[str, Any]]):
        for submodule_id, state in state.items():
            self._optimizers[submodule_id].set_state(state)
