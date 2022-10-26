import abc
from typing import Any, List, Mapping, Union

from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.loss_and_optim.rl_loss_and_optim import RLLossAndOptim
from ray.rllib.utils.nested_dict import NestedDict



class MultiAgentRLLossAndOptim(abc.ABC):
    def __init__(self,
                 module: MultiAgentRLModule,
                 rl_loss_and_optim_classes: Union[RLLossAndOptim, List[RLLossAndOptim]],
                 optim_configs: Union[NestedDict, Mapping[str, Any]]):
        self._module = module
        self._rl_loss_and_optim_classes = rl_loss_and_optim_classes
        self._optim_configs = optim_configs

    @abc.abstractproperty
    def trainable_modules(self) -> Mapping[RLModule, RLLossAndOptim]:
        """The map of trainable `RLModule`s to `RLLossAndOptim` instances"""

    @abc.abstractmethod
    def compute_loss(self, fwd_out: Mapping[str, Any]) -> Mapping[str, Any]:
        """Computes a loss based on fwd_out.

        Args:
            fwd_out: A dictionary of inputs for computing loss, typically
                generated from computed from self._module.forward_train.
        
        Returns:
            A dictionary of loss tensors used for computing the gradients of
            lossess with respect to fwd_out, and applying those gradients to
            self._module if necessary.
        """

    @abc.abstractmethod
    def compute_gradients_and_apply(self, loss_out: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """Compute and apply gradients to self._module if necessary.

        Computes the gradients for updating self._module based on loss_out and
        applies them to self._module if necessary. 

        """

    @abc.abstractmethod
    def get_state(self) -> Mapping[str, Any]:
        """Returns the state dict of this RLLossAndOptim instance."""

    @abc.abstractmethod
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        """Set the state of this RLLossAndOptim instance. """


class DefaultMARLLossAndOptim(MultiAgentRLLossAndOptim):

    def __init__(self,
                 module: MultiAgentRLModule,
                 rl_loss_and_optim_classes: Union[RLLossAndOptim, Mapping[str, RLLossAndOptim]],
                 optim_configs: Union[NestedDict, Mapping[str, Any]]):
        super().__init__(module, rl_loss_and_optim_classes, optim_configs)
        self._trainable_modules = {}
        for submodule_id in module.get_trainable_module_ids():
            submodule = module[submodule_id]
            if (isinstance(rl_loss_and_optim_classes, RLLossAndOptim) and
                isinstance(optim_configs, NestedDict)):
                self._trainable_modules[submodule] = rl_loss_and_optim_classes(optim_configs)
            elif (isinstance(rl_loss_and_optim_classes, dict) and 
                isinstance(optim_configs, dict)):
                cls = rl_loss_and_optim_classes[submodule_id]
                cfg = optim_configs[submodule_id]
                self._trainable_modules[submodule] = cls(cfg)
            else:
                #TODO: avnishn fill in the value error.
                raise ValueError


    def compute_loss(self, fwd_out: Mapping[str, Any]) -> Mapping[str, Any]:
        multi_agent_loss_dict = {}
        trainable_module_map = self.trainable_modules
        for module_id, agent_fwd_out in fwd_out.items():
            loss_and_optim = trainable_module_map[module_id]
            loss_dict = loss_and_optim.compute_loss(agent_fwd_out)
            multi_agent_loss_dict[module_id] = loss_dict
        return multi_agent_loss_dict

    def compute_gradients_and_apply(self, loss_out: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        stats_and_infos = {}
        trainable_module_map = self.trainable_modules
        for module_id, loss_dict in loss_out.items():
            loss_and_optim = trainable_module_map[module_id]
            module_stats_and_infos_out = loss_and_optim.compute_gradients_and_apply(loss_dict)
            stats_and_infos[module_id] = module_stats_and_infos_out
        return stats_and_infos
