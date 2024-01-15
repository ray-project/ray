import abc
import math

from typing import Any, Dict

from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import LAST_TARGET_UPDATE_TS, NUM_TARGET_UPDATES
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import ModuleID

QF_PREDS = "qf_preds"
QF_TARGET_PREDS = "qf_target_preds"


class SACLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Store the current alpha in log form. We need it during optimization
        # in log form.
        self.curr_log_alpha: Dict[ModuleID, Scheduler] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                math.log(self.config.get_config_for_module(module_id).initial_alpha)
            )
        )

    @override(Learner)
    def additional_update_for_module(
        self,
        *,
        module_id: ModuleID,
        config: SACConfig,
        timestep: int,
        last_update: int,
        **kwargs
    ) -> Dict[str, Any]:
        """Updates the target Q Networks.
        
        Args:
            module_id: Module ID of the module to be updated.

        """
        results = super().additional_update_for_module(
            module_id=module_id, config=config, timestep=timestep,
        )

        # TODO (Sven): APPO uses `config.target_update_frequency`. Can we 
        # choose a standard here?
        if (timestep - last_update) >= config.target_network_update_freq:
            self._update_module_target_networks(module_id, config)
            results[NUM_TARGET_UPDATES] = 1
            results[LAST_TARGET_UPDATE_TS] = timestep
        else:
            results[NUM_TARGET_UPDATES] = 0
            results[LAST_TARGET_UPDATE_TS] = last_update

        return results
    
    @abc.abstractclassmethod
    def _update_module_target_networks(self, module_id: ModuleID, config: SACConfig) -> None:
        """Update the target Q network(s) of each module with the current Q network.
        
        The update is made via Polyak averaging.
        
        Args:
            module_id: The module ID whose target Q network(s) should be updated.
            config: The `AlgorithmConfig` specific in the given `module_id`.
        """

