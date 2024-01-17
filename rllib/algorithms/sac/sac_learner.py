import abc
import math
import numpy as np

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


# TODO (simon): Add and remove variables, if module is added or removed.
class SACLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Store the current alpha in log form. We need it during optimization
        # in log form.
        self.curr_log_alpha: Dict[ModuleID, Scheduler] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                # Note, we want to train the temperature parameter.
                np.log(self.config.get_config_for_module(module_id).initial_alpha),
                trainable=True,
            )
        )

        # TODO (simon): Write an `add_parameters` and `remove_parameters` methods to the
        # `Learner`.
        # Add the temperature parameters to the optimizer's parameters.
        for module_id, log_alpha in self.curr_log_alpha.items():
            self._named_optimizers[module_id + "_default_optimizer"].param_groups.append(
                {"log_alpha": log_alpha}
            )

        def get_target_entropy(module_id):
            """Returns the target entropy to use for the loss.

            Args:
                module_id: Module ID for which the target entropy should be
                    returned.

            Returns:
                Target entropy.
            """
            target_entropy = self.config.get_config_for_module(module_id).target_entropy
            action_space = self._module_spec.module_specs[module_id].action_space.shape
            if target_entropy is None or target_entropy == "auto":
                target_entropy = -np.prod(action_space)
            return target_entropy

        # TODO (sven): Do we always have the `config.action_space` here?
        self.target_entropy: Dict[ModuleID, Scheduler] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(get_target_entropy(module_id))
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
            module_id=module_id,
            config=config,
            timestep=timestep,
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
    def _update_module_target_networks(
        self, module_id: ModuleID, config: SACConfig
    ) -> None:
        """Update the target Q network(s) of each module with the current Q network.

        The update is made via Polyak averaging.

        Args:
            module_id: The module ID whose target Q network(s) should be updated.
            config: The `AlgorithmConfig` specific in the given `module_id`.
        """
