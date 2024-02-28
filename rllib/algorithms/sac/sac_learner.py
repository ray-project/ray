import abc
import numpy as np

from typing import Any, Dict

from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import LAST_TARGET_UPDATE_TS, NUM_TARGET_UPDATES
from ray.rllib.utils.typing import ModuleID, TensorType

# Now, this is double defined: In `SACRLModule` and here. I would keep it here
# or push it into the `Learner` as these are recurring keys in RL.
LOGPS_KEY = "logps"
QF_LOSS_KEY = "qf_loss"
QF_MEAN_KEY = "qf_mean"
QF_MAX_KEY = "qf_max"
QF_MIN_KEY = "qf_min"
QF_PREDS = "qf_preds"
QF_TARGET_PREDS = "qf_target_preds"
QF_TWIN_LOSS_KEY = "qf_twin_loss"
QF_TWIN_PREDS = "qf_twin_preds"
TD_ERROR_KEY = "td_error"


class SACLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        # Store the current alpha in log form. We need it during optimization
        # in log form.
        self.curr_log_alpha: Dict[ModuleID, TensorType] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                # Note, we want to train the temperature parameter.
                [np.log(self.config.get_config_for_module(module_id).initial_alpha)],
                trainable=True,
            )
        )

        # We need to call the `super()`'s `build()` method here to have the variables
        # for the alpha already defined.
        super().build()

        def get_target_entropy(module_id):
            """Returns the target entropy to use for the loss.

            Args:
                module_id: Module ID for which the target entropy should be
                    returned.

            Returns:
                Target entropy.
            """
            target_entropy = self.config.get_config_for_module(module_id).target_entropy
            if target_entropy is None or target_entropy == "auto":
                # TODO (sven): Do we always have the `config.action_space` here?
                target_entropy = -np.prod(
                    self._module_spec.module_specs[module_id].action_space.shape
                )
            return target_entropy

        self.target_entropy: Dict[ModuleID, TensorType] = LambdaDefaultDict(
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

    @override(Learner)
    def remove_module(self, module_id: ModuleID) -> None:
        """Removes the temperature and target entropy.

        Note, this means that we also need to remove the corresponding
        temperature optimizer.
        """
        super().remove_module(module_id)
        self.curr_log_alpha.pop(module_id, None)
        self.target_entropy.pop(module_id, None)

    @abc.abstractmethod
    def _update_module_target_networks(
        self, module_id: ModuleID, config: SACConfig
    ) -> None:
        """Update the target Q network(s) of each module with the current Q network.

        The update is made via Polyak averaging.

        Args:
            module_id: The module ID whose target Q network(s) should be updated.
            config: The `AlgorithmConfig` specific in the given `module_id`.
        """
